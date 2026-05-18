#!/bin/bash
# ================================================================
# WOC Netem Latency Injection Script
# ================================================================
# Run this ON EACH SERVER NODE before starting your experiment.
# It adds artificial RTT between server-to-server RPCs only.
#
# Usage:
#   ./netem_setup.sh apply <delay_ms> [jitter_ms]   # Add latency
#   ./netem_setup.sh remove                          # Clean up
#   ./netem_setup.sh status                          # Show current rules
#   ./netem_setup.sh verify <peer_ip>                # Measure actual RTT
#
# Latency tiers:
#   LAN baseline  :  0ms  (no netem)
#   DC cross-rack : 10ms  → RTT ~20ms
#   WAN near      : 25ms  → RTT ~50ms   (Montreal→NYC)
#   WAN far       : 50ms  → RTT ~100ms  (Montreal→London)
#   WAN extreme   :100ms  → RTT ~200ms
# ================================================================

IFACE=$(ip route | grep default | awk '{print $5}' | head -1)
if [ -z "$IFACE" ]; then
    echo "Could not detect default interface. Set IFACE manually."
    exit 1
fi

# Server-to-server subnet — adjust if your cluster uses a different range
SERVER_SUBNET="192.168.73.0/24"

case "$1" in

  # ── Apply latency (all traffic on this node) ───────────────────────────────
  apply)
    DELAY=${2:?Usage: $0 apply <delay_ms> [jitter_ms]}
    JITTER=${3:-0}

    echo "=============================================="
    echo "Applying netem on interface: $IFACE"
    echo "Delay: ${DELAY}ms  Jitter: ${JITTER}ms"
    echo "Subnet: $SERVER_SUBNET"
    echo "=============================================="

    # Remove existing rules first
    tc qdisc del dev $IFACE root 2>/dev/null || true

    if [ "$JITTER" -gt 0 ]; then
        # HTB root → netem on default class → filter for server subnet
        tc qdisc add dev $IFACE root handle 1: htb default 10
        tc class add dev $IFACE parent 1: classid 1:1 htb rate 10gbit ceil 10gbit
        tc class add dev $IFACE parent 1: classid 1:10 htb rate 10gbit ceil 10gbit

        # Netem with jitter on server-to-server class
        tc qdisc add dev $IFACE parent 1:1 handle 10: netem \
            delay ${DELAY}ms ${JITTER}ms distribution normal

        # Filter: match server subnet → class 1:1 (with delay)
        tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 \
            match ip dst ${SERVER_SUBNET} flowid 1:1

        echo "Applied: ${DELAY}ms ± ${JITTER}ms (normal distribution) to $SERVER_SUBNET"
    else
        # Simple case: flat delay, no jitter
        tc qdisc add dev $IFACE root handle 1: htb default 10
        tc class add dev $IFACE parent 1: classid 1:1 htb rate 10gbit ceil 10gbit
        tc class add dev $IFACE parent 1: classid 1:10 htb rate 10gbit ceil 10gbit

        tc qdisc add dev $IFACE parent 1:1 handle 10: netem delay ${DELAY}ms

        tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 \
            match ip dst ${SERVER_SUBNET} flowid 1:1

        echo "Applied: ${DELAY}ms flat delay to $SERVER_SUBNET"
    fi

    echo ""
    echo "Verify with: $0 status"
    echo "Verify RTT:  $0 verify <peer_server_ip>"
    ;;

  # ── Apply to specific peer IPs only ───────────────────────────────────────
  apply_peers)
    DELAY=${2:?Usage: $0 apply_peers <delay_ms> <ip1> [ip2 ...]}
    shift 2
    PEERS=("$@")

    tc qdisc del dev $IFACE root 2>/dev/null || true
    tc qdisc add dev $IFACE root handle 1: htb default 10
    tc class add dev $IFACE parent 1: classid 1:1 htb rate 10gbit ceil 10gbit
    tc class add dev $IFACE parent 1: classid 1:10 htb rate 10gbit ceil 10gbit
    tc qdisc add dev $IFACE parent 1:1 handle 10: netem delay ${DELAY}ms

    for ip in "${PEERS[@]}"; do
        tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 \
            match ip dst ${ip}/32 flowid 1:1
        echo "Filter added: $ip → ${DELAY}ms delay"
    done
    ;;

  # ── Also inject packet loss ────────────────────────────────────────────────
  apply_loss)
    DELAY=${2:?Usage: $0 apply_loss <delay_ms> <loss_pct>}
    LOSS=${3:?Usage: $0 apply_loss <delay_ms> <loss_pct>}

    tc qdisc del dev $IFACE root 2>/dev/null || true
    tc qdisc add dev $IFACE root handle 1: htb default 10
    tc class add dev $IFACE parent 1: classid 1:1 htb rate 10gbit ceil 10gbit
    tc class add dev $IFACE parent 1: classid 1:10 htb rate 10gbit ceil 10gbit

    tc qdisc add dev $IFACE parent 1:1 handle 10: netem \
        delay ${DELAY}ms loss ${LOSS}%

    tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 \
        match ip dst ${SERVER_SUBNET} flowid 1:1

    echo "Applied: ${DELAY}ms delay + ${LOSS}% packet loss to $SERVER_SUBNET"
    ;;

  # ── Remove all netem rules ─────────────────────────────────────────────────
  remove)
    tc qdisc del dev $IFACE root 2>/dev/null && \
        echo "Netem rules removed from $IFACE" || \
        echo "No rules to remove on $IFACE"
    ;;

  # ── Show current rules ─────────────────────────────────────────────────────
  status)
    echo "=== qdisc ==="
    tc qdisc show dev $IFACE
    echo ""
    echo "=== classes ==="
    tc class show dev $IFACE 2>/dev/null || true
    echo ""
    echo "=== filters ==="
    tc filter show dev $IFACE 2>/dev/null || true
    ;;

  # ── Verify actual RTT to a peer ───────────────────────────────────────────
  verify)
    PEER=${2:?Usage: $0 verify <peer_ip>}
    echo "Measuring RTT to $PEER (10 pings)..."
    ping -c 10 -i 0.2 $PEER | tail -2
    echo ""
    echo "Expected RTT ≈ 2 × one-way delay (netem adds delay in each direction)"
    ;;

  *)
    echo "Usage: $0 {apply <ms> [jitter]|apply_peers <ms> <ip...>|apply_loss <ms> <pct>|remove|status|verify <ip>}"
    exit 1
    ;;
esac
