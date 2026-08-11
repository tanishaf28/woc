#!/bin/bash
# ================================================================
# Apply/Remove netem on ALL server nodes via SSH
# Run this from your LOCAL machine (jumpbox/laptop)
# ================================================================
USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"

# Server IPs in ID order — update to match your experiment config
SERVER_IPS=(
    "192.168.73.220"
    "192.168.73.240"
    "192.168.73.108"
    "192.168.73.179"
    "192.168.73.154"
)

DELAY=${2:-25}   # Default 25ms one-way
JITTER=${3:-0}

apply_netem() {
    local IP=$1
    echo "  Applying ${DELAY}ms delay on $IP ..."
    scp -i $SSH_KEY ${REMOTE_DIR}/netem_setup.sh $USER@$IP:${REMOTE_DIR}/ 2>/dev/null || true
    ssh -i $SSH_KEY $USER@$IP "chmod +x ${REMOTE_DIR}/netem_setup.sh && \
        sudo ${REMOTE_DIR}/netem_setup.sh apply $DELAY $JITTER"
}

remove_netem() {
    local IP=$1
    echo "  Removing netem on $IP ..."
    ssh -i $SSH_KEY $USER@$IP "sudo ${REMOTE_DIR}/netem_setup.sh remove 2>/dev/null || true"
}

verify_netem() {
    local SRC_IP=$1
    local DST_IP=$2
    echo "  RTT $SRC_IP → $DST_IP:"
    ssh -i $SSH_KEY $USER@$SRC_IP "ping -c 5 -i 0.1 $DST_IP 2>/dev/null | tail -1"
}

case "$1" in
  apply)
    echo "Applying ${DELAY}ms netem on all ${#SERVER_IPS[@]} server nodes..."
    for IP in "${SERVER_IPS[@]}"; do
        apply_netem $IP &
    done
    wait
    echo ""
    echo "Verifying (server 0 → server 1):"
    verify_netem "${SERVER_IPS[0]}" "${SERVER_IPS[1]}"
    ;;

  remove)
    echo "Removing netem from all server nodes..."
    for IP in "${SERVER_IPS[@]}"; do
        remove_netem $IP &
    done
    wait
    echo "Done."
    ;;

  verify)
    echo "RTT matrix (baseline should be ~0.1ms, with netem ~2×${DELAY}ms):"
    verify_netem "${SERVER_IPS[0]}" "${SERVER_IPS[1]}"
    verify_netem "${SERVER_IPS[1]}" "${SERVER_IPS[2]}"
    verify_netem "${SERVER_IPS[0]}" "${SERVER_IPS[-1]}"
    ;;

  *)
    echo "Usage: $0 {apply [delay_ms] [jitter_ms] | remove | verify}"
    echo ""
    echo "Latency tiers:"
    echo "  $0 apply 0          # Baseline (remove netem)"
    echo "  $0 apply 10         # DC cross-rack → RTT ~20ms"
    echo "  $0 apply 25 5       # WAN-near (Montreal→NYC) with jitter"
    echo "  $0 apply 50 10      # WAN-far (Montreal→London)"
    echo "  $0 apply 100 15     # WAN-extreme"
    exit 1
    ;;
esac
