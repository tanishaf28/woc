#!/bin/bash
# ================================================================
# WOC Cloud Cluster Stopper - 11 Servers + 50 Clients
# ================================================================

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"

# All 11 server VMs
SERVER_IPS=(
"192.168.228.176"
"192.168.228.57"
"192.168.228.200"
"192.168.228.113"
"192.168.228.54"
"192.168.228.207"
"192.168.228.150"
"192.168.228.100"
"192.168.228.55"
"192.168.228.144"
"192.168.228.143"
)

# All 4 client VMs (hosting 50 clients total)
CLIENT_IPS=(
"192.168.228.118"
"192.168.228.84"
)

echo "=================================================="
echo " WOC Cluster Shutdown (50 Clients → 11 Servers)"
echo "=================================================="

# ---------------------------------------------------------------
# FUNCTION: Kill processes on a node
# ---------------------------------------------------------------
kill_on_node() {
    local ip=$1
    local type=$2   # "Client" or "Server"

    echo ""
    echo "→ Stopping ${type} on ${ip}"

    # Send SIGTERM first
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pkill -TERM -f woc 2>/dev/null" || true

    # Wait for graceful shutdown
    if [ "$type" = "Client" ]; then
        echo "  Waiting 30s for client graceful shutdown..."
        sleep 30
    else
        sleep 2
    fi

    # Check if still running
    local count
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pgrep -f woc 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \n' || echo 0)

    if [ "$count" -gt 0 ]; then
        echo "  Still running → Killing $count processes on $ip"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
            $USER@$ip "pkill -9 -f woc 2>/dev/null" || true
        sleep 1
    fi
    
    # Final check
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pgrep -f woc 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \n' || echo 0)

    if [ "$count" -eq 0 ]; then
        echo "   ${type} on ${ip} stopped"
    else
        echo "  WARNING: $count process(es) still active on ${ip}"
    fi
}

# ---------------------------------------------------------------
# STEP 1 — STOP CLIENTS (4 VMs hosting 50 clients)
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 1: Stopping 50 Clients on 4 VMs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for ip in "${CLIENT_IPS[@]}"; do
    kill_on_node "$ip" "Client" &
done

# Wait for all parallel client shutdowns
wait

echo ""
echo "Waiting 5 seconds for servers to flush metrics..."
sleep 5

# ---------------------------------------------------------------
# STEP 2 — STOP SERVERS (11 VMs)
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 2: Stopping 11 Servers"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for ip in "${SERVER_IPS[@]}"; do
    kill_on_node "$ip" "Server"
done

sleep 2

# ---------------------------------------------------------------
# FINAL VERIFICATION
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Verification (checking if ANY woc processes remain)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

any_left=false

for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pgrep -f woc 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \n' || echo 0)
    if [ "$count" -gt 0 ]; then
        echo " ${ip}: ${count} processes STILL running"
        any_left=true
    fi
done

if [ "$any_left" = false ]; then
    echo " All WOC processes stopped on all nodes."
else
    echo " Some processes remain. Manual cleanup may be needed."
fi

echo ""
echo "=================================================="
echo " CLUSTER SHUTDOWN COMPLETE"
echo "=================================================="
