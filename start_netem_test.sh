#!/bin/bash
# ================================================================
# WOC Netem Test - Combined Start & Stop
# ================================================================
# Launches a homogeneous cluster with network latency injection
# Usage:
#   ./start_netem_test.sh [delay_ms] [jitter_ms]  # Start cluster with netem
#   ./stop_netem_test.sh                          # Stop cluster & remove netem
#
# Examples:
#   ./start_netem_test.sh 25 5      # 25ms delay with 5ms jitter (WAN-near)
#   ./start_netem_test.sh 50 10     # 50ms delay with 10ms jitter (WAN-far)
#   ./start_netem_test.sh 10        # 10ms delay, no jitter (DC cross-rack)
# ================================================================

set -e
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

# ────────────────────────────────────────────────────────────────
# USER / SSH CONFIG
# ────────────────────────────────────────────────────────────────
USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"

# ────────────────────────────────────────────────────────────────
# REMOTE DIRECTORY SETUP
# ────────────────────────────────────────────────────────────────
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_homo.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# ────────────────────────────────────────────────────────────────
# NETEM PARAMETERS
# ────────────────────────────────────────────────────────────────
DELAY=${1:-25}        # Default 25ms one-way (WAN-near)
JITTER=${2:-5}        # Default 5ms jitter

# ────────────────────────────────────────────────────────────────
# WOC PARAMETERS - NETEM TEST OPTIMIZED
# ────────────────────────────────────────────────────────────────
NUM_SERVERS=5
NUM_CLIENTS=2               # Match the base 5 server / 2 client layout
THRESHOLD=2                 
OPS=0
EVAL_TYPE=0
BATCHSIZE=10                
MSG_SIZE=512
MODE=1
CONFLICT_RATE=0             # 0% for clean latency measurements
INDEP_RATIO=100.0             
COMMON_RATIO=0.0             
BATCH_COMPOSITION="object-specific"
PIPELINE_MODE="true"
MAX_INFLIGHT=5               
USE_ADAPTIVE_LIMITER="false"
PARALLEL_FAST_PATH="true"
LOG_LEVEL="info"            # Info level for clean netem runs
ENABLE_PRIORITY="true"
LATENCY_DEBUG="false"       # False for netem (no debug noise)
SERVER_BATCHING="false"

# ────────────────────────────────────────────────────────────────
# CLOUD IP LIST
# ────────────────────────────────────────────────────────────────
SERVER_IPS=(
"192.168.73.220" "192.168.73.240" "192.168.73.108" "192.168.73.179" "192.168.73.154"
)

CLIENT_HOST_IPS=(
"192.168.73.45" "192.168.73.229"
)

CLIENTS_PER_VM=1

# ────────────────────────────────────────────────────────────────
# BUILD WOC BINARY LOCALLY
# ────────────────────────────────────────────────────────────────
echo "=============================================="
echo " Building WOC binary locally..."
echo "=============================================="
go build -o "$BINARY"
echo " Build complete."

# ────────────────────────────────────────────────────────────────
# COPY BINARY TO ALL VMs
# ────────────────────────────────────────────────────────────────
copy_binary() {
    local TARGET_IP=$1
    echo " Copying binary to $TARGET_IP ..."
    scp -i $SSH_KEY "$BINARY" $USER@$TARGET_IP:$REMOTE_DIR/
}

echo "=============================================="
echo " Copying binary to all servers and clients..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    copy_binary $ip
done

# ────────────────────────────────────────────────────────────────
# APPLY NETEM ON ALL SERVERS
# ────────────────────────────────────────────────────────────────
apply_netem() {
    local IP=$1
    echo " Applying ${DELAY}ms delay (${JITTER}ms jitter) on $IP ..."
    scp -i $SSH_KEY ${REMOTE_DIR}/netem_setup.sh $USER@$IP:${REMOTE_DIR}/ 2>/dev/null || true
    ssh -i $SSH_KEY $USER@$IP "chmod +x ${REMOTE_DIR}/netem_setup.sh && \
        sudo ${REMOTE_DIR}/netem_setup.sh apply $DELAY $JITTER"
}

echo "=============================================="
echo " Applying netem latency on all server nodes..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}"; do
    apply_netem $ip &
done
wait

echo ""
echo "Verifying netem (server 0 → server 1):"
ssh -i $SSH_KEY $USER@${SERVER_IPS[0]} "ping -c 3 -i 0.1 ${SERVER_IPS[1]} 2>/dev/null | tail -1"

# ────────────────────────────────────────────────────────────────
# START SERVER FUNCTION
# ────────────────────────────────────────────────────────────────
start_server() {
    local SERVER_ID=$1
    local SERVER_IP=$2

    echo " Starting Server $SERVER_ID on $SERVER_IP ..."

    ssh -i $SSH_KEY $USER@$SERVER_IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/server${SERVER_ID} ${EVAL_DIR}
        SERVER_BATCHING=${SERVER_BATCHING} \
        PARALLEL_FAST_PATH=${PARALLEL_FAST_PATH} \
        nohup ./$BINARY \
            -id=${SERVER_ID} \
            -n=${NUM_SERVERS} \
            -t=${THRESHOLD} \
            -path=${CONFIG_PATH} \
            -pd=true \
            -role=0 \
            -ops=${OPS} \
            -b=${BATCHSIZE} \
            -indep=${INDEP_RATIO} \
            -common=${COMMON_RATIO} \
            -et=${EVAL_TYPE} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -log=${LOG_LEVEL} \
            -ep=${ENABLE_PRIORITY} \
            > ${LOG_DIR}/server${SERVER_ID}/output.log 2>&1 &
    "
}

# ────────────────────────────────────────────────────────────────
# START CLIENT FUNCTION
# ────────────────────────────────────────────────────────────────
start_client() {
    local CLIENT_ID=$1
    local CLIENT_IP=$2
    
    echo " Starting Client $CLIENT_ID on $CLIENT_IP ..."

    ssh -i $SSH_KEY $USER@$CLIENT_IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/client${CLIENT_ID} ${EVAL_DIR}/client${CLIENT_ID}
        PIPELINE_MODE=${PIPELINE_MODE} \
        MAX_INFLIGHT=${MAX_INFLIGHT} \
        nohup ./$BINARY \
            -id=${CLIENT_ID} \
            -n=${NUM_SERVERS} \
            -t=${THRESHOLD} \
            -path=${CONFIG_PATH} \
            -ops=${OPS} \
            -et=${EVAL_TYPE} \
            -pd=true \
            -role=1 \
            -b=${BATCHSIZE} \
            -indep=${INDEP_RATIO} \
            -common=${COMMON_RATIO} \
            -conflictrate=${CONFLICT_RATE} \
            -bcomp=${BATCH_COMPOSITION} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -log=${LOG_LEVEL} \
            > ${LOG_DIR}/client${CLIENT_ID}/output.log 2>&1 &
    "
}

# ────────────────────────────────────────────────────────────────
# START SERVERS
# ────────────────────────────────────────────────────────────────
echo "=============================================="
echo " Starting all servers (NETEM TEST MODE)..."
echo "=============================================="

for i in "${!SERVER_IPS[@]}"; do
    start_server $i "${SERVER_IPS[$i]}"
    sleep 1
done

echo "Waiting 15 seconds for cluster stabilization..."
sleep 15

# ────────────────────────────────────────────────────────────────
# START CLIENTS
# ────────────────────────────────────────────────────────────────
echo "=============================================="
echo " Starting ${NUM_CLIENTS} clients..."
echo "=============================================="

client_id=${NUM_SERVERS}

for vm_ip in "${CLIENT_HOST_IPS[@]}"; do
    for ((c=0; c<CLIENTS_PER_VM; c++)); do
        if [ $client_id -lt $((NUM_SERVERS + NUM_CLIENTS)) ]; then
            start_client $client_id "$vm_ip"
            ((client_id++))
            sleep 1
        fi
    done
done

echo "=============================================="
echo " NETEM TEST cluster started successfully!"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Servers: ${NUM_SERVERS} (IDs 0-$((NUM_SERVERS-1)))"
echo "  Clients: ${NUM_CLIENTS} (IDs ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "  Latency: ${DELAY}ms ± ${JITTER}ms"
echo "  Config File: cluster_homo.conf"
echo ""
echo "Netem latency tiers:"
echo "  0ms    → Baseline (no delay)"
echo "  10ms   → DC cross-rack (RTT ~20ms)"
echo "  25ms   → WAN-near Montreal→NYC (RTT ~50ms)"
echo "  50ms   → WAN-far Montreal→London (RTT ~100ms)"
echo "  100ms  → WAN-extreme (RTT ~200ms)"
echo ""
echo "Monitor server logs:"
echo "  ssh -i $SSH_KEY $USER@${SERVER_IPS[0]} 'tail -f ${LOG_DIR}/server0/output.log'"
echo ""
echo "Stop cluster & remove netem:"
echo "  ./stop_netem_test.sh"
echo "=============================================="
