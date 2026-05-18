#!/bin/bash
# ================================================================
# WOC Crash Test - Combined Start & Stop
# ================================================================
# Launches a homogeneous cluster configured for crash testing
# Usage:
#   ./start_crash_test.sh              # Start cluster for crash testing
#   ./stop_crash_test.sh               # Stop cluster and collect results
#
# Then run crash scenarios:
#   ./crash_test.sh leader             # Kill server 0 (leader)
#   ./crash_test.sh follower 1         # Kill server 1
#   ./crash_test.sh f_of_n 2           # Kill 2 random followers
#   ./crash_test.sh restore 0          # Restore a crashed server
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
# WOC PARAMETERS - CRASH TEST OPTIMIZED
# ────────────────────────────────────────────────────────────────
NUM_SERVERS=5
NUM_CLIENTS=2               # Match the base 5 server / 2 client layout
THRESHOLD=2                 
OPS=0
EVAL_TYPE=0
BATCHSIZE=10                
MSG_SIZE=512
MODE=1
CONFLICT_RATE=10            # 10% conflicts to exercise quorum logic
INDEP_RATIO=70.0             
COMMON_RATIO=20.0             
BATCH_COMPOSITION="object-specific"
PIPELINE_MODE="true"
MAX_INFLIGHT=5               
USE_ADAPTIVE_LIMITER="false"
PARALLEL_FAST_PATH="true"
LOG_LEVEL="debug"            # DEBUG for crash test visibility
ENABLE_PRIORITY="true"
LATENCY_DEBUG="true"         # Enable for crash debugging
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
        LATENCY_DEBUG=${LATENCY_DEBUG} \
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
echo " Starting all servers (CRASH TEST MODE)..."
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
echo " CRASH TEST cluster started successfully!"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Servers: ${NUM_SERVERS} (IDs 0-$((NUM_SERVERS-1)))"
echo "  Clients: ${NUM_CLIENTS} (IDs ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "  Log Level: ${LOG_LEVEL}"
echo "  Config File: cluster_homo.conf"
echo ""
echo "Ready for crash testing!"
echo "Usage:"
echo "  ./crash_test.sh leader             # Kill server 0 (leader)"
echo "  ./crash_test.sh follower <id>      # Kill a specific follower"
echo "  ./crash_test.sh f_of_n <f>         # Kill f random followers"
echo "  ./crash_test.sh status             # Check server status"
echo "  ./crash_test.sh restore <id>       # Restore a crashed server"
echo ""
echo "Stop cluster:"
echo "  ./stop_crash_test.sh"
echo "=============================================="
