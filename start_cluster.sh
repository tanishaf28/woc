#!/bin/bash
# ================================================================
# WOC Cloud Cluster Launcher - 20 CLIENT VERSION
# ================================================================

set -e
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

# -----------------------------
# USER / SSH CONFIG
# -----------------------------
USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"

# -----------------------------
# REMOTE DIRECTORY SETUP
# -----------------------------
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_localhost.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# -----------------------------
# WOC PARAMETERS
# -----------------------------
NUM_SERVERS=5
NUM_CLIENTS=2               
THRESHOLD=1                 
OPS=0
EVAL_TYPE=0
BATCHSIZE=1                # ✅ CHANGED: 1 → 10 (better for cloud)
MSG_SIZE=512
MODE=1
CONFLICT_RATE=0              # ✅ CHANGED: 100 → 0 (test fast path)
INDEP_RATIO=100.0             # ✅ CHANGED: 0.0 → 95.0
COMMON_RATIO=00.0             # ✅ CHANGED: 0.0 → 5.0
BATCH_COMPOSITION="object-specific"
PIPELINE_MODE="true"
MAX_INFLIGHT=1               # ✅ CHANGED: 3 → 5
USE_ADAPTIVE_LIMITER="false"
PARALLEL_FAST_PATH="true"
LOG_LEVEL="info"             # ✅ CHANGED: "debug" → "info" (production)
ENABLE_PRIORITY="true"
LATENCY_DEBUG="false"
SERVER_BATCHING="false"

# -----------------------------
# CLOUD IP LIST
# -----------------------------
SERVER_IPS=(
"192.168.228.176" "192.168.228.57" "192.168.228.200" "192.168.228.113" "192.168.228.54"
)

# ✅ NEW: 10 VMs for clients (2 clients per VM = 20 total)
CLIENT_HOST_IPS=(
"192.168.228.207" "192.168.228.150"
)

CLIENTS_PER_VM=1  # ✅ NEW: Each VM runs 2 client processes

# -----------------------------
# BUILD WOC BINARY LOCALLY
# -----------------------------
echo "=============================================="
echo "Building WOC binary locally..."
echo "=============================================="
go build -o "$BINARY"
echo " Build complete."

# -----------------------------
# COPY BINARY TO ALL VMs
# -----------------------------
copy_binary() {
    local TARGET_IP=$1
    echo " Copying binary to $TARGET_IP ..."
    scp -i $SSH_KEY "$BINARY" $USER@$TARGET_IP:$REMOTE_DIR/
}

echo "=============================================="
echo "Copying binary to all servers and clients..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    copy_binary $ip
done

# -----------------------------
# START SERVER FUNCTION (UNCHANGED)
# -----------------------------
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

# -----------------------------
# START CLIENT FUNCTION (UNCHANGED)
# -----------------------------
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

# -----------------------------
# START SERVERS (UNCHANGED)
# -----------------------------
echo "=============================================="
echo "Starting all servers..."
echo "=============================================="

for i in "${!SERVER_IPS[@]}"; do
    start_server $i "${SERVER_IPS[$i]}"
    sleep 1
done

echo "Waiting 15 seconds for cluster stabilization..."
sleep 15

# -----------------------------
# START CLIENTS (MODIFIED FOR 20 CLIENTS)
# -----------------------------
echo "=============================================="
echo "Starting ${NUM_CLIENTS} clients (${CLIENTS_PER_VM} per VM)..."
echo "=============================================="

client_id=${NUM_SERVERS}  # ✅ Start from ID 5

# ✅ NEW: Loop over VMs and start 2 clients per VM
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
echo " WOC cluster launched successfully!"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Servers: ${NUM_SERVERS} (IDs 0-$((NUM_SERVERS-1)))"
echo "  Clients: ${NUM_CLIENTS} (IDs ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "  Clients per VM: ${CLIENTS_PER_VM}"
echo ""
echo "Monitor logs:"
echo "  ssh -i $SSH_KEY ubuntu@${SERVER_IPS[0]} 'tail -f ${LOG_DIR}/server0/output.log'"
echo "  ssh -i $SSH_KEY ubuntu@${CLIENT_HOST_IPS[0]} 'tail -f ${LOG_DIR}/client5/output.log'"
echo ""
echo "Stop all processes:"
echo "  ./stop_cluster.sh"
echo "=============================================="
