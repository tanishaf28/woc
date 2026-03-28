#!/bin/bash
# ================================================================
# CORA Cloud Cluster Launcher - AUTO CLIENT DISTRIBUTION
# ================================================================

set -e
trap 'echo "❌ Script interrupted. Exiting..."; exit 1' INT

# ============================================================================
# 🔧 EXPERIMENT CONFIGURATION
# ============================================================================
NUM_SERVERS=11           # Change: 3, 5, 7, or 11
NUM_CLIENTS=2          # Change: 1, 2, 5, 10, 20, or 50
BATCHSIZE=1          # Batch size for this experiment

# ============================================================================
# SSH / PATH CONFIG
# ============================================================================
USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_localhost.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# ============================================================================
# CORA PARAMETERS
# ============================================================================
THRESHOLD=1  # Auto-calculate quorum
OPS=0
EVAL_TYPE=0
MSG_SIZE=512
MODE=1
CONFLICT_RATE=0
INDEP_RATIO=100.0
COMMON_RATIO=0.0
BATCH_COMPOSITION="object-specific"
PIPELINE_MODE="true"
MAX_INFLIGHT=10
USE_ADAPTIVE_LIMITER="false"
PARALLEL_FAST_PATH="true"
LOG_LEVEL="debug"              # "info" for production, "debug" for debugging
ENABLE_PRIORITY="true"
LATENCY_DEBUG="false"
SERVER_BATCHING="false"

# ============================================================================
# VM IP CONFIGURATION
# ============================================================================

# Server VMs (first 11 VMs)
SERVER_IPS=(
"192.168.228.176"   # Server 0
"192.168.228.57"    # Server 1
"192.168.228.200"   # Server 2
"192.168.228.113"
"192.168.228.54"
"192.168.228.207"
"192.168.228.150"
"192.168.228.100"
"192.168.228.55"
"192.168.228.144"
"192.168.228.143"
)

# Client Host VMs (last 4 VMs available)
CLIENT_HOST_IPS=(
"192.168.228.118"
"192.168.228.84"
)

# ============================================================================
# AUTO CLIENT DISTRIBUTION
# ============================================================================

NUM_CLIENT_VMS=${#CLIENT_HOST_IPS[@]}

# Calculate base clients per VM and remainder
BASE=$((NUM_CLIENTS / NUM_CLIENT_VMS))
REM=$((NUM_CLIENTS % NUM_CLIENT_VMS))

# Build CLIENTS_PER_VM array (first REM VMs get +1 client)
CLIENTS_PER_VM=()
for ((i=0; i<NUM_CLIENT_VMS; i++)); do
    if [ $i -lt $REM ]; then
        CLIENTS_PER_VM+=($((BASE + 1)))
    else
        CLIENTS_PER_VM+=($BASE)
    fi
done

# Trim unused client VMs
USED_VMS=0
for c in "${CLIENTS_PER_VM[@]}"; do
    if [ "$c" -gt 0 ]; then
        USED_VMS=$((USED_VMS + 1))
    fi
done
CLIENT_HOST_IPS=("${CLIENT_HOST_IPS[@]:0:$USED_VMS}")

# ============================================================================
# DISPLAY CONFIGURATION
# ============================================================================

echo "=============================================="
echo "📊 CORA Experiment Configuration"
echo "=============================================="
echo "Servers : $NUM_SERVERS (Threshold: $THRESHOLD)"
echo "Clients : $NUM_CLIENTS"
echo "Batch   : $BATCHSIZE"
echo ""
echo "Client distribution:"
for i in "${!CLIENT_HOST_IPS[@]}"; do
    echo "  ${CLIENT_HOST_IPS[$i]} → ${CLIENTS_PER_VM[$i]} clients"
done
echo "=============================================="
echo ""

# ============================================================================
# VALIDATION
# ============================================================================

# Trim SERVER_IPS to actual NUM_SERVERS
SERVER_IPS=("${SERVER_IPS[@]:0:$NUM_SERVERS}")

if [ ${#SERVER_IPS[@]} -ne $NUM_SERVERS ]; then
    echo "❌ ERROR: Not enough server IPs configured"
    exit 1
fi

if [ $NUM_CLIENTS -gt 0 ] && [ ${#CLIENT_HOST_IPS[@]} -eq 0 ]; then
    echo "❌ ERROR: No client VMs available"
    exit 1
fi

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

copy_binary() {
    scp -i $SSH_KEY "$BINARY" $USER@$1:$REMOTE_DIR/ 2>/dev/null
}

start_server() {
    local SERVER_ID=$1
    local SERVER_IP=$2
    
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
    " 2>/dev/null
}

start_client() {
    local CLIENT_ID=$1
    local CLIENT_IP=$2
    
    ssh -i $SSH_KEY $USER@$CLIENT_IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/client${CLIENT_ID} ${EVAL_DIR}
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
    " 2>/dev/null
}

# ============================================================================
# BUILD + DEPLOY
# ============================================================================

echo "🔨 Building CORA binary..."
cd $REMOTE_DIR
go build -o $BINARY
echo "✅ Build complete"
echo ""

echo "📦 Copying binary to all VMs..."
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    echo "  → $ip"
    copy_binary $ip &
done
wait
echo "✅ Binary deployed"
echo ""

# ============================================================================
# START SERVERS
# ============================================================================

echo "🚀 Starting $NUM_SERVERS servers..."
for i in "${!SERVER_IPS[@]}"; do
    echo "  [Server $i] ${SERVER_IPS[$i]}"
    start_server $i "${SERVER_IPS[$i]}"
    [ $i -eq 0 ] && sleep 3 || sleep 1
done
echo ""

echo "⏳ Waiting 15s for cluster stabilization..."
sleep 15
echo ""

# ============================================================================
# START CLIENTS
# ============================================================================

if [ $NUM_CLIENTS -gt 0 ]; then
    echo "🚀 Starting $NUM_CLIENTS clients..."
    
    cid=$NUM_SERVERS  # Client IDs start after server IDs
    
    for vm_idx in "${!CLIENT_HOST_IPS[@]}"; do
        vm_ip="${CLIENT_HOST_IPS[$vm_idx]}"
        num_clients="${CLIENTS_PER_VM[$vm_idx]}"
        
        if [ "$num_clients" -gt 0 ]; then
            echo "  → ${vm_ip}: starting ${num_clients} clients"
            
            for ((c=0; c<num_clients; c++)); do
                start_client $cid "$vm_ip"
                ((cid++))
                sleep 0.3
            done
        fi
    done
    echo ""
fi

# ============================================================================
# SUCCESS SUMMARY
# ============================================================================

echo "=============================================="
echo "✅ CORA Cluster Started Successfully!"
echo "=============================================="
echo "Configuration:"
echo "  Servers: $NUM_SERVERS (IDs 0-$((NUM_SERVERS-1)))"
echo "  Clients: $NUM_CLIENTS (IDs $NUM_SERVERS-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "  Batch Size: $BATCHSIZE"
echo "  Total VMs: $((${#SERVER_IPS[@]} + ${#CLIENT_HOST_IPS[@]}))"
echo ""
echo "Monitor logs:"
echo "  ssh $USER@${SERVER_IPS[0]} 'tail -f ${LOG_DIR}/server0/output.log'"
if [ $NUM_CLIENTS -gt 0 ]; then
    echo "  ssh $USER@${CLIENT_HOST_IPS[0]} 'tail -f ${LOG_DIR}/client${NUM_SERVERS}/output.log'"
fi
echo ""
echo "Stop cluster:"
echo "  ./stop_cora_cluster.sh"
echo "=============================================="
