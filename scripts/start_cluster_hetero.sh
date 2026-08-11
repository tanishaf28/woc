#!/bin/bash
# ================================================================
# WOC Cloud Cluster Launcher - HETEROGENEOUS CLUSTER
# ================================================================

set -e
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

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
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# -----------------------------
# WOC PARAMETERS
# -----------------------------
NUM_SERVERS="${NUM_SERVERS:-5}"
# Compute default threshold (f) = floor((NUM_SERVERS - 1) / 2) unless overridden
if [ -z "${THRESHOLD:-}" ]; then
    THRESHOLD=$(( (NUM_SERVERS - 1) / 2 ))
fi
OPS=0
EVAL_TYPE=0
BATCHSIZE="${BATCHSIZE:-1}"
MSG_SIZE=512
MODE=1
INDEP_RATIO="${INDEP_RATIO:-90.0}"
NUM_OBJECTS="${NUM_OBJECTS:-1000}"    # size of the fixed, hash-ring-mapped object pool
BATCH_COMPOSITION="object-specific"
PIPELINE_MODE="${PIPELINE_MODE:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-5}"
USE_ADAPTIVE_LIMITER="${USE_ADAPTIVE_LIMITER:-false}"
LOG_LEVEL="${LOG_LEVEL:-info}"
ENABLE_PRIORITY="${ENABLE_PRIORITY:-true}"
LATENCY_DEBUG="false"
SERVER_BATCHING="false"

# -----------------------------
# CLOUD IP LIST + CONFIG SELECTION
# -----------------------------
# All 11 server IPs and 10 client IPs; sliced by NUM_SERVERS/NUM_CLIENTS below.
ALL_SERVER_IPS=(
    "192.168.73.59"  "192.168.73.243" "192.168.73.117" "192.168.73.16"
    "192.168.73.94"  "192.168.73.222" "192.168.73.250" "192.168.73.5"
    "192.168.73.237" "192.168.73.85"  "192.168.73.65"
)
ALL_CLIENT_IPS=(
    "192.168.73.159" "192.168.73.84"  "192.168.73.218" "192.168.73.219"
    "192.168.73.25"  "192.168.73.117" "192.168.73.16"  "192.168.73.94"
    "192.168.73.173" "192.168.73.71"
)

case "$NUM_SERVERS" in
    3)  CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_3n_10c.conf" ;;
    5)  CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_10c.conf" ;;
    7)  CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_7n_10c.conf" ;;
    11) CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_11n_10c.conf" ;;
    *)  echo "ERROR: unsupported NUM_SERVERS=${NUM_SERVERS}. Supported: 3, 5, 7, 11"; exit 1 ;;
esac

SCALE_CLIENTS="${SCALE_CLIENTS:-false}"
FIXED_CLIENT_COUNT="${FIXED_CLIENT_COUNT:-2}"
if [ "${SCALE_CLIENTS}" = "true" ]; then
    NUM_CLIENTS="${NUM_CLIENTS:-$(( NUM_SERVERS < 10 ? NUM_SERVERS : 10 ))}"
else
    NUM_CLIENTS="${NUM_CLIENTS:-${FIXED_CLIENT_COUNT}}"
fi
# Clients are always pinned (client idx J → server J % NUM_SERVERS)
SERVER_IPS=("${ALL_SERVER_IPS[@]:0:$NUM_SERVERS}")
CLIENT_HOST_IPS=("${ALL_CLIENT_IPS[@]:0:$NUM_CLIENTS}")

CLIENTS_PER_VM=1  # One client per VM

# -----------------------------
# BUILD WOC BINARY LOCALLY
# -----------------------------
echo "=============================================="
echo "Building WOC binary locally..."
echo "=============================================="
(cd "$REPO_ROOT" && go build -o "${SCRIPT_DIR}/${BINARY}")
echo " Build complete."

# -----------------------------
# COPY BINARY TO ALL VMs
# -----------------------------
copy_binary() {
    local TARGET_IP=$1
    echo " Copying binary to $TARGET_IP ..."
    scp -i $SSH_KEY "$BINARY" $USER@$TARGET_IP:$REMOTE_DIR/
}

copy_config() {
    local TARGET_IP=$1
    echo " Copying config to $TARGET_IP ..."
    ssh -i $SSH_KEY $USER@$TARGET_IP "mkdir -p $REMOTE_DIR/config"
    scp -i $SSH_KEY "$CONFIG_PATH" $USER@$TARGET_IP:$REMOTE_DIR/config/
}

echo "=============================================="
echo "Copying binary to all servers and clients..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    copy_binary "$ip"
    copy_config "$ip"
done

# -----------------------------
# START SERVER FUNCTION
# -----------------------------
start_server() {
    local SERVER_ID=$1
    local SERVER_IP=$2

    echo " Starting Server $SERVER_ID on $SERVER_IP ..."

    ssh -i $SSH_KEY $USER@$SERVER_IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/server${SERVER_ID} ${EVAL_DIR}
        SERVER_BATCHING=${SERVER_BATCHING} \
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
            -numobjects=${NUM_OBJECTS} \
            -et=${EVAL_TYPE} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -log=${LOG_LEVEL} \
            -ep=${ENABLE_PRIORITY} \
            > ${LOG_DIR}/server${SERVER_ID}/output.log 2>&1 &
    "
}

# -----------------------------
# START CLIENT FUNCTION
# -----------------------------
start_client() {
    local CLIENT_ID=$1
    local CLIENT_IP=$2
    local TARGET_SERVER=${3:--1}

    echo " Starting Client $CLIENT_ID on $CLIENT_IP (pinserver=${TARGET_SERVER})..."

    ssh -i $SSH_KEY $USER@$CLIENT_IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/client${CLIENT_ID} ${EVAL_DIR}/client${CLIENT_ID}
        PIPELINE_MODE=${PIPELINE_MODE} \
        MAX_INFLIGHT=${MAX_INFLIGHT} \
        USE_ADAPTIVE_LIMITER=${USE_ADAPTIVE_LIMITER} \
        ENABLE_TIMESERIES=${ENABLE_TIMESERIES:-false} \
        TPS_TIMELINE_INTERVAL_MS=${TPS_TIMELINE_INTERVAL_MS:-500} \
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
            -numobjects=${NUM_OBJECTS} \
            -bcomp=${BATCH_COMPOSITION} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -log=${LOG_LEVEL} \
            -pinserver=${TARGET_SERVER} \
            > ${LOG_DIR}/client${CLIENT_ID}/output.log 2>&1 &
    "
}

# -----------------------------
# START SERVERS
# -----------------------------
echo "=============================================="
echo "Starting all servers (Heterogeneous Cluster)..."
echo "=============================================="

for i in "${!SERVER_IPS[@]}"; do
    start_server "$i" "${SERVER_IPS[$i]}"
    sleep 1
done

echo "Waiting 15 seconds for cluster stabilization..."
sleep 15

# -----------------------------
# START CLIENTS
# -----------------------------
echo "Cleaning previous timeline files from client VMs..."
for vm_ip in "${CLIENT_HOST_IPS[@]}"; do
    ssh -i $SSH_KEY $USER@$vm_ip "
        rm -f ${EVAL_DIR}/client*/tps_timeline_*.csv 2>/dev/null || true
        echo 'Cleaned timeline files on ${vm_ip}'
    " &
done
wait
echo "Client dirs cleaned."

echo "=============================================="
echo "Starting ${NUM_CLIENTS} clients (${CLIENTS_PER_VM} per VM)..."
echo "=============================================="

client_id=${NUM_SERVERS}
client_idx=0

for vm_ip in "${CLIENT_HOST_IPS[@]}"; do
    for ((c=0; c<CLIENTS_PER_VM; c++)); do
        if [ $client_id -lt $((NUM_SERVERS + NUM_CLIENTS)) ]; then
            start_client "$client_id" "$vm_ip" "$((client_idx % NUM_SERVERS))"
            client_id=$((client_id + 1))
            client_idx=$((client_idx + 1))
            sleep 1
        fi
    done
done

echo "=============================================="
echo " WOC heterogeneous cluster launched successfully!"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Cluster Type: HETEROGENEOUS (5 servers + 2 clients)"
echo "  Servers: ${NUM_SERVERS} (IDs 0-$((NUM_SERVERS-1)))"
echo "  Clients: ${NUM_CLIENTS} (IDs ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "  Config File: cluster_hetero_new.conf"
echo ""
echo "Monitor logs:"
echo "  ssh -i $SSH_KEY ubuntu@${SERVER_IPS[0]} 'tail -f ${LOG_DIR}/server0/output.log'"
echo "  ssh -i $SSH_KEY ubuntu@${CLIENT_HOST_IPS[0]} 'tail -f ${LOG_DIR}/client5/output.log'"
echo ""
echo "Stop all processes:"
echo "  ./stop_cluster_hetero.sh"
echo "=============================================="
