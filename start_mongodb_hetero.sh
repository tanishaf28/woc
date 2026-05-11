#!/bin/bash
# ================================================================
# WOC MongoDB Workload Launcher - HETEROGENEOUS CLUSTER
# ================================================================

set -euo pipefail
trap 'echo "Script interrupted. Exiting..."; exit 1' INT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# -----------------------------
# USER / SSH CONFIG
# -----------------------------
USER="ubuntu"
if [ -n "${SSH_KEY:-}" ]; then
    SSH_KEY="${SSH_KEY}"
elif [ -f "/home/ubuntu/.ssh/tani.pem" ]; then
    SSH_KEY="/home/ubuntu/.ssh/tani.pem"
elif [ -f "${HOME}/.ssh/tani.pem" ]; then
    SSH_KEY="${HOME}/.ssh/tani.pem"
else
    SSH_KEY="/home/ubuntu/.ssh/tani.pem"
fi

# -----------------------------
# REMOTE DIRECTORY SETUP
# -----------------------------
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
LOCAL_CONFIG="${SCRIPT_DIR}/config/cluster_hetero.conf"
REMOTE_CONFIG="${REMOTE_DIR}/config/cluster_hetero.conf"
LOCAL_YCSB_DIR="${SCRIPT_DIR}/ycsb/workData"
REMOTE_YCSB_DIR="${REMOTE_DIR}/ycsb/workData"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# -----------------------------
# WOC PARAMETERS
# -----------------------------
NUM_SERVERS="${NUM_SERVERS:-5}"
NUM_CLIENTS="${NUM_CLIENTS:-2}"
THRESHOLD="${THRESHOLD:-2}"
OPS="${OPS:-0}"
EVAL_TYPE=1
BATCHSIZE="${BATCHSIZE:-10}"
MSG_SIZE="${MSG_SIZE:-512}"
MODE="${MODE:-1}"
CONFLICT_RATE="${CONFLICT_RATE:-0}"
INDEP_RATIO="${INDEP_RATIO:-100.0}"
COMMON_RATIO="${COMMON_RATIO:-0.0}"
BATCH_COMPOSITION="${BATCH_COMPOSITION:-object-specific}"
PIPELINE_MODE="${PIPELINE_MODE:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-5}"
PARALLEL_FAST_PATH="${PARALLEL_FAST_PATH:-true}"
LOG_LEVEL="${LOG_LEVEL:-info}"
ENABLE_PRIORITY="${ENABLE_PRIORITY:-true}"
SERVER_BATCHING="${SERVER_BATCHING:-false}"

MONGO_WORKLOAD="${1:-${MONGO_WORKLOAD:-a}}"
if [[ ! "$MONGO_WORKLOAD" =~ ^[a-f]$ ]]; then
    echo "ERROR: Invalid workload '$MONGO_WORKLOAD'. Valid workloads: a b c d e f"
    exit 1
fi

# -----------------------------
# CLOUD IP LIST (HETEROGENEOUS)
# -----------------------------
SERVER_IPS=(
"192.168.73.159" "192.168.73.84" "192.168.73.218" "192.168.73.219" "192.168.73.25"
)

CLIENT_HOST_IPS=(
"192.168.73.69" "192.168.73.235"
)

CLIENTS_PER_VM=1

require_file() {
    local file="$1"
    if [ ! -f "$file" ]; then
        echo "ERROR: Missing required file: $file"
        exit 1
    fi
}

# -----------------------------
# PRECHECKS
# -----------------------------
require_file "$SSH_KEY"
require_file "$LOCAL_CONFIG"

if [ ! -f "${LOCAL_YCSB_DIR}/workload.dat" ] || [ ! -f "${LOCAL_YCSB_DIR}/run_workload${MONGO_WORKLOAD}.dat" ]; then
    echo "YCSB workload files missing. Generating in ycsb/workData..."
    (
        cd "${SCRIPT_DIR}/ycsb/scripts"
        bash genData.sh
    )
fi

require_file "${LOCAL_YCSB_DIR}/workload.dat"
require_file "${LOCAL_YCSB_DIR}/run_workload${MONGO_WORKLOAD}.dat"

# -----------------------------
# BUILD WOC BINARY LOCALLY
# -----------------------------
echo "=============================================="
echo "Building WOC binary locally..."
echo "=============================================="
go build -o "$BINARY"
echo "Build complete."

# -----------------------------
# COPY ASSETS TO ALL VMS
# -----------------------------
copy_assets() {
    local target_ip="$1"

    echo "Copying binary/config/workload files to ${target_ip} ..."

    ssh -i "$SSH_KEY" "$USER@$target_ip" "mkdir -p ${REMOTE_DIR}/config ${REMOTE_YCSB_DIR}"

    scp -i "$SSH_KEY" "$BINARY" "$USER@$target_ip:${REMOTE_DIR}/"
    scp -i "$SSH_KEY" "$LOCAL_CONFIG" "$USER@$target_ip:${REMOTE_CONFIG}"
    scp -i "$SSH_KEY" "${LOCAL_YCSB_DIR}/workload.dat" "$USER@$target_ip:${REMOTE_YCSB_DIR}/workload.dat"
    scp -i "$SSH_KEY" "${LOCAL_YCSB_DIR}/run_workload${MONGO_WORKLOAD}.dat" "$USER@$target_ip:${REMOTE_YCSB_DIR}/run_workload${MONGO_WORKLOAD}.dat"
}

echo "=============================================="
echo "Copying artifacts to all servers and clients..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    copy_assets "$ip"
done

# -----------------------------
# START SERVER FUNCTION
# -----------------------------
start_server() {
    local server_id="$1"
    local server_ip="$2"

    echo "Starting Server ${server_id} on ${server_ip} ..."

    ssh -i "$SSH_KEY" "$USER@$server_ip" "
        cd ${REMOTE_DIR}
        mkdir -p ${LOG_DIR}/server${server_id} ${EVAL_DIR}
        SERVER_BATCHING=${SERVER_BATCHING} \
        PARALLEL_FAST_PATH=${PARALLEL_FAST_PATH} \
        nohup ./${BINARY} \
            -id=${server_id} \
            -n=${NUM_SERVERS} \
            -t=${THRESHOLD} \
            -path=${REMOTE_CONFIG} \
            -pd=true \
            -role=0 \
            -ops=${OPS} \
            -b=${BATCHSIZE} \
            -indep=${INDEP_RATIO} \
            -common=${COMMON_RATIO} \
            -et=${EVAL_TYPE} \
            -mload=${MONGO_WORKLOAD} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -log=${LOG_LEVEL} \
            -ep=${ENABLE_PRIORITY} \
            > ${LOG_DIR}/server${server_id}/output.log 2>&1 &
    "
}

# -----------------------------
# START CLIENT FUNCTION
# -----------------------------
start_client() {
    local client_id="$1"
    local client_ip="$2"

    echo "Starting Client ${client_id} on ${client_ip} ..."

    ssh -i "$SSH_KEY" "$USER@$client_ip" "
        cd ${REMOTE_DIR}
        mkdir -p ${LOG_DIR}/client${client_id} ${EVAL_DIR}/client${client_id}
        PIPELINE_MODE=${PIPELINE_MODE} \
        MAX_INFLIGHT=${MAX_INFLIGHT} \
        nohup ./${BINARY} \
            -id=${client_id} \
            -n=${NUM_SERVERS} \
            -t=${THRESHOLD} \
            -path=${REMOTE_CONFIG} \
            -ops=${OPS} \
            -et=${EVAL_TYPE} \
            -mload=${MONGO_WORKLOAD} \
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
            > ${LOG_DIR}/client${client_id}/output.log 2>&1 &
    "
}

# -----------------------------
# START SERVERS
# -----------------------------
echo "=============================================="
echo "Starting all servers (MongoDB, Heterogeneous Cluster)..."
echo "=============================================="

for i in "${!SERVER_IPS[@]}"; do
    start_server "$i" "${SERVER_IPS[$i]}"
    sleep 1
done

echo "Waiting 20 seconds for cluster + MongoDB initialization..."
sleep 20

# -----------------------------
# START CLIENTS
# -----------------------------
echo "=============================================="
echo "Starting ${NUM_CLIENTS} clients (${CLIENTS_PER_VM} per VM)..."
echo "=============================================="

client_id=${NUM_SERVERS}

for vm_ip in "${CLIENT_HOST_IPS[@]}"; do
    for ((c=0; c<CLIENTS_PER_VM; c++)); do
        if [ "$client_id" -lt "$((NUM_SERVERS + NUM_CLIENTS))" ]; then
            start_client "$client_id" "$vm_ip"
            ((client_id++))
            sleep 1
        fi
    done
done

echo "=============================================="
echo "MongoDB heterogeneous workload launched successfully"
echo "=============================================="
echo "Workload: run_workload${MONGO_WORKLOAD}.dat"
echo "EvalType: MongoDB (et=1)"
echo "Servers: ${NUM_SERVERS}, Clients: ${NUM_CLIENTS}"
echo
echo "Monitor logs:"
echo "  ssh -i ${SSH_KEY} ${USER}@${SERVER_IPS[0]} 'tail -f ${LOG_DIR}/server0/output.log'"
echo "  ssh -i ${SSH_KEY} ${USER}@${CLIENT_HOST_IPS[0]} 'tail -f ${LOG_DIR}/client5/output.log'"
echo
echo "Stop cluster:"
echo "  ./stop_mongodb_hetero.sh"
