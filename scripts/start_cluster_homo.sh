#!/bin/bash
# ================================================================
# WOC Cloud Cluster Launcher - HOMOGENEOUS CLUSTER
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
CONFIG_LOCAL="${REPO_ROOT}/config/cluster_homo.conf"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_homo.conf"
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
NUM_CLIENTS="${NUM_CLIENTS:-2}"
OPS=0
EVAL_TYPE=0
BATCHSIZE="${BATCHSIZE:-10}"
MSG_SIZE=512
MODE=1
INDEP_RATIO="${INDEP_RATIO:-100.0}"
NUM_OBJECTS="${NUM_OBJECTS:-1000}"    # size of the fixed, hash-ring-mapped object pool
BATCH_COMPOSITION="object-specific"
PIPELINE_MODE="${PIPELINE_MODE:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-5}"
USE_ADAPTIVE_LIMITER="false"
LOG_LEVEL="${LOG_LEVEL:-info}"
ENABLE_PRIORITY="${ENABLE_PRIORITY:-true}"
LATENCY_DEBUG="false"
SERVER_BATCHING="false"

# -----------------------------
# CLOUD IP LIST
# -----------------------------
# The homo pool is one flat, identically-typed IP list (config/cluster_homo.conf,
# 39+ entries) -- unlike the hetero pool's per-size config files, every
# (NUM_SERVERS, NUM_CLIENTS) combination just slices a prefix of the same
# file, since server id i always maps to row i regardless of cluster size.
mapfile -t ALL_IPS < <(awk 'NF >= 2 { print $2 }' "$CONFIG_LOCAL")
REQUIRED_IPS=$((NUM_SERVERS + NUM_CLIENTS))
if [ "${#ALL_IPS[@]}" -lt "$REQUIRED_IPS" ]; then
    echo "ERROR: ${CONFIG_LOCAL} has ${#ALL_IPS[@]} IPs, need ${REQUIRED_IPS} (NUM_SERVERS=${NUM_SERVERS} + NUM_CLIENTS=${NUM_CLIENTS})"
    exit 1
fi

SERVER_IPS=("${ALL_IPS[@]:0:$NUM_SERVERS}")
CLIENT_HOST_IPS=("${ALL_IPS[@]:$NUM_SERVERS:$NUM_CLIENTS}")

CLIENTS_PER_VM=1

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
    copy_binary $ip
    copy_config $ip
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
    local PIN_SERVER=$((CLIENT_ID % NUM_SERVERS))

    echo " Starting Client $CLIENT_ID on $CLIENT_IP (pinserver=${PIN_SERVER})..."

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
            -numobjects=${NUM_OBJECTS} \
            -bcomp=${BATCH_COMPOSITION} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -pinserver=${PIN_SERVER} \
            -log=${LOG_LEVEL} \
            > ${LOG_DIR}/client${CLIENT_ID}/output.log 2>&1 &
    "
}

# -----------------------------
# START SERVERS
# -----------------------------
echo "=============================================="
echo "Starting all servers (Homogeneous Cluster)..."
echo "=============================================="

for i in "${!SERVER_IPS[@]}"; do
    start_server $i "${SERVER_IPS[$i]}"
    sleep 1
done

echo "Waiting 15 seconds for cluster stabilization..."
sleep 15

# -----------------------------
# START CLIENTS
# -----------------------------
echo "=============================================="
echo "Starting ${NUM_CLIENTS} clients (${CLIENTS_PER_VM} per VM)..."
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
echo " WOC homogeneous cluster launched successfully!"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Cluster Type: HOMOGENEOUS (cb8-30gb-280)"
echo "  Servers: ${NUM_SERVERS} (IDs 0-$((NUM_SERVERS-1)))"
echo "  Clients: ${NUM_CLIENTS} (IDs ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "  Config File: cluster_homo.conf"
echo ""
echo "Monitor logs:"
echo "  ssh -i $SSH_KEY ubuntu@${SERVER_IPS[0]} 'tail -f ${LOG_DIR}/server0/output.log'"
echo "  ssh -i $SSH_KEY ubuntu@${CLIENT_HOST_IPS[0]} 'tail -f ${LOG_DIR}/client5/output.log'"
echo ""
echo "Stop all processes:"
echo "  ./stop_cluster_homo.sh"
echo "=============================================="
