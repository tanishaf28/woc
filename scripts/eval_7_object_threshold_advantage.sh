#!/bin/bash
# ================================================================
# EVAL 7: Object-Weight Advantage — per-object threshold tuning
#
# Demonstrates the paper's per-object independence claim (§3.2): CORA can
# give one specific object its own failure threshold (and therefore its own
# fast-path weight vector/quorum size), without touching every other
# object's threshold. No other protocol here (Cabinet, Raft) can do this at
# all — they have exactly one threshold for the whole system, so tuning
# fault-tolerance/latency trade-offs for one hot object always means paying
# (or saving) the same cost for every other object too.
#
# Design: a fixed 5-node heterogeneous cluster runs with a fixed GLOBAL
# threshold (COLD_THRESHOLD) for every object except one designated "hot"
# object (obj-0), whose own threshold (-hotobjthreshold) is swept
# independently across HOT_THRESHOLDS. Two clients run concurrently in every
# test case:
#   - client 0 (-targetobjid=obj-0): measures the HOT object's own
#     throughput/latency as ITS threshold varies.
#   - client 1 (-targetobjid=obj-1): measures a COLD object's throughput/
#     latency, which should stay flat across the whole sweep — direct
#     evidence that changing one object's threshold doesn't affect any
#     other object, which is the actual claim being tested here.
#
# -hotobjthreshold/-targetobjid are new WOC-only flags (parameters.go/
# main.go/client.go) added specifically for this eval.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

RESULT_ROOT="${SCRIPT_DIR}/results/eval7_object_threshold_advantage"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

NUM_SERVERS=5
COLD_THRESHOLD="${COLD_THRESHOLD:-2}"     # every object except obj-0 uses this -t
HOT_THRESHOLDS_STR="${HOT_THRESHOLDS:-1 2 3 4 5}"
read -r -a HOT_THRESHOLDS <<< "$HOT_THRESHOLDS_STR"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
NUM_OBJECTS="${NUM_OBJECTS:-1000}"
INDEP_RATIO="${INDEP_RATIO:-90.0}"
MSG_SIZE=512
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_10c.conf"
CLUSTER_ACTIVE=false

# Same 5-server / first-2-client slice as start_cluster_hetero.sh's pool, for
# IP consistency with every other eval in this repo.
SERVER_IPS=(
    "192.168.73.59"  "192.168.73.243" "192.168.73.117" "192.168.73.16"
    "192.168.73.94"
)
CLIENT_IPS=(
    "192.168.73.159" "192.168.73.84"
)

mkdir -p "$RUN_DIR"

ssh_exec() {
    local host=$1
    shift
    ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$USER@$host" "$*"
}

build_and_copy() {
    echo "Building WOC binary locally..."
    (cd "$REPO_ROOT" && go build -o "${SCRIPT_DIR}/${BINARY}")
    echo "Copying binary + config to all nodes..."
    for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        ssh_exec "$ip" "mkdir -p '$REMOTE_DIR/config' '$LOG_DIR' '$EVAL_DIR'"
        scp -i "$SSH_KEY" "${SCRIPT_DIR}/${BINARY}" "$USER@$ip:$REMOTE_DIR/" >/dev/null
        scp -i "$SSH_KEY" "${REPO_ROOT}/config/cluster_hetero_5n_10c.conf" "$USER@$ip:$REMOTE_DIR/config/" >/dev/null
    done
}

start_server() {
    local sid=$1 ip=$2 hot_t=$3
    ssh_exec "$ip" "
        cd $REMOTE_DIR
        mkdir -p '${LOG_DIR}/server${sid}' '${EVAL_DIR}'
        nohup ./${BINARY} \
            -id=${sid} -n=${NUM_SERVERS} -t=${COLD_THRESHOLD} \
            -path=${CONFIG_PATH} -pd=true -role=0 -ops=0 -b=1 \
            -indep=${INDEP_RATIO} -numobjects=${NUM_OBJECTS} \
            -et=0 -ms=${MSG_SIZE} -mode=1 -log=info -ep=true \
            -hotobjthreshold=${hot_t} -hotobjid=obj-0 \
            > '${LOG_DIR}/server${sid}/output.log' 2>&1 &
    "
}

start_client() {
    local cid=$1 ip=$2 target_server=$3 target_obj=$4 label=$5
    ssh_exec "$ip" "
        cd $REMOTE_DIR
        mkdir -p '${LOG_DIR}/client${cid}' '${EVAL_DIR}/client${cid}'
        PIPELINE_MODE=true MAX_INFLIGHT=5 \
        nohup ./${BINARY} \
            -id=${cid} -n=${NUM_SERVERS} -t=${COLD_THRESHOLD} \
            -path=${CONFIG_PATH} -ops=0 -et=0 -pd=true -role=1 -b=1 \
            -indep=${INDEP_RATIO} -numobjects=${NUM_OBJECTS} \
            -ms=${MSG_SIZE} -mode=1 -log=info \
            -pinserver=${target_server} -targetobjid=${target_obj} \
            -suffix=${label} \
            > '${LOG_DIR}/client${cid}/output.log' 2>&1 &
    "
}

stop_all() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        ssh_exec "$ip" "pkill -TERM -x ${BINARY} 2>/dev/null || true" &
    done
    wait
    sleep 3
    for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        ssh_exec "$ip" "pkill -9 -x ${BINARY} 2>/dev/null || true" &
    done
    wait
    sleep 1
}

collect_case() {
    local label=$1
    local dest="${RUN_DIR}/${label}"
    mkdir -p "${dest}/client0_hot" "${dest}/client1_cold" "${dest}/server0"
    scp -i "$SSH_KEY" -r "$USER@${CLIENT_IPS[0]}:${EVAL_DIR}/client5/"* "${dest}/client0_hot/" 2>/dev/null || true
    scp -i "$SSH_KEY" -r "$USER@${CLIENT_IPS[1]}:${EVAL_DIR}/client6/"* "${dest}/client1_cold/" 2>/dev/null || true
    scp -i "$SSH_KEY" -r "$USER@${SERVER_IPS[0]}:${EVAL_DIR}/server0/"* "${dest}/server0/" 2>/dev/null || true
    echo "  Archived to: $dest"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_all || true
    fi
}
trap cleanup EXIT

echo "=================================================="
echo " EVAL 7: Object-Weight Advantage (per-object threshold)"
echo " Cluster: n=${NUM_SERVERS} heterogeneous | cold -t=${COLD_THRESHOLD}"
echo " Hot-object (obj-0) threshold sweep: ${HOT_THRESHOLDS[*]}"
echo "=================================================="

build_and_copy

for hot_t in "${HOT_THRESHOLDS[@]}"; do
    label="hot_t${hot_t}_cold_t${COLD_THRESHOLD}"
    echo ""
    echo "--- Case: ${label} ---"

    for i in "${!SERVER_IPS[@]}"; do
        start_server "$i" "${SERVER_IPS[$i]}" "$hot_t"
        sleep 1
    done
    CLUSTER_ACTIVE=true
    echo "Waiting 15s for cluster stabilization..."
    sleep 15

    # client IDs follow NUM_SERVERS (5), matching every other eval's
    # convention (client N+0, N+1, ...): client5 -> hot (obj-0), client6 ->
    # cold (obj-1). Both pinned to server 0 so the routing win from the
    # owner-aware client fix is exercised identically for both.
    start_client 5 "${CLIENT_IPS[0]}" 0 "obj-0" "hot"
    start_client 6 "${CLIENT_IPS[1]}" 0 "obj-1" "cold"

    echo "Running for ${RUNTIME_SECONDS}s..."
    sleep "$RUNTIME_SECONDS"

    stop_all
    CLUSTER_ACTIVE=false
    collect_case "$label"

    echo "Cooling down..."
    sleep 5
done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size "$NUM_SERVERS" || true

echo ""
echo "=================================================="
echo " EVAL 7 complete. Results in: $RUN_DIR"
echo " Expect: client0_hot's throughput/latency to move with hot_t;"
echo " client1_cold's to stay flat across every case."
echo "=================================================="
