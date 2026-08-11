#!/bin/bash
# ================================================================
# EVAL 3: Client Scaling Evaluation (HETEROGENEOUS, object-map workload)
#
# Fixed 5-server cluster. Sweeps total client count from a handful up to
# 50, with up to 2 client processes packed per client VM (matching
# config/cluster_hetero_55n_5s_50w.conf, which has 5 server slots + 50
# client slots across 25 VMs, 2 consecutive ids per VM).
#
# Fixed across the sweep: INDEP_RATIO=90.0, BATCHSIZE=1, MSG_SIZE=512.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
REMOTE_EVAL_DIR="${REMOTE_DIR}/eval"
REMOTE_LOG_DIR="${REMOTE_DIR}/logs"
BINARY="woc"
MERGE_SCRIPT="${REPO_ROOT}/merge_eval.py"

CONFIG_LOCAL="${REPO_ROOT}/config/cluster_hetero_55n_5s_50w.conf"
CONFIG_REMOTE="${REMOTE_DIR}/$(basename "$CONFIG_LOCAL")"

NUM_SERVERS=5
THRESHOLD=$(( (NUM_SERVERS - 1) / 2 ))
INDEP_RATIO=90.0
BATCHSIZE=1
MSGSIZE=512
MAX_INFLIGHT="${MAX_INFLIGHT:-5}"
PIPELINE_MODE="${PIPELINE_MODE:-true}"
NUM_OBJECTS="${NUM_OBJECTS:-1000}"
LOG_LEVEL="${LOG_LEVEL:-info}"

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"

RESULT_ROOT="${SCRIPT_DIR}/results/eval3_client_scaling"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

# Test cases: total client count (up to 2 clients packed per client VM)
CLIENT_COUNTS=(2 5 10 15 20 25 30 35 40 45 50)

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash eval_3_client_scaling.sh

Sweeps total client count over 2,5,10,15,20,25,30,35,40,45,50 against a
fixed 5-server heterogeneous cluster, packing up to 2 client processes
per client VM. INDEP_RATIO=90.0, BATCHSIZE=1, MSG_SIZE=512 fixed.

Environment overrides:
  RUNTIME_SECONDS=30   wall-clock seconds per run

Results archived under: results/eval3_client_scaling/<timestamp>/<label>/
EOF
    exit 0
fi

mkdir -p "$RUN_DIR"

(cd "$REPO_ROOT" && go build -o "${SCRIPT_DIR}/${BINARY}")

remote_exec() {
    local host=$1
    shift
    ssh -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" "$USER@$host" "$*"
}

host_for_id() {
    local id=$1
    awk -v id="$id" '$1==id {print $2; exit}' "$CONFIG_LOCAL"
}

copy_binary() {
    local host=$1
    scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" "$BINARY" "$USER@$host:$REMOTE_DIR/"
}

copy_config() {
    local host=$1
    remote_exec "$host" "mkdir -p '$REMOTE_DIR/config'"
    scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" "$CONFIG_LOCAL" "$USER@$host:$CONFIG_REMOTE"
}

distribute_all() {
    echo "Distributing binary + config to all nodes in ${CONFIG_LOCAL}..."
    mapfile -t ALL_HOSTS < <(awk '!seen[$2]++ {print $2}' "$CONFIG_LOCAL")
    for host in "${ALL_HOSTS[@]}"; do
        copy_binary "$host"
        copy_config "$host"
    done
    echo "  Distributed to ${#ALL_HOSTS[@]} unique hosts."
}

stop_nodes() {
    local ids=("$@")
    local id host
    declare -A touched_hosts=()
    for id in "${ids[@]}"; do
        host="$(host_for_id "$id")"
        touched_hosts["$host"]=1
    done
    for host in "${!touched_hosts[@]}"; do
        remote_exec "$host" "pkill -TERM -x woc 2>/dev/null || true"
    done
    sleep 2
    for host in "${!touched_hosts[@]}"; do
        remote_exec "$host" "pkill -9 -x woc 2>/dev/null || true"
    done
}

archive_case() {
    local label=$1
    shift
    local case_dir="${RUN_DIR}/${label}"
    mkdir -p "$case_dir"

    local idx=0
    local host
    for host in "$@"; do
        local node_dir="${case_dir}/node_${idx}"
        mkdir -p "$node_dir"
        scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" -r \
            "$USER@$host:${REMOTE_EVAL_DIR}/" "$node_dir/" 2>/dev/null || true
        scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" -r \
            "$USER@$host:${REMOTE_LOG_DIR}/" "$node_dir/" 2>/dev/null || true
        idx=$((idx + 1))
    done
}

merge_case_results() {
    local label=$1
    local client_count=$2
    local case_dir="${RUN_DIR}/${label}"
    local case_eval_dir="${case_dir}/eval"
    local case_merged_dir="${case_dir}/merged"
    local client_start_id=$NUM_SERVERS
    local client_end_id=$((NUM_SERVERS + client_count - 1))
    local client_id_filter="${client_start_id}-${client_end_id}"
    local server_id_filter="0-$((NUM_SERVERS - 1))"

    mkdir -p "$case_eval_dir" "$case_merged_dir"

    for node_dir in "${case_dir}"/node_*; do
        [ -d "$node_dir/eval" ] || continue
        cp -r "$node_dir/eval/"* "$case_eval_dir/" 2>/dev/null || true
    done

    echo "Merging client and server CSVs for ${label}..."
    if [ -f "$MERGE_SCRIPT" ]; then
        python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" --ids "$client_id_filter"
        python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" --servers --ids "$server_id_filter"
    else
        echo " ✗ merge_eval.py not found at ${MERGE_SCRIPT}"
    fi

    # Raw per-node eval/log copies are scratch input for the merge above;
    # a long sweep keeping all of them fills the disk.
    rm -rf "${case_dir}"/node_* "$case_eval_dir"
}

start_server() {
    local server_id=$1 host=$2

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_LOG_DIR/server_${server_id}' '$REMOTE_EVAL_DIR'
SERVER_BATCHING=false \
nohup ./$BINARY \
    -id=${server_id} \
    -n=${NUM_SERVERS} \
    -t=${THRESHOLD} \
    -path='$CONFIG_REMOTE' \
    -pd=true \
    -role=0 \
    -ops=0 \
    -b=${BATCHSIZE} \
    -indep=${INDEP_RATIO} \
    -numobjects=${NUM_OBJECTS} \
    -et=0 \
    -ms=${MSGSIZE} \
    -mode=1 \
    -log=${LOG_LEVEL} \
    -ep=true \
    > '$REMOTE_LOG_DIR/server_${server_id}/output.log' 2>&1 &
EOF
}

start_client() {
    local client_id=$1 host=$2
    local pin_server=$((client_id % NUM_SERVERS))

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_LOG_DIR/client_${client_id}' '$REMOTE_EVAL_DIR'
PIPELINE_MODE=${PIPELINE_MODE} \
MAX_INFLIGHT=${MAX_INFLIGHT} \
nohup ./$BINARY \
    -id=${client_id} \
    -n=${NUM_SERVERS} \
    -t=${THRESHOLD} \
    -path='$CONFIG_REMOTE' \
    -ops=0 \
    -et=0 \
    -pd=true \
    -role=1 \
    -b=${BATCHSIZE} \
    -indep=${INDEP_RATIO} \
    -numobjects=${NUM_OBJECTS} \
    -bcomp=object-specific \
    -ms=${MSGSIZE} \
    -mode=1 \
    -pinserver=${pin_server} \
    -log=${LOG_LEVEL} \
    > '$REMOTE_LOG_DIR/client_${client_id}/output.log' 2>&1 &
EOF
}

run_case() {
    local client_count=$1
    local label="clients_${client_count}"
    local server_ids=()
    local client_ids=()
    local i

    for ((i = 0; i < NUM_SERVERS; i++)); do server_ids+=("$i"); done
    for ((i = 0; i < client_count; i++)); do client_ids+=("$((NUM_SERVERS + i))"); done

    echo ""
    echo "=================================================="
    echo "Running: ${label} (servers=${NUM_SERVERS}, clients=${client_count})"
    echo "=================================================="

    for server_id in "${server_ids[@]}"; do
        start_server "$server_id" "$(host_for_id "$server_id")"
        sleep 1
    done

    sleep 15

    for client_id in "${client_ids[@]}"; do
        start_client "$client_id" "$(host_for_id "$client_id")"
        sleep 0.2
    done

    sleep "$RUNTIME_SECONDS"

    local all_ids=("${server_ids[@]}" "${client_ids[@]}")
    stop_nodes "${all_ids[@]}"

    mapfile -t archive_hosts < <(
        { for id in "${all_ids[@]}"; do host_for_id "$id"; done; } | awk '!seen[$0]++'
    )
    archive_case "$label" "${archive_hosts[@]}"
    merge_case_results "$label" "$client_count"
}

cleanup() {
    local all_ids=()
    local i
    for ((i = 0; i < NUM_SERVERS; i++)); do all_ids+=("$i"); done
    for ((i = 0; i < 50; i++)); do all_ids+=("$((NUM_SERVERS + i))"); done
    stop_nodes "${all_ids[@]}" 2>/dev/null || true
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              CLIENT SCALING EVALUATION (HETEROGENEOUS)          ║"
echo "║      5 servers fixed, clients swept 2..50 (2 per client VM)     ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

distribute_all

for client_count in "${CLIENT_COUNTS[@]}"; do
    run_case "$client_count"
done

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size "$NUM_SERVERS"

echo ""
echo "=================================================="
echo " Client scaling evaluation sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
