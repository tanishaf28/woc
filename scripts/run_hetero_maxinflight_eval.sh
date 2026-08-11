#!/bin/bash
# ================================================================
# MAX-INFLIGHT EVALUATION RUNNER (HETEROGENEOUS) — 4 cluster sizes
#
# The only sweep that wasn't subsumed into run_plainmsg_evals.sh (I2D/
# batch/msgsize) when the old run_hetero_plainmsg_evals.sh /
# run_hetero_plainmsg.sh drivers were consolidated. Sweeps client-side
# MAX_INFLIGHT pipelining depth against heterogeneous clusters of
# n = 3, 5, 7, 11 replicas.
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
CLIENT_COUNT=2

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero_maxinflight_eval"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

SERVER_COUNTS=(3 5 7 11)

declare -A CONFIG_BY_COUNT=(
    [3]="config/cluster_hetero_3n_2s_1w.conf"
    [5]="config/cluster_hetero_5n_2s_3w.conf"
    [7]="config/cluster_hetero_7n_3s_4w.conf"
    [11]="config/cluster_hetero_11n_4s_7w.conf"
)

# Fixed across the sweep.
THRESHOLD=1
INDEP_RATIO=90.0
BATCHSIZE=1
MSGSIZE=512
MAX_INFLIGHT_VALUES=(1 2 3 4 5 8 10 15 20 25 30 35 40)

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_hetero_maxinflight_eval.sh

Sweeps MAX_INFLIGHT over 1,2,3,4,5,8,10,15,20,25,30,35,40 with
INDEP_RATIO=90.0, BATCHSIZE=1, MSG_SIZE=512 fixed, across cluster sizes
n = 3, 5, 7, 11 (heterogeneous configs).

Environment overrides:
  RUNTIME_SECONDS=30   wall-clock seconds per run

Results archived under: results/hetero_maxinflight_eval/<timestamp>/n<N>/<label>/
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

copy_binary() {
    local host=$1
    scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" "$BINARY" "$USER@$host:$REMOTE_DIR/"
}

copy_config() {
    local host=$1
    local config_local=$2
    local config_remote="$REMOTE_DIR/$(basename "$config_local")"
    remote_exec "$host" "mkdir -p '$REMOTE_DIR/config'"
    scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" "$config_local" "$USER@$host:$config_remote"
}

read_cluster_ips() {
    local config_local=$1
    local server_count=$2

    mapfile -t ALL_IPS < <(awk 'NF >= 2 { print $2 }' "$config_local")
    local required_count=$((server_count + CLIENT_COUNT))
    if [ "${#ALL_IPS[@]}" -lt "$required_count" ]; then
        echo "ERROR: ${config_local} does not contain enough IPs for ${server_count} servers and ${CLIENT_COUNT} clients"
        exit 1
    fi

    SERVER_IPS=("${ALL_IPS[@]:0:server_count}")
    CLIENT_IPS=("${ALL_IPS[@]:server_count:CLIENT_COUNT}")
}

stop_nodes() {
    local ips=("$@")
    local host
    for host in "${ips[@]}"; do
        remote_exec "$host" "pkill -TERM -x woc 2>/dev/null || true"
    done
    sleep 2
    for host in "${ips[@]}"; do
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
    local server_count=$2
    local client_count=$3
    local case_dir="${RUN_DIR}/${label}"
    local case_eval_dir="${case_dir}/eval"
    local case_merged_dir="${case_dir}/merged"
    local client_start_id=$server_count
    local client_end_id=$((server_count + client_count - 1))
    local client_id_filter="${client_start_id}-${client_end_id}"
    local server_id_filter="0-$((server_count - 1))"

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

    # Raw per-node eval/log copies (node_*/, eval/) are only scratch input for
    # the merge above; a long sweep keeping all of them fills the disk
    # (each holds a full copy of every CSV/log on that node). Once merged/
    # has the case's output, drop the raw copies.
    rm -rf "${case_dir}"/node_* "$case_eval_dir"
}

start_server() {
    local server_id=$1 host=$2 server_count=$3 config_remote=$4

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_LOG_DIR/server_${server_count}_${server_id}' '$REMOTE_EVAL_DIR'
SERVER_BATCHING=false \
nohup ./$BINARY \
    -id=${server_id} \
    -n=${server_count} \
    -t=${THRESHOLD} \
    -path='$config_remote' \
    -pd=true \
    -role=0 \
    -ops=0 \
    -b=${BATCHSIZE} \
    -indep=${INDEP_RATIO} \
    -numobjects=1000 \
    -et=0 \
    -ms=${MSGSIZE} \
    -mode=1 \
    -log=info \
    -ep=true \
    > '$REMOTE_LOG_DIR/server_${server_count}_${server_id}/output.log' 2>&1 &
EOF
}

start_client() {
    local client_id=$1 host=$2 server_count=$3 config_remote=$4 max_inflight=$5
    local pin_server=$((client_id % server_count))

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_LOG_DIR/client_${server_count}_${client_id}' '$REMOTE_EVAL_DIR'
PIPELINE_MODE=true \
MAX_INFLIGHT=${max_inflight} \
nohup ./$BINARY \
    -id=${client_id} \
    -n=${server_count} \
    -t=${THRESHOLD} \
    -path='$config_remote' \
    -ops=0 \
    -et=0 \
    -pd=true \
    -role=1 \
    -b=${BATCHSIZE} \
    -indep=${INDEP_RATIO} \
    -numobjects=1000 \
    -bcomp=object-specific \
    -ms=${MSGSIZE} \
    -mode=1 \
    -pinserver=${pin_server} \
    -log=info \
    > '$REMOTE_LOG_DIR/client_${server_count}_${client_id}/output.log' 2>&1 &
EOF
}

run_case() {
    local server_count=$1 max_inflight=$2
    local label="eval_maxinflight_${max_inflight}"
    local config_local="${REPO_ROOT}/${CONFIG_BY_COUNT[$server_count]}"
    local config_remote="${REMOTE_DIR}/$(basename "$config_local")"
    local full_label="n${server_count}/${label}"

    read_cluster_ips "$config_local" "$server_count"

    echo ""
    echo "=================================================="
    echo "Running: ${full_label}"
    echo "  Servers=${server_count} max_inflight=${max_inflight}"
    echo "=================================================="

    for host in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        copy_binary "$host"
        copy_config "$host" "$config_local"
    done

    for server_id in "${!SERVER_IPS[@]}"; do
        start_server "$server_id" "${SERVER_IPS[$server_id]}" "$server_count" "$config_remote"
        sleep 1
    done

    sleep 15

    local client_id="$server_count"
    for client_vm_idx in "${!CLIENT_IPS[@]}"; do
        start_client "$client_id" "${CLIENT_IPS[$client_vm_idx]}" "$server_count" "$config_remote" "$max_inflight"
        client_id=$((client_id + 1))
        sleep 1
    done

    sleep "$RUNTIME_SECONDS"

    stop_nodes "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"
    archive_case "$full_label" "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"
    merge_case_results "$full_label" "$server_count" "${#CLIENT_IPS[@]}"
}

cleanup() {
    stop_nodes "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}" 2>/dev/null || true
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          MAX-INFLIGHT EVALUATION RUNNER (HETEROGENEOUS)         ║"
echo "║         Cluster sizes: n = 3, 5, 7, 11 (heterogeneous)         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

for server_count in "${SERVER_COUNTS[@]}"; do
    for max_inflight in "${MAX_INFLIGHT_VALUES[@]}"; do
        run_case "$server_count" "$max_inflight"
    done
done

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR"

echo ""
echo "=================================================="
echo " Max-inflight evaluation sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
