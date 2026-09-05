#!/bin/bash
# ================================================================
# READ RATIO EVALUATION RUNNER (HOMOGENEOUS) — WOC
#
# Sweeps -readratio (% of ops that are reads vs writes) over 0/25/50/75/100
# against homogeneous clusters of n = 3, 5, 7, 11 replicas, indep=90.0/
# batch=1/msgsize=512 fixed -- matches eval_4_read_ratio.sh's values
# exactly (see that script for why the sweep points were chosen), but
# across the homo cluster-size sweep the way run_homo_plainmsg_evals.sh
# does for eval1/2/3, instead of eval_4's single fixed 5-node cluster.
#
# -readmode is fixed for the whole sweep (default: safe) rather than
# crossed with -readratio, same choice as eval_4_read_ratio.sh -- override
# via READ_MODE=fast for a separate "cost of safety" comparison run. Note
# dependent-object reads always use the quorum path regardless of
# -readmode (see consensus.go's handleRead) -- only independent-object
# reads actually change behavior between fast/safe.
#
# THRESHOLD scales as floor((n-1)/2) per size, matching
# start_cluster_hetero.sh's own default formula (NOT eval_4_read_ratio.sh's
# hardcoded THRESHOLD=1, which only happens to undershoot that formula at
# its fixed n=5 case and isn't reused here).
#
# Uses the same build-once/scp-per-case/raw-SSH-launch idiom as
# run_homo_plainmsg_evals.sh (config/cluster_homo.conf is a flat pool, so
# each size just slices a prefix -- no per-size config files needed).
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
CONFIG_LOCAL="${REPO_ROOT}/config/cluster_homo.conf"

RESULT_ROOT="${SCRIPT_DIR}/results/homo_readratio_eval"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
INDEP_RATIO="${INDEP_RATIO:-90.0}"
BATCHSIZE=1
MSGSIZE=512
NUM_OBJECTS="${NUM_OBJECTS:-1000}"
PIPELINE_MODE="${PIPELINE_MODE:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-5}"
LOG_LEVEL="${LOG_LEVEL:-info}"
READ_MODE="${READ_MODE:-safe}"  # fast|safe, fixed for the whole sweep

READ_RATIO_VALUES=(0 25 50 75 100)  # matches eval_4_read_ratio.sh exactly

SERVER_COUNTS=(3 5 7 11)
if [ -n "${CLUSTER_SIZES:-}" ]; then
    read -r -a SERVER_COUNTS <<< "$CLUSTER_SIZES"
fi

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_homo_readratio_eval.sh

Sweeps READ_RATIO over 0,25,50,75,100 (matches eval_4_read_ratio.sh) with
INDEP_RATIO=90.0, BATCHSIZE=1, MSG_SIZE=512 fixed, across cluster sizes
n = 3, 5, 7, 11 (homogeneous, sliced from config/cluster_homo.conf).

Environment overrides:
  RUNTIME_SECONDS=30          wall-clock seconds per run
  READ_MODE=safe               fast|safe, applied to every case in this run
  CLUSTER_SIZES="3 5 7 11"    override the cluster-size sweep

Results archived under: results/homo_readratio_eval/<timestamp>/n<N>/<label>/
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
    remote_exec "$host" "mkdir -p '$REMOTE_DIR/config'"
    scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" "$CONFIG_LOCAL" "$USER@$host:$REMOTE_DIR/config/"
}

read_cluster_ips() {
    local server_count=$1

    mapfile -t ALL_IPS < <(awk 'NF >= 2 { print $2 }' "$CONFIG_LOCAL")
    local required_count=$((server_count + CLIENT_COUNT))
    if [ "${#ALL_IPS[@]}" -lt "$required_count" ]; then
        echo "ERROR: ${CONFIG_LOCAL} does not contain enough IPs for ${server_count} servers and ${CLIENT_COUNT} clients"
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

    rm -rf "${case_dir}"/node_* "$case_eval_dir"
}

start_server() {
    local server_id=$1 host=$2 server_count=$3 config_remote=$4 threshold=$5

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_LOG_DIR/server_${server_count}_${server_id}' '$REMOTE_EVAL_DIR'
SERVER_BATCHING=false \
nohup ./$BINARY \
    -id=${server_id} \
    -n=${server_count} \
    -t=${threshold} \
    -path='$config_remote' \
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
    > '$REMOTE_LOG_DIR/server_${server_count}_${server_id}/output.log' 2>&1 &
EOF
}

start_client() {
    local client_id=$1 host=$2 server_count=$3 config_remote=$4 threshold=$5 readratio=$6
    local pin_server=$((client_id % server_count))

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_LOG_DIR/client_${server_count}_${client_id}' '$REMOTE_EVAL_DIR'
PIPELINE_MODE=${PIPELINE_MODE} \
MAX_INFLIGHT=${MAX_INFLIGHT} \
nohup ./$BINARY \
    -id=${client_id} \
    -n=${server_count} \
    -t=${threshold} \
    -path='$config_remote' \
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
    -readratio=${readratio} \
    -readmode=${READ_MODE} \
    > '$REMOTE_LOG_DIR/client_${server_count}_${client_id}/output.log' 2>&1 &
EOF
}

run_case() {
    local server_count=$1 label=$2 readratio=$3
    local threshold=$(( (server_count - 1) / 2 ))
    local config_remote="${REMOTE_DIR}/config/$(basename "$CONFIG_LOCAL")"
    local full_label="n${server_count}/${label}"

    read_cluster_ips "$server_count"

    echo ""
    echo "=================================================="
    echo "Running: ${full_label}"
    echo "  Servers=${server_count} t=${threshold} indep=${INDEP_RATIO} readratio=${readratio} readmode=${READ_MODE}"
    echo "=================================================="

    for host in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        copy_binary "$host"
        copy_config "$host"
    done

    for server_id in "${!SERVER_IPS[@]}"; do
        start_server "$server_id" "${SERVER_IPS[$server_id]}" "$server_count" "$config_remote" "$threshold"
        sleep 1
    done

    sleep 15

    local client_id="$server_count"
    for client_vm_idx in "${!CLIENT_IPS[@]}"; do
        start_client "$client_id" "${CLIENT_IPS[$client_vm_idx]}" "$server_count" "$config_remote" "$threshold" "$readratio"
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
echo "║          READ RATIO EVALUATION RUNNER (HOMOGENEOUS) — WOC        ║"
echo "║         Cluster sizes: n = 3, 5, 7, 11 (homogeneous)           ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

for server_count in "${SERVER_COUNTS[@]}"; do
    for read_ratio in "${READ_RATIO_VALUES[@]}"; do
        run_case "$server_count" "eval_readratio_${read_ratio}" "$read_ratio"
    done
done

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR"

echo ""
echo "=================================================="
echo " Read ratio evaluation sweep complete (WOC, homogeneous)"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
