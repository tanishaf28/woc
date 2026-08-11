#!/bin/bash
# ================================================================
# PLAIN-MSG EVALUATION RUNNER — single script, 4 cluster sizes
#
# Replaces the server-count-fixed sweep logic previously split across
# eval_hetero_cluster_scale.sh (server-count scaling only) and
# run_hetero_plainmsg_evals.sh's eval1/eval_batching/eval_msgsize blocks
# (fixed at 5 servers). This runs all three sweeps across every cluster
# size in one pass:
#   eval1  Independent/Dependent (I2D) ratio sweep   (Fig. 3)
#   eval2  Batch size sweep                          (Fig. 5)
#   eval3  Message size sweep
# against heterogeneous clusters of n = 3, 5, 7, 11 replicas.
#
# Does NOT cover: max-inflight sweep, netem/fault injection, crash
# injection — those stay in their existing dedicated scripts.
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

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
EVAL_ONLY="${1:-all}"

# Client scaling mode:
#   SCALE_CLIENTS=false (default) — fixed client count, round-robin routing.
#     Isolates pure quorum-overhead cost as N grows (load per server is constant).
#   SCALE_CLIENTS=true — clients = min(N,10), each pinned to one server.
#     Shows WOC/EPaxos scaling vs Cabinet bottleneck.
SCALE_CLIENTS="${SCALE_CLIENTS:-false}"
FIXED_CLIENT_COUNT="${FIXED_CLIENT_COUNT:-2}"

RESULT_ROOT="${SCRIPT_DIR}/results/plainmsg_evals"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

SERVER_COUNTS=(3 5 7 11)
if [ -n "${CLUSTER_SIZES:-}" ]; then
    read -r -a SERVER_COUNTS <<< "$CLUSTER_SIZES"
fi

declare -A CONFIG_BY_COUNT=(
    [3]="config/cluster_hetero_3n_10c.conf"
    [5]="config/cluster_hetero_5n_10c.conf"
    [7]="config/cluster_hetero_7n_10c.conf"
    [11]="config/cluster_hetero_11n_10c.conf"
)

# Fixed across all sweeps unless the sweep itself varies it.
THRESHOLD=1
MAX_INFLIGHT=5
DEFAULT_INDEP=90.0
DEFAULT_BATCHSIZE=1
DEFAULT_MSGSIZE=512

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_plainmsg_evals.sh [eval1|eval2|eval3|all]

  eval1   Independent ratio sweep (8 I2D points), batch=1, msgsize=512
  eval2   Batch size sweep (1,10,50,100,500,1000,2000), I2D=90/10, msgsize=512
  eval3   Message size sweep (64,512,1024,2048,4096), I2D=90/10, batch=1
  all     Run eval1, eval2, eval3 (default)

Each eval runs across cluster sizes n = 3, 5, 7, 11 (heterogeneous configs).

Environment overrides:
  RUNTIME_SECONDS=30          wall-clock seconds per run
  CLUSTER_SIZES="3 5 7 11"    override the cluster-size sweep (e.g. "5")
  SCALE_CLIENTS=false|true    fixed FIXED_CLIENT_COUNT clients vs min(N,10) pinned clients
  FIXED_CLIENT_COUNT=2        client count when SCALE_CLIENTS=false

Results archived under: results/plainmsg_evals/<timestamp>/n<N>/<label>/
EOF
    exit 0
fi

case "$EVAL_ONLY" in
    all|eval1|eval2|eval3) ;;
    *)
        echo "ERROR: unknown selector '${EVAL_ONLY}'. Run with --help."
        exit 1
        ;;
esac

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
    local client_count=${3:-2}

    mapfile -t ALL_IPS < <(awk 'NF >= 2 { print $2 }' "$config_local")
    local required_count=$((server_count + client_count))
    if [ "${#ALL_IPS[@]}" -lt "$required_count" ]; then
        echo "ERROR: ${config_local} does not contain enough IPs for ${server_count} servers and ${client_count} clients"
        exit 1
    fi

    SERVER_IPS=("${ALL_IPS[@]:0:server_count}")
    CLIENT_IPS=("${ALL_IPS[@]:server_count:client_count}")
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
    # the merge above; an 80-case sweep keeping all of them fills the disk
    # (each holds a full copy of every CSV/log on that node). Once merged/
    # has the case's output, drop the raw copies.
    rm -rf "${case_dir}"/node_* "$case_eval_dir"
}

# start_server/start_client take the three swept knobs explicitly so one
# run_case can serve all three eval families.
start_server() {
    local server_id=$1 host=$2 server_count=$3 config_remote=$4
    local indep=$5 batchsize=$6 msgsize=$7

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
    -b=${batchsize} \
    -indep=${indep} \
    -numobjects=1000 \
    -et=0 \
    -ms=${msgsize} \
    -mode=1 \
    -log=info \
    -ep=true \
    > '$REMOTE_LOG_DIR/server_${server_count}_${server_id}/output.log' 2>&1 &
EOF
}

start_client() {
    local client_id=$1 host=$2 server_count=$3 config_remote=$4
    local indep=$5 batchsize=$6 msgsize=$7
    local pin_server=${8:--1}

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_LOG_DIR/client_${server_count}_${client_id}' '$REMOTE_EVAL_DIR'
PIPELINE_MODE=true \
MAX_INFLIGHT=${MAX_INFLIGHT} \
nohup ./$BINARY \
    -id=${client_id} \
    -n=${server_count} \
    -t=${THRESHOLD} \
    -path='$config_remote' \
    -ops=0 \
    -et=0 \
    -pd=true \
    -role=1 \
    -b=${batchsize} \
    -indep=${indep} \
    -numobjects=1000 \
    -bcomp=object-specific \
    -ms=${msgsize} \
    -mode=1 \
    -log=info \
    -pinserver=${pin_server} \
    > '$REMOTE_LOG_DIR/client_${server_count}_${client_id}/output.log' 2>&1 &
EOF
}

# run_case: one (server_count, knob combination) trial — start, wait
# RUNTIME_SECONDS, stop, archive, merge.
run_case() {
    local server_count=$1 label=$2
    local indep=$3 batchsize=$4 msgsize=$5
    local client_count
    if [ "${SCALE_CLIENTS}" = "true" ]; then
        client_count=$(( server_count < 10 ? server_count : 10 ))
    else
        client_count=${FIXED_CLIENT_COUNT}
    fi
    local config_local="${REPO_ROOT}/${CONFIG_BY_COUNT[$server_count]}"
    local config_remote="${REMOTE_DIR}/$(basename "$config_local")"
    local full_label="n${server_count}/${label}"

    read_cluster_ips "$config_local" "$server_count" "$client_count"

    echo ""
    echo "=================================================="
    echo "Running: ${full_label}"
    echo "  Servers=${server_count} Clients=${client_count} (pinned) indep=${indep} batch=${batchsize} msgsize=${msgsize}"
    echo "=================================================="

    for host in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        copy_binary "$host"
        copy_config "$host" "$config_local"
    done

    for server_id in "${!SERVER_IPS[@]}"; do
        start_server "$server_id" "${SERVER_IPS[$server_id]}" "$server_count" "$config_remote" \
            "$indep" "$batchsize" "$msgsize"
        sleep 1
    done

    sleep 15

    local client_id="$server_count"
    for client_vm_idx in "${!CLIENT_IPS[@]}"; do
        local pin_server=$(( client_id - server_count ))
        start_client "$client_id" "${CLIENT_IPS[$client_vm_idx]}" "$server_count" "$config_remote" \
            "$indep" "$batchsize" "$msgsize" "$pin_server"
        client_id=$((client_id + 1))
        sleep 1
    done

    sleep "$RUNTIME_SECONDS"

    stop_nodes "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"
    archive_case "$full_label" "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"
    merge_case_results "$full_label" "$server_count" "$client_count"
}

cleanup() {
    stop_nodes "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}" 2>/dev/null || true
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              PLAIN-MSG EVALUATION RUNNER                       ║"
echo "║         Cluster sizes: n = 3, 5, 7, 11 (heterogeneous)         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

for server_count in "${SERVER_COUNTS[@]}"; do

    # ============================================================
    # EVAL 1: Independent ratio sweep
    # ============================================================
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval1" ]]; then
        for indep in 100.0 90.0 80.0 60.0 40.0 20.0 10.0 0.0; do
            run_case "$server_count" "eval1_indep_${indep}" \
                "$indep" "$DEFAULT_BATCHSIZE" "$DEFAULT_MSGSIZE"
        done
    fi

    # ============================================================
    # EVAL 2: batch size sweep
    # ============================================================
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval2" ]]; then
        for batch_size in 1 10 50 100 500 1000 2000; do
            run_case "$server_count" "eval2_batch_${batch_size}" \
                "$DEFAULT_INDEP" "$batch_size" "$DEFAULT_MSGSIZE"
        done
    fi

    # ============================================================
    # EVAL 3: message size sweep
    # ============================================================
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval3" ]]; then
        for msg_size in 64 512 1024 2048 4096; do
            run_case "$server_count" "eval3_msgsize_${msg_size}" \
                "$DEFAULT_INDEP" "$DEFAULT_BATCHSIZE" "$msg_size"
        done
    fi

done

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR"

echo ""
echo "=================================================="
echo " Plain-msg evaluation sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
