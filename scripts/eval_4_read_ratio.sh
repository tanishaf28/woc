#!/bin/bash
# ================================================================
# EVAL 4: Read Ratio Sweep (PlainMsg, heterogeneous 5-node cluster)
#
# Sweeps -readratio (% of ops that are reads) across 0/25/50/75/100 at a
# fixed INDEP_RATIO=90.0. Uses PlainMsg (-et=0), not MongoDB, since the
# thing being measured is CORA's own read mechanism (ObjectState.Value +
# weighted-quorum gather) rather than a MongoDB pass-through — MongoDB
# reads bypass that mechanism entirely by querying the real follower.
#
# -readmode is fixed for the whole sweep (default: safe) rather than
# crossed with -readratio; override via READ_MODE=fast if you want a
# separate "cost of safety" comparison run. Note dependent-object reads
# always use the quorum path regardless of -readmode (see consensus.go's
# handleRead) — only independent-object reads actually change behavior
# between fast/safe.
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
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_2s_3w.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"
MERGE_SCRIPT="${REPO_ROOT}/merge_eval.py"
RESULT_ROOT="${SCRIPT_DIR}/results/eval4_read_ratio"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
RUNTIME="${RUNTIME:-60}"  # seconds per test case
NUM_SERVERS=5
NUM_CLIENTS=2
THRESHOLD=1
INDEP_RATIO="${INDEP_RATIO:-90.0}"
BATCHSIZE=1
MSGSIZE=512
NUM_OBJECTS="${NUM_OBJECTS:-1000}"
PIPELINE_MODE="${PIPELINE_MODE:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-5}"
LOG_LEVEL="${LOG_LEVEL:-info}"
READ_MODE="${READ_MODE:-safe}"  # fast|safe, fixed for the whole sweep

# 5-Node Cluster: 2 Strong (c16) + 3 Weak (c4)
SERVER_IPS=(
"192.168.73.59"
"192.168.73.243"
"192.168.73.192"
"192.168.73.134"
"192.168.73.132"
)

CLIENT_HOST_IPS=(
"192.168.73.218"
"192.168.73.219"
)

SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10)

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash eval_4_read_ratio.sh

Sweeps -readratio over 0,25,50,75,100 against a fixed 5-server
heterogeneous cluster (PlainMsg). INDEP_RATIO=90.0, BATCHSIZE=1,
MSG_SIZE=512 fixed; -readmode fixed for the whole sweep (default: safe).

Environment overrides:
  RUNTIME=60        wall-clock seconds per test case
  READ_MODE=safe    fast|safe, applied to every case in this run
  INDEP_RATIO=90.0
  NUM_OBJECTS=1000

Results archived under: results/eval4_read_ratio/<timestamp>/<label>/
EOF
    exit 0
fi

mkdir -p "$RUN_DIR"

# Test cases: READRATIO values
TEST_CASES=(0 25 50 75 100)

echo "=============================================="
echo "EVAL 4: Read Ratio Sweep (PlainMsg)"
echo "=============================================="
echo "Test cases: ${TEST_CASES[*]}"
echo "Read mode (fixed): ${READ_MODE}"
echo "Indep ratio (fixed): ${INDEP_RATIO}"
echo "Runtime per test: ${RUNTIME}s"
echo ""

remote_exec() {
    local host=$1
    shift
    ssh "${SSH_OPTS[@]}" "$USER@$host" "$*"
}

create_remote_dirs() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "mkdir -p '$REMOTE_DIR' '$LOG_DIR' '$EVAL_DIR' '$REMOTE_DIR/config'"
    done
}

build_and_distribute() {
    echo "  Building WOC binary..."
    (cd "$REPO_ROOT" && go build -o "${SCRIPT_DIR}/${BINARY}")

    echo "  Distributing binary + config to all nodes..."
    create_remote_dirs
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        scp "${SSH_OPTS[@]}" "$BINARY" "$USER@$ip:$REMOTE_DIR/" 2>/dev/null &
        scp "${SSH_OPTS[@]}" "${REPO_ROOT}/config/cluster_hetero_5n_2s_3w.conf" "$USER@$ip:$CONFIG_PATH" 2>/dev/null &
    done
    wait
    echo "  ✓ Distribution complete"
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
            "$USER@$host:${EVAL_DIR}/" "$node_dir/" 2>/dev/null || true
        scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" -r \
            "$USER@$host:${LOG_DIR}/" "$node_dir/" 2>/dev/null || true
        idx=$((idx + 1))
    done
}

merge_case_results() {
    local label=$1
    local case_dir="${RUN_DIR}/${label}"
    local case_eval_dir="${case_dir}/eval"
    local case_merged_dir="${case_dir}/merged"
    local client_start_id=$NUM_SERVERS
    local client_end_id=$((NUM_SERVERS + NUM_CLIENTS - 1))
    local client_id_filter="${client_start_id}-${client_end_id}"
    local server_id_filter="0-$((NUM_SERVERS - 1))"

    mkdir -p "$case_eval_dir" "$case_merged_dir"

    for node_dir in "${case_dir}"/node_*; do
        [ -d "$node_dir/eval" ] || continue
        cp -r "$node_dir/eval/"* "$case_eval_dir/" 2>/dev/null || true
    done

    if [ -f "$MERGE_SCRIPT" ]; then
        python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" --ids "$client_id_filter"
        python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" --servers --ids "$server_id_filter"
    else
        echo "  Warning: merge_eval.py not found at $MERGE_SCRIPT"
    fi

    # Raw per-node eval/log copies are scratch input for the merge above;
    # a long sweep keeping all of them fills the disk.
    rm -rf "${case_dir}"/node_* "$case_eval_dir"
}

start_workload_nodes() {
    local readratio=$1

    echo "  Starting WOC servers..."
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -9 -x woc 2>/dev/null || true; cd '$REMOTE_DIR'; nohup ./$BINARY -id=$i -path='$CONFIG_PATH' -et=0 -n=$NUM_SERVERS -t=$THRESHOLD -b=$BATCHSIZE -mode=1 -bcomp=object-specific -indep=$INDEP_RATIO -numobjects=$NUM_OBJECTS -ms=$MSGSIZE -pipeline=$PIPELINE_MODE -maxinflight=$MAX_INFLIGHT -log=$LOG_LEVEL -ep=true -role=0 > '$LOG_DIR/server_${i}_read_${readratio}.log' 2>&1 &"
    done

    echo "  Starting WOC clients..."
    for i in "${!CLIENT_HOST_IPS[@]}"; do
        ip="${CLIENT_HOST_IPS[$i]}"
        client_id=$((NUM_SERVERS + i))
        pin_server=$((i % NUM_SERVERS))
        remote_exec "$ip" "pkill -9 -x woc 2>/dev/null || true; cd '$REMOTE_DIR'; nohup ./$BINARY -id=$client_id -path='$CONFIG_PATH' -et=0 -n=$NUM_SERVERS -t=$THRESHOLD -b=$BATCHSIZE -mode=1 -bcomp=object-specific -indep=$INDEP_RATIO -numobjects=$NUM_OBJECTS -ms=$MSGSIZE -pipeline=$PIPELINE_MODE -maxinflight=$MAX_INFLIGHT -log=$LOG_LEVEL -ops=0 -readratio=$readratio -readmode=$READ_MODE -pinserver=$pin_server -role=1 > '$LOG_DIR/client_${i}_read_${readratio}.log' 2>&1 &"
    done
}

stop_workload_nodes() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -TERM -x woc 2>/dev/null || true"
    done
    sleep 3
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -9 -x woc 2>/dev/null || true"
    done
}

cleanup() {
    stop_workload_nodes || true
}
trap cleanup EXIT

start_cluster() {
    local readratio=$1
    local test_num=$2
    local label="read_${readratio}"

    echo ""
    echo "--- Test $test_num: READRATIO=$readratio (mode=$READ_MODE) ---"

    start_workload_nodes "$readratio"

    echo "  Cluster started. Running for ${RUNTIME}s..."
    sleep "$RUNTIME"

    echo "  Stopping workload processes..."
    stop_workload_nodes
    sleep 2

    echo "  Archiving results..."
    archive_case "$label" "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"
    merge_case_results "$label"
}

# Run tests
build_and_distribute

test_num=1
for readratio in "${TEST_CASES[@]}"; do
    start_cluster "$readratio" "$test_num"
    test_num=$((test_num + 1))
done

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size "$NUM_SERVERS"

echo ""
echo "=============================================="
echo "✓ EVAL 4 COMPLETE"
echo "=============================================="
echo ""
echo "Results archived in: $RUN_DIR"
echo ""
echo "Merged client/server summaries are under: $RUN_DIR/*/merged/"
