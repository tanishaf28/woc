#!/bin/bash
# ================================================================
# EVAL 1: Independent Ratio Evaluation (MongoDB workload a)
# Tests various INDEP_RATIO values: 100, 90, 80, 60, 40, 20, 10, 0
# Each configuration runs for 60 seconds.
# ================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_new.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"
MERGE_SCRIPT="${REPO_ROOT}/merge_eval.py"
RESULT_ROOT="${SCRIPT_DIR}/results/eval1_indep_ratio"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
RUNTIME=60  # 60 seconds per test
NUM_SERVERS=5
NUM_CLIENTS=2
THRESHOLD=1
BATCHSIZE=1
PIPELINE_MODE=true
MAX_INFLIGHT=5
MONGO_CLIENT_POOL=16
LOG_LEVEL="info"

# 5-Node Cluster: 2 Strong (c16) + 3 Weak (c4)
SERVER_IPS=(
"192.168.73.59"
"192.168.73.243"
"192.168.73.192"
"192.168.73.134"
"192.168.73.132"
)

CLIENT_HOST_IPS=(
"192.168.73.167"
"192.168.73.137"
)

WORKLOAD="a"
SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10)

mkdir -p "$RUN_DIR"

# Test cases: INDEP_RATIO values
TEST_CASES=(100.0 90.0 80.0 60.0 40.0 20.0 10.0 0.0)

echo "=============================================="
echo "EVAL 1: Independent Ratio (MongoDB workload a)"
echo "=============================================="
echo "Test cases: ${#TEST_CASES[@]}"
echo "Runtime per test: ${RUNTIME}s"
echo ""

remote_exec() {
    local host=$1
    shift
    ssh "${SSH_OPTS[@]}" "$USER@$host" "$*"
}

create_remote_dirs() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "mkdir -p '$REMOTE_DIR' '$LOG_DIR' '$EVAL_DIR' '$REMOTE_DIR/mongodb_data'"
    done
}

wait_for_mongo_ready() {
    local host=$1
    local label=$2
    local attempt

    for attempt in $(seq 1 60); do
        if remote_exec "$host" "mongosh --host 127.0.0.1 --port 27017 --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done

    remote_exec "$host" "echo '--- mongod.log ---'; tail -n 50 '$LOG_DIR/mongod.log' 2>/dev/null || true; echo '--- mongod.out ---'; tail -n 50 '$LOG_DIR/mongod.out' 2>/dev/null || true" || true
    echo "  Warning: MongoDB readiness timed out on $label ($host)"
    return 1
}

start_mongo_cluster() {
    echo "  Creating remote directories..."
    create_remote_dirs

    # Standalone mongod per server (no replica set): each server's
    # MongoFollower connects to its own local instance via MONGODB_URI
    # (defaults to localhost:27017, no directConnection param). Forming a
    # real replica set here (as this script used to, via --replSet +
    # rs.initiate) would make the Go driver auto-discover the other members
    # and route all writes to whichever one is elected primary, silently
    # funneling every "independent" replica's MongoDB traffic onto one
    # shared node instead of each replica's own local database - see
    # start_mongodb_hetero.sh, which uses this same standalone approach.
    echo "  Starting MongoDB on all servers..."
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -x mongod 2>/dev/null || true; rm -rf '$REMOTE_DIR/mongodb_data/'* '$REMOTE_DIR/mongodb_data'/.[!.]* '$REMOTE_DIR/mongodb_data'/..?* 2>/dev/null || true; rm -f '$REMOTE_DIR/mongodb_data/mongod.lock' '$REMOTE_DIR/mongodb_data/WiredTiger.lock' '$LOG_DIR/mongod.log' '$LOG_DIR/mongod.out' 2>/dev/null || true; mkdir -p '$REMOTE_DIR/mongodb_data' '$LOG_DIR'; mongod --port 27017 --dbpath '$REMOTE_DIR/mongodb_data' --bind_ip_all --logpath '$LOG_DIR/mongod.log' --logappend --fork --pidfilepath '$REMOTE_DIR/mongodb_data/mongod.pid'"
    done

    for i in "${!SERVER_IPS[@]}"; do
        wait_for_mongo_ready "${SERVER_IPS[$i]}" "server${i}" || true
    done
}

build_and_distribute() {
    echo "  Building WOC binary..."
    (cd "$REPO_ROOT" && go build -o "${SCRIPT_DIR}/${BINARY}")

    echo "  Distributing to all nodes..."
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        scp "${SSH_OPTS[@]}" "$BINARY" "$USER@$ip:$REMOTE_DIR/" 2>/dev/null &
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
}

start_workload_nodes() {
    local indep=$1

    echo "  Starting WOC servers..."
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -f 'woc.*-path' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=$i -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=$THRESHOLD -b=$BATCHSIZE -mode=1 -mcli=$MONGO_CLIENT_POOL -mload='$WORKLOAD' -bcomp=object-specific -indep=$indep -pipeline=$PIPELINE_MODE -maxinflight=$MAX_INFLIGHT -log=$LOG_LEVEL -ep=true -role=0 > '$LOG_DIR/server_${i}_indep_${indep}.log' 2>&1 &"
    done

    echo "  Starting WOC clients..."
    for i in "${!CLIENT_HOST_IPS[@]}"; do
        ip="${CLIENT_HOST_IPS[$i]}"
        client_id=$((NUM_SERVERS + i))
        pin_server=$((i % NUM_SERVERS))
        remote_exec "$ip" "pkill -f 'woc.*-path' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=$client_id -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=$THRESHOLD -b=$BATCHSIZE -mode=1 -mload='$WORKLOAD' -bcomp=object-specific -indep=$indep -pipeline=$PIPELINE_MODE -maxinflight=$MAX_INFLIGHT -log=$LOG_LEVEL -ops=0 -pinserver=$pin_server -role=1 > '$LOG_DIR/client_${i}_indep_${indep}.log' 2>&1 &"
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
    for ip in "${SERVER_IPS[@]}"; do
        remote_exec "$ip" "pkill -f mongod 2>/dev/null || true" || true
    done
}

trap cleanup EXIT

start_cluster() {
    local indep=$1
    local test_num=$2
    local label="indep_${indep}"

    echo ""
    echo "--- Test $test_num: INDEP=$indep ---"

    start_workload_nodes "$indep"

    echo "  Cluster started. Running for ${RUNTIME}s..."
    sleep $RUNTIME

    # Stop only workload processes between cases; MongoDB stays up for the full sweep.
    echo "  Stopping workload processes..."
    stop_workload_nodes
    sleep 2

    echo "  Archiving results..."
    archive_case "$label" "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"
    merge_case_results "$label"
}

# Run tests
build_and_distribute

start_mongo_cluster

test_num=1
for indep in "${TEST_CASES[@]}"; do
    start_cluster "$indep" "$test_num"
    test_num=$((test_num + 1))
done

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size "$NUM_SERVERS"

echo ""
echo "=============================================="
echo "✓ EVAL 1 COMPLETE"
echo "=============================================="
echo ""
echo "Results archived in: $RUN_DIR"
echo ""
echo "Merged client/server summaries are under: $RUN_DIR/*/merged/"
