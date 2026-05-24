#!/bin/bash
# ================================================================
# EVAL 2: Max Pipeline In-Flight Evaluation
# Tests pipeline depths: 1,2,3,4,5,8,10,15,20,25,30,35,40,45,50
# Each configuration runs for 30 seconds
# ================================================================

set -u

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_2s3w.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"
RUNTIME=30  # 30 seconds per test

# 5-Node Cluster
SERVER_IPS=(
"192.168.73.159"
"192.168.73.84"
"192.168.73.69"
"192.168.73.235"
"192.168.73.194"
)

CLIENT_HOST_IPS=(
"192.168.73.218"
"192.168.73.219"
)

WORKLOAD="a"
INDEP_RATIO=100.0
COMMON_RATIO=0.0
SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10)

# Max inflight values to test
MAX_INFLIGHT_VALUES=(1 2 3 4 5 8 10 15 20 25 30 35 40 45 50)

echo "=============================================="
echo "EVAL 2: Max Pipeline In-Flight"
echo "=============================================="
echo "Test cases: ${#MAX_INFLIGHT_VALUES[@]}"
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

    for attempt in $(seq 1 30); do
        if remote_exec "$host" "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done

    echo "  Warning: MongoDB readiness timed out on $label ($host)"
    return 1
}

start_mongo_cluster() {
    echo "  Creating remote directories..."
    create_remote_dirs

    echo "  Starting MongoDB on all servers..."
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -f mongod 2>/dev/null || true; rm -f '$REMOTE_DIR/mongodb_data/mongod.lock' '$REMOTE_DIR/mongodb_data/WiredTiger.lock' '$LOG_DIR/mongod.log' 2>/dev/null || true; mkdir -p '$REMOTE_DIR/mongodb_data' '$LOG_DIR'; nohup mongod --port 27017 --replSet wocrs --dbpath '$REMOTE_DIR/mongodb_data' --bind_ip 0.0.0.0 --logpath '$LOG_DIR/mongod.log' --logappend > '$LOG_DIR/mongod.out' 2>&1 &"
    done

    for i in "${!SERVER_IPS[@]}"; do
        wait_for_mongo_ready "${SERVER_IPS[$i]}" "server${i}" || true
    done
}

init_replica_set() {
    echo "  Initializing MongoDB replica set..."
    remote_exec "${SERVER_IPS[0]}" "mongosh --eval \"rs.initiate({ _id: 'wocrs', members: [ {_id: 0, host: '${SERVER_IPS[0]}:27017'}, {_id: 1, host: '${SERVER_IPS[1]}:27017'}, {_id: 2, host: '${SERVER_IPS[2]}:27017'}, {_id: 3, host: '${SERVER_IPS[3]}:27017'}, {_id: 4, host: '${SERVER_IPS[4]}:27017'} ] })\" >/dev/null 2>&1 || true"

    for attempt in $(seq 1 30); do
        if remote_exec "${SERVER_IPS[0]}" "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done

    echo "  Warning: replica set readiness timed out"
    return 1
}

build_and_distribute() {
    echo "  Building WOC binary..."
    go build -o "$BINARY"
    
    echo "  Distributing to all nodes..."
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        scp "${SSH_OPTS[@]}" "$BINARY" "$USER@$ip:$REMOTE_DIR/" 2>/dev/null &
    done
    wait
    echo "  ✓ Distribution complete"
}

start_workload_nodes() {
    local max_inflight=$1

    echo "  Starting WOC servers..."
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -f 'woc.*-path' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=$i -path='$CONFIG_PATH' -et=1 -n=5 -t=1 -role=0 -mload='$WORKLOAD' -max_inflight=$max_inflight > '$LOG_DIR/server_${i}_inflight_${max_inflight}.log' 2>&1 &"
    done

    echo "  Starting WOC clients..."
    for i in "${!CLIENT_HOST_IPS[@]}"; do
        ip="${CLIENT_HOST_IPS[$i]}"
        client_id=$((5 + i))
        remote_exec "$ip" "pkill -f 'woc.*-path' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=$client_id -path='$CONFIG_PATH' -et=1 -n=5 -t=1 -role=1 -mload='$WORKLOAD' -max_inflight=$max_inflight > '$LOG_DIR/client_${i}_inflight_${max_inflight}.log' 2>&1 &"
    done
}

stop_workload_nodes() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -f 'woc.*-conf' 2>/dev/null || true"
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
    local max_inflight=$1
    local test_num=$2
    
    echo ""
    echo "--- Test $test_num: MAX_INFLIGHT=$max_inflight ---"
    
    start_workload_nodes "$max_inflight"
    
    echo "  Cluster started. Running for ${RUNTIME}s..."
    sleep $RUNTIME
    
    # Stop only workload processes between cases; MongoDB stays up for the full sweep.
    echo "  Stopping workload processes..."
    stop_workload_nodes
    sleep 2
}

# Run tests
build_and_distribute

start_mongo_cluster
init_replica_set

test_num=1
for max_inflight in "${MAX_INFLIGHT_VALUES[@]}"; do
    start_cluster "$max_inflight" "$test_num"
    test_num=$((test_num + 1))
done

echo ""
echo "=============================================="
echo "✓ EVAL 2 COMPLETE"
echo "=============================================="
echo ""
echo "Results stored in:"
echo "  Server logs: $LOG_DIR/server_*_inflight_*.log"
echo "  Client logs: $LOG_DIR/client_*_inflight_*.log"
echo "  Eval data:   $EVAL_DIR/test_*.csv"
echo ""
echo "Retrieve results with:"
echo "  ssh -i $SSH_KEY $USER@${SERVER_IPS[0]} 'ls -lah $EVAL_DIR/'"
