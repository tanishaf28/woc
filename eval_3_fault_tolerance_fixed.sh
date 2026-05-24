#!/bin/bash
# ================================================================
# EVAL 3: Fault Tolerance - MongoDB Workload A
# FIX: -et=1, correct flags, SIGTERM stop with wait
# ================================================================

set -u

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_2s3w.conf"
LOG_DIR="${REMOTE_DIR}/logs"
RUNTIME=30
WORKLOAD="a"
SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10)

NUM_SERVERS=5
THRESHOLD=1
BATCHSIZE=1
INDEP_RATIO=90.0
COMMON_RATIO=10.0
MAX_INFLIGHT=5
MONGO_CLIENT_POOL=16
LOG_LEVEL="info"

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

declare -A TEST_CASES=(
    ["no_failure"]=""
    ["node1_fails"]="1"
    ["node4_fails"]="4"
    ["node0_node1_fail"]="0,1"
)

echo "=============================================="
echo "EVAL 3: Fault Tolerance (MongoDB)"
echo "=============================================="

remote_exec() { local host=$1; shift; ssh "${SSH_OPTS[@]}" "$USER@$host" "$*"; }

wait_for_mongo_ready() {
    local host=$1 attempt
    for attempt in $(seq 1 30); do
        remote_exec "$host" "mongosh --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1" && return 0
        sleep 1
    done
    return 1
}

start_mongo_cluster() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "mkdir -p '$REMOTE_DIR/mongodb_data' '$LOG_DIR'"
    done
    for ip in "${SERVER_IPS[@]}"; do
        remote_exec "$ip" "pkill -f mongod 2>/dev/null || true; \
            rm -f '$REMOTE_DIR/mongodb_data/mongod.lock' '$REMOTE_DIR/mongodb_data/WiredTiger.lock' 2>/dev/null || true; \
            nohup mongod --port 27017 --replSet wocrs --dbpath '$REMOTE_DIR/mongodb_data' \
            --bind_ip 0.0.0.0 --logpath '$LOG_DIR/mongod.log' --logappend \
            > '$LOG_DIR/mongod.out' 2>&1 &"
    done
    for ip in "${SERVER_IPS[@]}"; do wait_for_mongo_ready "$ip" || true; done
}

init_replica_set() {
    remote_exec "${SERVER_IPS[0]}" "mongosh --eval \"rs.initiate({_id:'wocrs',members:[
        {_id:0,host:'${SERVER_IPS[0]}:27017',priority:1},
        {_id:1,host:'${SERVER_IPS[1]}:27017',priority:1},
        {_id:2,host:'${SERVER_IPS[2]}:27017',priority:0},
        {_id:3,host:'${SERVER_IPS[3]}:27017',priority:0},
        {_id:4,host:'${SERVER_IPS[4]}:27017',priority:0}]})\" >/dev/null 2>&1 || true"
    sleep 8
}

build_and_distribute() {
    go build -o "$BINARY"
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        scp "${SSH_OPTS[@]}" "$BINARY" "$USER@$ip:$REMOTE_DIR/" 2>/dev/null &
    done
    wait
    echo "  Done"
}

start_workload_nodes() {
    local scenario=$1
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -f 'woc.*-path' 2>/dev/null || true; \
            nohup '$REMOTE_DIR/$BINARY' \
            -id=$i -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=$THRESHOLD \
            -b=$BATCHSIZE -mode=1 -mcli=$MONGO_CLIENT_POOL -mload='$WORKLOAD' \
            -bcomp=object-specific \
            -indep=$INDEP_RATIO -common=$COMMON_RATIO \
            -pipeline=true -maxinflight=$MAX_INFLIGHT \
            -log=$LOG_LEVEL -ep=true -role=0 \
            > '$LOG_DIR/server_${i}_${scenario}.log' 2>&1 &"
    done
    for i in "${!CLIENT_HOST_IPS[@]}"; do
        ip="${CLIENT_HOST_IPS[$i]}"
        client_id=$((NUM_SERVERS + i))
        remote_exec "$ip" "pkill -f 'woc.*-path' 2>/dev/null || true; \
            nohup '$REMOTE_DIR/$BINARY' \
            -id=$client_id -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=$THRESHOLD \
            -b=$BATCHSIZE -mode=1 -mload='$WORKLOAD' \
            -bcomp=object-specific \
            -indep=$INDEP_RATIO -common=$COMMON_RATIO \
            -pipeline=true -maxinflight=$MAX_INFLIGHT \
            -log=$LOG_LEVEL -ops=0 -role=1 \
            > '$LOG_DIR/client_${i}_${scenario}.log' 2>&1 &"
    done
}

kill_servers() {
    local nodes_csv=$1
    [ -z "$nodes_csv" ] && return 0
    IFS=',' read -ra nodes <<< "$nodes_csv"
    for node_id in "${nodes[@]}"; do
        ip="${SERVER_IPS[$node_id]}"
        echo "  Killing server $node_id on $ip"
        remote_exec "$ip" "pkill -TERM -x woc 2>/dev/null || true" &
    done
    wait
    sleep 2
    for node_id in "${nodes[@]}"; do
        remote_exec "${SERVER_IPS[$node_id]}" "pkill -9 -x woc 2>/dev/null || true" &
    done
    wait
}

stop_workload_nodes() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -TERM -x woc 2>/dev/null || true" &
    done
    wait
    sleep 3
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -9 -x woc 2>/dev/null || true" &
    done
    wait
}

cleanup() {
    stop_workload_nodes || true
    for ip in "${SERVER_IPS[@]}"; do
        remote_exec "$ip" "pkill -f mongod 2>/dev/null || true" || true
    done
}
trap cleanup EXIT

build_and_distribute
start_mongo_cluster
init_replica_set

for scenario in "${!TEST_CASES[@]}"; do
    failed_nodes="${TEST_CASES[$scenario]}"
    echo ""
    echo "--- Scenario: $scenario (kill nodes: '${failed_nodes:-none}') ---"
    start_workload_nodes "$scenario"
    sleep 10
    if [ -n "$failed_nodes" ]; then
        echo "  Injecting failures..."
        kill_servers "$failed_nodes"
    fi
    echo "  Running for ${RUNTIME}s..."
    sleep $RUNTIME
    stop_workload_nodes
    sleep 2
done

echo ""
echo "EVAL 3 COMPLETE (MongoDB)"
