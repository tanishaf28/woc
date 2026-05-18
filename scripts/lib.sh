#!/bin/bash
# Common helpers for YCSB evaluation scripts
set -u

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_2s3w.conf"
SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10)

remote_exec() {
    local host=$1
    shift
    ssh "${SSH_OPTS[@]}" "$USER@$host" "$*"
}

create_remote_dirs() {
    for ip in "$@"; do
        remote_exec "$ip" "mkdir -p '$REMOTE_DIR' '$LOG_DIR' '$EVAL_DIR' '$REMOTE_DIR/mongodb_data' '$REMOTE_DIR/ycsb_logs'"
    done
}

wait_for_mongo_ready() {
    local host=$1
    for attempt in $(seq 1 30); do
        if remote_exec "$host" "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done
    echo "Warning: MongoDB not responding on $host"
    return 1
}

start_mongo_cluster() {
    # Starts mongod on provided server IPs and waits for readiness
    local servers=("${@}")
    if [ "${#servers[@]}" -eq 0 ]; then
        servers=("${SERVER_IPS[@]}")
    fi

    for ip in "${servers[@]}"; do
        remote_exec "$ip" "pkill -f mongod 2>/dev/null || true; rm -f '$REMOTE_DIR/mongodb_data/mongod.lock' '$REMOTE_DIR/mongodb_data/WiredTiger.lock' '$LOG_DIR/mongod.log' 2>/dev/null || true; mkdir -p '$REMOTE_DIR/mongodb_data' '$LOG_DIR'; nohup mongod --port 27017 --replSet wocrs --dbpath '$REMOTE_DIR/mongodb_data' --bind_ip 0.0.0.0 --logpath '$LOG_DIR/mongod.log' --logappend > '$LOG_DIR/mongod.out' 2>&1 &"
    done

    for ip in "${servers[@]}"; do
        wait_for_mongo_ready "$ip" || true
    done
}

init_replica_set() {
    local servers=("${@}")
    if [ "${#servers[@]}" -eq 0 ]; then
        servers=("${SERVER_IPS[@]}")
    fi

    echo "  Initializing replica set with ${#servers[@]} members..."
    local members=""
    local idx=0
    for ip in "${servers[@]}"; do
        if [ -n "$members" ]; then
            members=",${members}"
        fi
        members="{ _id: ${idx}, host: '${ip}:27017' }${members}"
        idx=$((idx + 1))
    done

    # Run rs.initiate with explicit member list
    remote_exec "${servers[0]}" "mongosh --eval \"rs.initiate({ _id: 'wocrs', members: [${members}] })\" >/dev/null 2>&1 || true"

    for attempt in $(seq 1 30); do
        if remote_exec "${servers[0]}" "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done

    echo "  Warning: replica set readiness timed out"
    return 1
}

run_woc_client() {
    # Start a woc client instance on the client host. Provide additional flags as arguments.
    local client=$1; shift
    local extra_args="$*"
    local ts=$(date +%s)
    local rand=$(od -An -N2 -tu2 /dev/urandom | tr -d ' ' | tr -d '\n' 2>/dev/null || echo $RANDOM)
    local logname="$LOG_DIR/woc_client_${client}_$ts_${rand}.log"
    remote_exec "$client" "nohup '$REMOTE_DIR/$BINARY' -id=-1 -conf='$CONFIG_PATH' -role=1 -et=1 -mongodb ${extra_args} > '$logname' 2>&1 &"
}

run_ycsb_on_client() {
    # Backwards-compat shim: translate an incoming workload string into a woc client run.
    # Expectation: callers will be updated to use run_woc_client directly. If passed a YCSB-like string,
    # we simply start a woc client with default workload 'a' for compatibility.
    local client=$1; shift
    local workload_arg="$*"
    # try to extract workload file name (workloada/workloadb etc.)
    local workload="a"
    if [[ "$workload_arg" =~ workload([a-f]) ]]; then
        workload="${BASH_REMATCH[1]}"
    fi
    run_woc_client "$client" "-workload=$workload -maxexecutiontime=30"
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
    # start WOC servers and clients using globals SERVER_IPS and CLIENT_HOST_IPS
    local indep=${1:-50.0}
    local common=${2:-50.0}
    local workload=${3:-a}

    echo "  Starting WOC servers (workload=${workload}, indep=${indep}, common=${common})"
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -f 'woc.*-conf' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=$i -conf='$CONFIG_PATH' -role=0 -et=1 -mongodb -workload=${workload} -indep_ratio=${indep} -common_ratio=${common} > '$LOG_DIR/server_${i}_indep_${indep}_common_${common}_$(date +%s).log' 2>&1 &"
    done

    echo "  Starting WOC clients"
    for i in "${!CLIENT_HOST_IPS[@]}"; do
        ip="${CLIENT_HOST_IPS[$i]}"
        remote_exec "$ip" "pkill -f 'woc.*-conf' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=-1 -conf='$CONFIG_PATH' -role=1 -et=1 -mongodb -workload=${workload} -indep_ratio=${indep} -common_ratio=${common} > '$LOG_DIR/client_${i}_indep_${indep}_common_${common}_$(date +%s).log' 2>&1 &"
    done
}

stop_workload_nodes() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -SIGTERM -f '$BINARY' 2>/dev/null || true"
    done
}
