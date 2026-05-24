#!/bin/bash
# ================================================================
# EVAL 4: Network Delay - MongoDB Workload A
# FIX: -et=1, correct flags, cached interfaces, scaled maxinflight
# ================================================================

set -u

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_2s3w.conf"
LOG_DIR="${REMOTE_DIR}/logs"
RUNTIME=45
WORKLOAD="a"
SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10)

NUM_SERVERS=5
THRESHOLD=1
BATCHSIZE=1
INDEP_RATIO=90.0
COMMON_RATIO=10.0
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

DELAY_CASES=(
"0/0/10"
"5/1/15"
"10/2/20"
"20/4/30"
"50/10/60"
"100/20/100"
"200/40/150"
)

_CACHED_SERVER_IFACES=()
_CACHED_CLIENT_IFACES=()

echo "=============================================="
echo "EVAL 4: Network Delay (MongoDB, Cabinet D1 sweep)"
echo "=============================================="

remote_exec() { local host=$1; shift; ssh "${SSH_OPTS[@]}" "$USER@$host" "$*"; }

detect_interface() {
    remote_exec "$1" "ip route show default 2>/dev/null | awk '{print \$5; exit}'"
}

cache_interfaces() {
    echo "  Caching network interfaces (before applying netem)..."
    _CACHED_SERVER_IFACES=()
    _CACHED_CLIENT_IFACES=()
    for ip in "${SERVER_IPS[@]}"; do
        iface=$(detect_interface "$ip")
        _CACHED_SERVER_IFACES+=("$iface")
        echo "    server $ip -> $iface"
    done
    for ip in "${CLIENT_HOST_IPS[@]}"; do
        iface=$(detect_interface "$ip")
        _CACHED_CLIENT_IFACES+=("$iface")
        echo "    client $ip -> $iface"
    done
}

apply_delay() {
    local delay_ms=$1 jitter_ms=$2
    if [ "$delay_ms" -eq 0 ]; then
        remove_delay
        return 0
    fi
    echo "  Applying ${delay_ms}ms +- ${jitter_ms}ms on all nodes..."
    for i in "${!SERVER_IPS[@]}"; do
        local iface="${_CACHED_SERVER_IFACES[$i]}"
        [ -z "$iface" ] && continue
        ssh "${SSH_OPTS[@]}" "$USER@${SERVER_IPS[$i]}" \
            "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
             sudo tc qdisc add dev '$iface' root netem delay ${delay_ms}ms ${jitter_ms}ms distribution normal" || true &
    done
    for i in "${!CLIENT_HOST_IPS[@]}"; do
        local iface="${_CACHED_CLIENT_IFACES[$i]}"
        [ -z "$iface" ] && continue
        ssh "${SSH_OPTS[@]}" "$USER@${CLIENT_HOST_IPS[$i]}" \
            "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
             sudo tc qdisc add dev '$iface' root netem delay ${delay_ms}ms ${jitter_ms}ms distribution normal" || true &
    done
    wait
    sleep 1
}

remove_delay() {
    echo "  Removing network delays..."
    for i in "${!SERVER_IPS[@]}"; do
        local iface="${_CACHED_SERVER_IFACES[$i]:-}"
        [ -z "$iface" ] && iface=$(detect_interface "${SERVER_IPS[$i]}")
        [ -z "$iface" ] && continue
        ssh "${SSH_OPTS[@]}" "$USER@${SERVER_IPS[$i]}" \
            "sudo tc qdisc del dev '$iface' root 2>/dev/null || true" || true &
    done
    for i in "${!CLIENT_HOST_IPS[@]}"; do
        local iface="${_CACHED_CLIENT_IFACES[$i]:-}"
        [ -z "$iface" ] && iface=$(detect_interface "${CLIENT_HOST_IPS[$i]}")
        [ -z "$iface" ] && continue
        ssh "${SSH_OPTS[@]}" "$USER@${CLIENT_HOST_IPS[$i]}" \
            "sudo tc qdisc del dev '$iface' root 2>/dev/null || true" || true &
    done
    wait
}

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
        {_id:0,host:'${SERVER_IPS[0]}:27017'},{_id:1,host:'${SERVER_IPS[1]}:27017'},
        {_id:2,host:'${SERVER_IPS[2]}:27017'},{_id:3,host:'${SERVER_IPS[3]}:27017'},
        {_id:4,host:'${SERVER_IPS[4]}:27017'}]})\" >/dev/null 2>&1 || true"
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
    local delay_ms=$1 mif=$2
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -f 'woc.*-path' 2>/dev/null || true; \
            nohup '$REMOTE_DIR/$BINARY' \
            -id=$i -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=$THRESHOLD \
            -b=$BATCHSIZE -mode=1 -mcli=$MONGO_CLIENT_POOL -mload='$WORKLOAD' \
            -bcomp=object-specific \
            -indep=$INDEP_RATIO -common=$COMMON_RATIO \
            -pipeline=true -maxinflight=$mif \
            -log=$LOG_LEVEL -ep=true -role=0 \
            > '$LOG_DIR/server_${i}_delay_${delay_ms}ms.log' 2>&1 &"
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
            -pipeline=true -maxinflight=$mif \
            -log=$LOG_LEVEL -ops=0 -role=1 \
            > '$LOG_DIR/client_${i}_delay_${delay_ms}ms.log' 2>&1 &"
    done
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
    remove_delay || true
    stop_workload_nodes || true
    for ip in "${SERVER_IPS[@]}"; do
        remote_exec "$ip" "pkill -f mongod 2>/dev/null || true" || true
    done
}
trap cleanup EXIT

build_and_distribute
cache_interfaces
start_mongo_cluster
init_replica_set

test_num=1
for entry in "${DELAY_CASES[@]}"; do
    delay_ms=$(echo "$entry" | cut -d/ -f1)
    jitter_ms=$(echo "$entry" | cut -d/ -f2)
    mif=$(echo "$entry" | cut -d/ -f3)

    echo ""
    echo "--- Test $test_num: DELAY=${delay_ms}ms +- ${jitter_ms}ms | MAX_INFLIGHT=${mif} ---"
    apply_delay "$delay_ms" "$jitter_ms"
    start_workload_nodes "$delay_ms" "$mif"
    sleep 5
    echo "  Running for ${RUNTIME}s..."
    sleep $RUNTIME
    stop_workload_nodes
    remove_delay
    sleep 2
    test_num=$((test_num + 1))
done

echo ""
echo "EVAL 4 COMPLETE (MongoDB)"
