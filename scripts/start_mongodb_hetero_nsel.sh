#!/bin/bash
# ================================================================
# MongoDB Cluster Launcher - HETEROGENEOUS, SELECTABLE SIZE (WOC)
#
# Generalizes start_mongodb_hetero.sh (fixed n=5) to NUM_SERVERS in
# {3,5,7,11}, for the MongoDB ratio sweep (eval 1) and threshold sweep
# (eval 2), which both need to run at every cluster size, matching the
# PlainMsg size sweep (eval_5_i2d_size_sweep.sh). Server/client IPs are
# read from the chosen config file instead of hardcoded, so this covers
# every size from one script.
#
# n=5 deliberately keeps using cluster_hetero_new.conf (NOT
# cluster_hetero_5n_2s_3w.conf) - this is the confirmed-correct n=5
# heterogeneous tier hardware (same file start_mongodb_hetero.sh already
# uses); cluster_hetero_5n_2s_3w.conf's IPs are a different, unrelated
# machine set for this repo. n=3/7/11 use the existing
# cluster_hetero_{n}n_2s_1w / 3s_4w / 4s_7w.conf tier files, which are
# already byte-identical across woc/epaxos/cabinet.
# ================================================================

set -e
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Parse workload argument
WORKLOAD="${1:-a}"
if [[ ! "$WORKLOAD" =~ ^[a-f]$ ]]; then
    echo "ERROR: workload must be one of: a b c d e f"
    exit 1
fi

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"

REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

MONGODB_PORT=27017

NUM_SERVERS="${NUM_SERVERS:-5}"
case "$NUM_SERVERS" in
    3)  CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_3n_2s_1w.conf" ;;
    5)  CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_new.conf" ;;
    7)  CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_7n_3s_4w.conf" ;;
    11) CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_11n_4s_7w.conf" ;;
    *)  echo "ERROR: unsupported NUM_SERVERS=${NUM_SERVERS}. Supported: 3, 5, 7, 11"; exit 1 ;;
esac
LOCAL_CONFIG_PATH="${REPO_ROOT}/config/$(basename "$CONFIG_PATH")"

mapfile -t ALL_IPS < <(awk 'NF >= 2 { print $2 }' "$LOCAL_CONFIG_PATH")
if [ "${#ALL_IPS[@]}" -lt "$NUM_SERVERS" ]; then
    echo "ERROR: ${LOCAL_CONFIG_PATH} does not contain enough IPs for ${NUM_SERVERS} servers"
    exit 1
fi
SERVER_IPS=("${ALL_IPS[@]:0:NUM_SERVERS}")
CLIENT_POOL_IPS=("${ALL_IPS[@]:NUM_SERVERS}")
if [ "${#CLIENT_POOL_IPS[@]}" -eq 0 ]; then
    echo "ERROR: ${LOCAL_CONFIG_PATH} has no client IPs left after ${NUM_SERVERS} servers"
    exit 1
fi
DEFAULT_NUM_CLIENTS="${#CLIENT_POOL_IPS[@]}"
NUM_CLIENTS="${NUM_CLIENTS:-$DEFAULT_NUM_CLIENTS}"
CLIENT_HOST_IPS=()
for ((k = 0; k < NUM_CLIENTS; k++)); do
    CLIENT_HOST_IPS+=("${CLIENT_POOL_IPS[$((k % ${#CLIENT_POOL_IPS[@]}))]}")
done

THRESHOLD="${THRESHOLD:-$(( (NUM_SERVERS - 1) / 2 ))}"
OPS="${OPS:-0}"
EVAL_TYPE=1
BATCHSIZE="${BATCHSIZE:-10}"
MODE="${MODE:-1}"
INDEP_RATIO="${INDEP_RATIO:-100.0}"
NUM_OBJECTS="${NUM_OBJECTS:-1000}"
READ_RATIO="${READ_RATIO:-0.0}"
BATCH_COMPOSITION="${BATCH_COMPOSITION:-object-specific}"
LOG_LEVEL="${LOG_LEVEL:-info}"
ENABLE_PRIORITY="${ENABLE_PRIORITY:-true}"
MSG_SIZE="${MSG_SIZE:-512}"

# Environment variables read via os.Getenv in client.go/consensus.go, NOT CLI
# flags - see start_mongodb_hetero.sh's identical note.
PIPELINE_MODE="${PIPELINE_MODE:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-5}"
USE_ADAPTIVE_LIMITER="${USE_ADAPTIVE_LIMITER:-false}"
SERVER_BATCHING="${SERVER_BATCHING:-false}"
LATENCY_DEBUG="${LATENCY_DEBUG:-false}"
# ENABLE_TIMESERIES/TPS_TIMELINE_INTERVAL_MS: off by default (matches every
# other sweep script in this repo except the crash eval) - the delay eval
# and workload sweep turn this on explicitly for their own runs.
ENABLE_TIMESERIES="${ENABLE_TIMESERIES:-false}"
TPS_TIMELINE_INTERVAL_MS="${TPS_TIMELINE_INTERVAL_MS:-500}"

CLIENTS_PER_VM=1

echo "=============================================="
echo "Building WOC binary locally..."
echo "=============================================="
(cd "$REPO_ROOT" && go build -o "${SCRIPT_DIR}/${BINARY}")
echo "Build complete."

copy_binary() {
    local TARGET_IP=$1
    echo "  Copying binary to $TARGET_IP ..."
    scp -i "$SSH_KEY" "$BINARY" "$USER@$TARGET_IP:$REMOTE_DIR/"
}

copy_config() {
    local TARGET_IP=$1
    echo "  Copying config to $TARGET_IP ..."
    ssh -i "$SSH_KEY" "$USER@$TARGET_IP" "mkdir -p $REMOTE_DIR/config"
    scp -i "$SSH_KEY" "$LOCAL_CONFIG_PATH" "$USER@$TARGET_IP:$REMOTE_DIR/config/"
}

sync_load_file() {
    local TARGET_IP=$1
    echo "  Syncing YCSB load file (workload.dat) to $TARGET_IP ..."
    ssh -i "$SSH_KEY" "$USER@$TARGET_IP" "mkdir -p '$REMOTE_DIR/ycsb/workData'"
    scp -i "$SSH_KEY" "${REPO_ROOT}/ycsb/workData/workload.dat" "$USER@$TARGET_IP:$REMOTE_DIR/ycsb/workData/"
}

setup_mongodb() {
    local TARGET_IP=$1
    local NODE_ID=$2
    echo "  Setting up MongoDB on $TARGET_IP (Node $NODE_ID) ..."
    ssh -i "$SSH_KEY" "$USER@$TARGET_IP" bash -s <<'EOF'
set -e
MONGODB_PORT=27017
REMOTE_DIR="/home/ubuntu/woc"

pkill -x mongod 2>/dev/null || true
sleep 1

rm -rf $REMOTE_DIR/mongodb_data/* $REMOTE_DIR/mongodb_data/.[!.]* $REMOTE_DIR/mongodb_data/..?* 2>/dev/null || true
rm -f $REMOTE_DIR/mongodb_data/mongod.lock $REMOTE_DIR/mongodb_data/WiredTiger.lock $REMOTE_DIR/logs/mongod.log 2>/dev/null || true
mkdir -p $REMOTE_DIR/mongodb_data $REMOTE_DIR/logs

mongod --port $MONGODB_PORT \
    --dbpath $REMOTE_DIR/mongodb_data \
    --bind_ip 0.0.0.0 \
    --logpath $REMOTE_DIR/logs/mongod.log \
    --logappend \
    --fork --pidfilepath $REMOTE_DIR/mongodb_data/mongod.pid

sleep 2
echo "MongoDB started on $MONGODB_PORT"
EOF
}

wait_for_mongo_ready() {
    local TARGET_IP=$1
    local NODE_LABEL=$2
    local attempt

    for attempt in $(seq 1 30); do
        if ssh -i "$SSH_KEY" "$USER@$TARGET_IP" "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done

    echo "  Warning: MongoDB readiness timed out on ${NODE_LABEL} (${TARGET_IP})"
    return 1
}

echo ""
echo "=============================================="
echo "Distributing to SERVER nodes (n=${NUM_SERVERS})..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}"; do
    copy_binary "$ip"
    copy_config "$ip"
    sync_load_file "$ip"
done

echo ""
echo "=============================================="
echo "Distributing to CLIENT nodes (${NUM_CLIENTS})..."
echo "=============================================="
for ip in "${CLIENT_HOST_IPS[@]}"; do
    copy_binary "$ip"
    copy_config "$ip"
done

echo ""
echo "=============================================="
echo "Setting up MongoDB on all servers..."
echo "=============================================="
for i in "${!SERVER_IPS[@]}"; do
    setup_mongodb "${SERVER_IPS[$i]}" "$i"
done

for i in "${!SERVER_IPS[@]}"; do
    wait_for_mongo_ready "${SERVER_IPS[$i]}" "server${i}" || true
done

echo ""
echo "=============================================="
echo "Starting WOC servers (heterogeneous ${NUM_SERVERS}-node)..."
echo "=============================================="

for i in "${!SERVER_IPS[@]}"; do
    ip="${SERVER_IPS[$i]}"
    log_file="$LOG_DIR/server_${i}.log"

    echo "  Starting server $i on $ip..."
    ssh -i "$SSH_KEY" "$USER@$ip" bash -s <<EOF &
set -e
cd $REMOTE_DIR
SERVER_BATCHING=$SERVER_BATCHING \
LATENCY_DEBUG=$LATENCY_DEBUG \
nohup ./$BINARY \
    -id=$i \
    -path=${REMOTE_DIR}/config/$(basename "$CONFIG_PATH") \
    -et=$EVAL_TYPE \
    -n=$NUM_SERVERS \
    -t=$THRESHOLD \
    -b=$BATCHSIZE \
    -ms=$MSG_SIZE \
    -mode=$MODE \
    -mcli=$NUM_CLIENTS \
    -mload=$WORKLOAD \
    -indep=$INDEP_RATIO \
    -numobjects=$NUM_OBJECTS \
    -readratio=$READ_RATIO \
    -bcomp=$BATCH_COMPOSITION \
    -log=$LOG_LEVEL \
    -ep=$ENABLE_PRIORITY \
    -role=0 \
    > $log_file 2>&1 &
echo \$! > /tmp/woc_${i}.pid
EOF
    sleep 0.5
done

echo ""
echo "=============================================="
echo "Waiting for servers to finish loading and start listening..."
echo "=============================================="
for i in "${!SERVER_IPS[@]}"; do
    ip="${SERVER_IPS[$i]}"
    port=$((10000 + i))
    echo "  Waiting for server $i ($ip:$port) ..."
    for attempt in $(seq 1 60); do
        if (exec 3<>"/dev/tcp/${ip}/${port}") 2>/dev/null; then
            exec 3>&- 3<&- 2>/dev/null || true
            echo "    server $i is listening."
            break
        fi
        sleep 1
        if [ "$attempt" -eq 60 ]; then
            echo "    WARNING: server $i ($ip:$port) did not start listening within 60s"
        fi
    done
done

echo ""
echo "=============================================="
echo "Starting WOC clients on MongoDB workload '$WORKLOAD'..."
echo "=============================================="

for i in "${!CLIENT_HOST_IPS[@]}"; do
    ip="${CLIENT_HOST_IPS[$i]}"
    log_file="$LOG_DIR/client_${i}.log"

    echo "  Starting client $i on $ip..."
    for j in $(seq 0 $((CLIENTS_PER_VM - 1))); do
        client_id=$((NUM_SERVERS + i * CLIENTS_PER_VM + j))
        pin_server=$((client_id % NUM_SERVERS))
        ssh -i "$SSH_KEY" "$USER@$ip" bash -s <<EOF &
set -e
cd $REMOTE_DIR
PIPELINE_MODE=$PIPELINE_MODE \
MAX_INFLIGHT=$MAX_INFLIGHT \
USE_ADAPTIVE_LIMITER=$USE_ADAPTIVE_LIMITER \
LATENCY_DEBUG=$LATENCY_DEBUG \
ENABLE_TIMESERIES=$ENABLE_TIMESERIES \
TPS_TIMELINE_INTERVAL_MS=$TPS_TIMELINE_INTERVAL_MS \
nohup ./$BINARY \
    -id=$client_id \
    -path=${REMOTE_DIR}/config/$(basename "$CONFIG_PATH") \
    -et=$EVAL_TYPE \
    -n=$NUM_SERVERS \
    -t=$THRESHOLD \
    -b=$BATCHSIZE \
    -ms=$MSG_SIZE \
    -mode=$MODE \
    -role=1 \
    -ops=$OPS \
    -mload=$WORKLOAD \
    -indep=$INDEP_RATIO \
    -numobjects=$NUM_OBJECTS \
    -readratio=$READ_RATIO \
    -bcomp=$BATCH_COMPOSITION \
    -pinserver=$pin_server \
    -log=$LOG_LEVEL \
    -ep=$ENABLE_PRIORITY \
    -numclients=$NUM_CLIENTS \
    > $log_file 2>&1 &
EOF
        sleep 0.5
    done
done

echo ""
echo "=============================================="
echo " HETEROGENEOUS ${NUM_SERVERS}-NODE MONGODB CLUSTER STARTED"
echo "=============================================="
echo ""
echo "Cluster Configuration:"
echo "  - Config: $(basename "$CONFIG_PATH")"
echo "  - Servers: ${SERVER_IPS[*]}"
echo "  - Clients: ${CLIENT_HOST_IPS[*]}"
echo "  - Workload: $WORKLOAD"
echo "  - MongoDB: standalone mongod per node (port $MONGODB_PORT)"
echo ""
