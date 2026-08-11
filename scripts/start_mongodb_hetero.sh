#!/bin/bash
# ================================================================
# MongoDB Cluster Launcher - HETEROGENEOUS 5-NODE (current cluster IPs)
# Starts mongod with --fork so the daemon detaches from SSH sessions.
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

# -----------------------------
# USER / SSH CONFIG
# -----------------------------
USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"

# -----------------------------
# REMOTE DIRECTORY SETUP
# -----------------------------
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_new.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# MongoDB-specific parameters
MONGODB_PORT=27017

# WOC PARAMETERS
NUM_SERVERS=5                          # fixed: SERVER_IPS below has exactly 5 real hosts
NUM_CLIENTS="${NUM_CLIENTS:-5}"
THRESHOLD="${THRESHOLD:-1}"
OPS="${OPS:-0}"         # 0 = infinite mode (client.go); run is bounded by run_all_evals.sh's RUNTIME_SECONDS instead
EVAL_TYPE=1            # 1 = MongoDB
BATCHSIZE="${BATCHSIZE:-10}"
MODE="${MODE:-1}"
INDEP_RATIO="${INDEP_RATIO:-100.0}"
NUM_OBJECTS="${NUM_OBJECTS:-1000}"
READ_RATIO="${READ_RATIO:-0.0}"
BATCH_COMPOSITION="${BATCH_COMPOSITION:-object-specific}"
LOG_LEVEL="${LOG_LEVEL:-info}"
ENABLE_PRIORITY="${ENABLE_PRIORITY:-true}"

# These environment variables are read via os.Getenv in client.go/
# consensus.go, NOT CLI flags -- they must be exported as VAR=value prefixes
# on the binary invocation below, not passed as -flag=value. (-pipeline and
# -maxinflight CLI flags exist but are dead code -- nothing in the binary
# ever reads them.)
PIPELINE_MODE="${PIPELINE_MODE:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-5}"
USE_ADAPTIVE_LIMITER="${USE_ADAPTIVE_LIMITER:-false}"
SERVER_BATCHING="${SERVER_BATCHING:-false}"
LATENCY_DEBUG="${LATENCY_DEBUG:-false}"
# Off by default (matches every other sweep script except the crash eval) -
# callers that want per-client TPS timeline CSVs (e.g. run_mongodb_delay_
# eval_5n.sh, run_mongodb_workload_sweep_5n.sh) set ENABLE_TIMESERIES=true
# before calling this script.
ENABLE_TIMESERIES="${ENABLE_TIMESERIES:-false}"
TPS_TIMELINE_INTERVAL_MS="${TPS_TIMELINE_INTERVAL_MS:-500}"

# 5-Node Cluster: 2 Strong (c16) + 3 Weak (c4)
SERVER_IPS=(
"192.168.73.59"
"192.168.73.243"
"192.168.73.117"
"192.168.73.16"
"192.168.73.94"
)

CLIENT_HOST_IPS=(
"192.168.73.159"
"192.168.73.84"
"192.168.73.218"
"192.168.73.219"
"192.168.73.25"
)

CLIENTS_PER_VM=1

# Build WOC binary locally
echo "=============================================="
echo "Building WOC binary locally..."
echo "=============================================="
(cd "$REPO_ROOT" && go build -o "${SCRIPT_DIR}/${BINARY}")
echo "✓ Build complete."

# Copy binary to all VMs
copy_binary() {
    local TARGET_IP=$1
    echo "  Copying binary to $TARGET_IP ..."
    scp -i $SSH_KEY "$BINARY" $USER@$TARGET_IP:$REMOTE_DIR/
}

copy_config() {
    local TARGET_IP=$1
    echo "  Copying config to $TARGET_IP ..."
    scp -i $SSH_KEY "$CONFIG_PATH" $USER@$TARGET_IP:$REMOTE_DIR/config/
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
    ssh -i $SSH_KEY $USER@$TARGET_IP bash -s <<'EOF'
set -e
MONGODB_PORT=27017
REMOTE_DIR="/home/ubuntu/woc"

# Stop existing MongoDB if running.
pkill -x mongod 2>/dev/null || true
sleep 1

# Clear stale database state from previous runs, then recreate directories.
rm -rf $REMOTE_DIR/mongodb_data/* $REMOTE_DIR/mongodb_data/.[!.]* $REMOTE_DIR/mongodb_data/..?* 2>/dev/null || true
rm -f $REMOTE_DIR/mongodb_data/mongod.lock $REMOTE_DIR/mongodb_data/WiredTiger.lock $REMOTE_DIR/logs/mongod.log 2>/dev/null || true
mkdir -p $REMOTE_DIR/mongodb_data $REMOTE_DIR/logs

# Standalone mongod (no replica set) -- each server's MongoFollower
# connects to its own local instance via MONGODB_URI (defaults to
# localhost:27017), so there's no need for cross-node replication.
mongod --port $MONGODB_PORT \
    --dbpath $REMOTE_DIR/mongodb_data \
    --bind_ip 0.0.0.0 \
    --logpath $REMOTE_DIR/logs/mongod.log \
    --logappend \
    --fork --pidfilepath $REMOTE_DIR/mongodb_data/mongod.pid

sleep 2
echo "✓ MongoDB started on $MONGODB_PORT"
EOF
}

wait_for_mongo_ready() {
    local TARGET_IP=$1
    local NODE_LABEL=$2
    local attempt

    for attempt in $(seq 1 30); do
        if ssh -i $SSH_KEY $USER@$TARGET_IP "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done

    echo "  Warning: MongoDB readiness timed out on ${NODE_LABEL} (${TARGET_IP})"
    return 1
}

# Copy binaries and configs to all servers
echo ""
echo "=============================================="
echo "Distributing to SERVER nodes..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}"; do
    copy_binary "$ip"
    copy_config "$ip"
    sync_load_file "$ip"
done

# Copy binaries to client nodes
echo ""
echo "=============================================="
echo "Distributing to CLIENT nodes..."
echo "=============================================="
for ip in "${CLIENT_HOST_IPS[@]}"; do    copy_binary "$ip"
    copy_config "$ip"
done

# Setup MongoDB on all servers
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

# Start WOC servers
echo ""
echo "=============================================="
echo "Starting WOC servers (heterogeneous 5-node)..."
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
    -path=$CONFIG_PATH \
    -et=$EVAL_TYPE \
    -n=$NUM_SERVERS \
    -t=$THRESHOLD \
    -b=$BATCHSIZE \
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

# Wait for each server's RPC listener to actually accept connections before
# starting clients. Servers now do a real MongoDB load (recordcount=100000)
# in initMongoDB() *before* calling net.Listen, which can take several
# seconds -- a fixed short sleep here used to be enough back when the load
# step was a no-op (missing workload.dat), but now clients that connect too
# early get "connection refused" and fatal out.
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

# Start WOC clients
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
    -path=$CONFIG_PATH \
    -et=$EVAL_TYPE \
    -n=$NUM_SERVERS \
    -t=$THRESHOLD \
    -b=$BATCHSIZE \
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
echo "✓ HETEROGENEOUS 5-NODE MONGODB CLUSTER STARTED"
echo "=============================================="
echo ""
echo "Cluster Configuration:"
echo "  - Servers: 192.168.73.59, 192.168.73.243, 192.168.73.117, 192.168.73.16, 192.168.73.94"
echo "  - Clients: 192.168.73.159, 192.168.73.84, 192.168.73.218, 192.168.73.219, 192.168.73.25"
echo "  - Workload: $WORKLOAD"
echo "  - MongoDB: standalone mongod per node (port $MONGODB_PORT)"
echo ""