#!/bin/bash
# ================================================================
# MongoDB Cluster Launcher - HETEROGENEOUS 5-NODE (2 Strong + 3 Weak)
# ================================================================

set -e
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

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
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_2s3w.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# MongoDB-specific parameters
MONGODB_PORT=27017
MONGODB_REPLICA_SET="wocrs"

# WOC PARAMETERS
NUM_SERVERS=5
NUM_CLIENTS=2
THRESHOLD=1
OPS=0
EVAL_TYPE=0
BATCHSIZE=10
MSG_SIZE=512
MODE=1
CONFLICT_RATE=0
INDEP_RATIO=100.0
COMMON_RATIO=0.0
BATCH_COMPOSITION="object-specific"
PIPELINE_MODE="true"
MAX_INFLIGHT=5
USE_ADAPTIVE_LIMITER="false"
PARALLEL_FAST_PATH="true"
LOG_LEVEL="info"
ENABLE_PRIORITY="true"
LATENCY_DEBUG="false"
SERVER_BATCHING="false"

# 5-Node Cluster: 2 Strong (c16) + 3 Weak (c4)
SERVER_IPS=(
"192.168.73.159"  # tani-hetero-c16-1 (strong)
"192.168.73.84"   # tani-hetero-c16-2 (strong)
"192.168.73.69"   # tani-c4-1 (weak)
"192.168.73.235"  # tani-c4-2 (weak)
"192.168.73.194"  # tani-c4-3 (weak)
)

CLIENT_HOST_IPS=(
"192.168.73.218"
"192.168.73.219"
)

CLIENTS_PER_VM=1

# Build WOC binary locally
echo "=============================================="
echo "Building WOC binary locally..."
echo "=============================================="
go build -o "$BINARY"
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

setup_mongodb() {
    local TARGET_IP=$1
    local NODE_ID=$2
    echo "  Setting up MongoDB on $TARGET_IP (Node $NODE_ID) ..."
    ssh -i $SSH_KEY $USER@$TARGET_IP bash -s <<'EOF'
set -e
MONGODB_PORT=27017
MONGODB_REPLICA_SET="wocrs"
REMOTE_DIR="/home/ubuntu/woc"

# Stop existing MongoDB if running.
pkill -f mongod 2>/dev/null || true
sleep 1

# Clear stale database state from previous runs, then recreate directories.
rm -rf $REMOTE_DIR/mongodb_data/* $REMOTE_DIR/mongodb_data/.[!.]* $REMOTE_DIR/mongodb_data/..?* 2>/dev/null || true
rm -f $REMOTE_DIR/mongodb_data/mongod.lock $REMOTE_DIR/mongodb_data/WiredTiger.lock $REMOTE_DIR/logs/mongod.log 2>/dev/null || true
mkdir -p $REMOTE_DIR/mongodb_data $REMOTE_DIR/logs

# Start MongoDB on this node in the background.
nohup mongod --port $MONGODB_PORT \
    --replSet $MONGODB_REPLICA_SET \
    --dbpath $REMOTE_DIR/mongodb_data \
    --bind_ip 0.0.0.0 \
    --logpath $REMOTE_DIR/logs/mongod.log \
    --logappend \
    > $REMOTE_DIR/logs/mongod.out 2>&1 &

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

wait_for_replica_set_ready() {
        local attempt

        for attempt in $(seq 1 60); do
                if ssh -i $SSH_KEY $USER@${SERVER_IPS[0]} "mongosh --quiet --eval '
try {
    var status = rs.status();
    var ready = status.members.every(function (m) {
        return m.stateStr === \"PRIMARY\" || m.stateStr === \"SECONDARY\";
    });
    if (ready) {
        quit(0);
    }
    quit(1);
} catch (e) {
    quit(1);
}
' >/dev/null 2>&1"; then
                        echo "  ✓ Replica set is ready"
                        return 0
                fi
                sleep 1
        done

        echo "  Warning: replica set never reached PRIMARY/SECONDARY readiness"
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
done

# Copy binaries to client nodes
echo ""
echo "=============================================="
echo "Distributing to CLIENT nodes..."
echo "=============================================="
for ip in "${CLIENT_HOST_IPS[@]}"; do
    copy_binary "$ip"
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

# Initialize MongoDB Replica Set (only on first node)
echo ""
echo "=============================================="
echo "Initializing MongoDB Replica Set..."
echo "=============================================="
FIRST_SERVER="${SERVER_IPS[0]}"
echo "Connecting to $FIRST_SERVER:$MONGODB_PORT to initialize replica set..."

ssh -i $SSH_KEY $USER@$FIRST_SERVER bash -s <<EOF
set -e
MONGODB_PORT=27017
MONGODB_REPLICA_SET="wocrs"

mongosh --port $MONGODB_PORT --eval "
rs.initiate({
    _id: '$MONGODB_REPLICA_SET',
    members: [
        {_id: 0, host: '${SERVER_IPS[0]}:$MONGODB_PORT'},
        {_id: 1, host: '${SERVER_IPS[1]}:$MONGODB_PORT'},
        {_id: 2, host: '${SERVER_IPS[2]}:$MONGODB_PORT'},
        {_id: 3, host: '${SERVER_IPS[3]}:$MONGODB_PORT'},
        {_id: 4, host: '${SERVER_IPS[4]}:$MONGODB_PORT'}
    ]
})
" 2>/dev/null || echo "Replica set may already be initialized"

sleep 2
mongosh --port $MONGODB_PORT --eval "rs.status()"
EOF

sleep 5
wait_for_replica_set_ready || true

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
nohup ./$BINARY \
    -id=$i \
    -path=$CONFIG_PATH \
    -et=1 \
    -n=$NUM_SERVERS \
    -t=$THRESHOLD \
    -b=$BATCHSIZE \
    -mode=$MODE \
    -mcli=$NUM_CLIENTS \
    -mload=$WORKLOAD \
    -role=0 \
    > $log_file 2>&1 &
echo \$! > /tmp/woc_${i}.pid
EOF
    sleep 0.5
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
        ssh -i "$SSH_KEY" "$USER@$ip" bash -s <<EOF &
set -e
cd $REMOTE_DIR
nohup ./$BINARY \
    -id=$client_id \
    -path=$CONFIG_PATH \
    -et=1 \
    -n=$NUM_SERVERS \
    -t=$THRESHOLD \
    -b=$BATCHSIZE \
    -mode=$MODE \
    -role=1 \
    -mload=$WORKLOAD \
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
echo "  - 2 Strong nodes (c16): 192.168.73.159, 192.168.73.84"
echo "  - 3 Weak nodes (c4):   192.168.73.69, 192.168.73.235, 192.168.73.194"
echo "  - Workload: $WORKLOAD"
echo "  - MongoDB Replica Set: $MONGODB_REPLICA_SET"
echo ""
echo "Monitor logs with: tail -f $LOG_DIR/*.log"
echo "Stop cluster with: bash ./stop_mongodb_hetero_5n.sh"
