#!/bin/bash

# ============================================
# WOC (Weighted Object Consensus) Cluster Launch Script
# ============================================
# 
# USAGE:
#   ./run_woc.sh                    # Start with default config
#   
#   # Customize via environment variables:
#   NUM_SERVERS=7 NUM_CLIENTS=3 OPS=500 ./run_woc.sh
#   INDEP_RATIO=80 COMMON_RATIO=20 ./run_woc.sh
#
# ARCHITECTURE:
#   - Leader (Server 0): Coordinates slow path consensus for common objects
#   - Followers (Server 1-N): Participate in both fast and slow paths
#   - Clients: Send requests to any server (round-robin or single)
#
# CONSENSUS PATHS:
#   1. FAST PATH (Independent Objects):
#      - Leaderless, peer-to-peer consensus
#      - Any replica can coordinate
#      - Weighted quorum based on object state
#      - Lower latency for independent operations
#
#   2. SLOW PATH (Common Objects):
#      - Leader-based consensus (Cabinet/Raft style)
#      - Client → Replica → Leader → Broadcast → Quorum → Commit
#      - Coordinator forwards to leader if needed
#      - Priority-weighted voting
#
# CONNECTIONS:
#   - All servers establish full mesh connectivity (server ↔ server)
#   - Clients connect to all servers (client → server)
#   - Same connections used for both fast and slow paths
#
# STARTUP SEQUENCE:
#   1. Leader (Server 0) starts and listens
#   2. Followers start and listen
#   3. All servers establish peer connections
#   4. Clients start and connect to servers
#   5. Clients send operations
#
# ============================================

set -e  # Exit on error
set -u  # Exit on undefined variable

# ============================================
# WOC Cluster Launch Script
# ============================================
# Starts leader (server 0), followers, and clients
# Supports both fast path (independent objects) and slow path (common objects)
# ============================================

# Configuration
declare -i NUM_SERVERS=5
declare -i NUM_CLIENTS=2
declare -i OPS=0       # Operations per client (set to 0 for infinite mode)
declare -i THRESHOLD=2      # Quorum threshold (f = threshold + 1)
declare -i EVAL_TYPE=0      # 0=plain msg, 1=mongodb
declare -i BATCHSIZE=10     # Batch size (operations per RPC)
declare -i MSG_SIZE=512     # Message size for plain msg
declare -i MODE=0           # 0=localhost, 1=distributed
declare -i CONFLICT_RATE=0 # Hot object conflict rate (0-100%)
INDEP_RATIO=100.0           # 70% independent objects (fast path)
COMMON_RATIO=0.0          # 10% common objects (slow path)
# Note: CONFLICT_RATE + INDEP_RATIO + COMMON_RATIO should equal 100
# Example: 20% hot + 70% independent + 10% common = 100%
CONFIG_PATH="./config/cluster_localhost.conf"
LOG_DIR="./logs"
BINARY="./bin/woc"
LOG_LEVEL="info"           # trace, debug, info, warn, error, fatal, panic
ENABLE_PRIORITY="true"     # true=cabinet mode, false=raft mode

# Track PIDs
declare -a SERVER_PIDS=()
declare -a CLIENT_PIDS=()

# Build the binary
echo "Building WOC binary..."
go build -o "$BINARY"
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
fi
echo "Build complete"

# Create log directories
mkdir -p "$LOG_DIR"

# Start servers (role=0)
echo ""
echo "============================================"
echo "Starting Servers (Leader + Followers)"
echo "============================================"
echo "Leader will be Server 0"
echo "Followers will be Server 1-$((NUM_SERVERS-1))"
echo ""

for ((i=0; i<NUM_SERVERS; i++)); do
    mkdir -p "${LOG_DIR}/server${i}"
    
    if [ $i -eq 0 ]; then
        echo "Starting Server ${i} (LEADER)..."
    else
        echo "Starting Server ${i} (FOLLOWER)..."
    fi
    
    "$BINARY" \
        -id=${i} \
        -n=${NUM_SERVERS} \
        -t=${THRESHOLD} \
        -path="${CONFIG_PATH}" \
        -pd=true \
        -role=0 \
        -ops=${OPS} \
        -b=${BATCHSIZE} \
        -indep=${INDEP_RATIO} \
        -common=${COMMON_RATIO} \
        -et=${EVAL_TYPE} \
        -ms=${MSG_SIZE} \
        -mode=${MODE} \
        -log="${LOG_LEVEL}" \
        -ep=${ENABLE_PRIORITY} \
        > "${LOG_DIR}/server${i}/output.log" 2>&1 &
    pid=$!
    SERVER_PIDS+=($pid)
    echo "  → PID: ${pid} | Log: ${LOG_DIR}/server${i}/output.log"
    
    # Longer delay after leader starts
    if [ $i -eq 0 ]; then
        sleep 3
    else
        sleep 1
    fi
done

# Wait for servers to establish connections
echo ""
echo "============================================"
echo "Waiting for server connections..."
echo "============================================"
echo "Leader establishing connections to followers..."
echo "Followers establishing connections to peers..."
echo "This may take up to 15 seconds..."
sleep 15

# Start clients (role=1)
echo ""
echo "============================================"
echo "Starting Clients"
echo "============================================"
for ((i=0; i<NUM_CLIENTS; i++)); do
    client_id=$((NUM_SERVERS + i))  # Client IDs start after server IDs
    mkdir -p "${LOG_DIR}/client${client_id}"
    
    echo "Starting Client ${client_id}..."
    "$BINARY" \
        -id=${client_id} \
        -n=${NUM_SERVERS} \
        -t=${THRESHOLD} \
        -path="${CONFIG_PATH}" \
        -ops=${OPS} \
        -et=${EVAL_TYPE} \
        -pd=true \
        -role=1 \
        -b=${BATCHSIZE} \
        -indep=${INDEP_RATIO} \
        -common=${COMMON_RATIO} \
        -conflictrate=${CONFLICT_RATE} \
        -ms=${MSG_SIZE} \
        -mode=${MODE} \
        -log="${LOG_LEVEL}" \
        > "${LOG_DIR}/client${client_id}/output.log" 2>&1 &
    pid=$!
    CLIENT_PIDS+=($pid)
    echo "  → PID: ${pid} | Log: ${LOG_DIR}/client${client_id}/output.log"
done

echo ""
echo "=============================================="
echo "Cluster Started Successfully"
echo "=============================================="
echo "Topology:"
echo "  Leader:      Server 0 (ID=0)"
echo "  Followers:   Server 1-$((NUM_SERVERS-1))"
echo "  Clients:     ${NUM_CLIENTS} (IDs: ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo ""
echo "Configuration:"
echo "  Servers:     ${NUM_SERVERS}"
echo "  Threshold:   ${THRESHOLD} (quorum = $((THRESHOLD+1)))"
if [ ${OPS} -le 0 ]; then
    echo "  Operations:  infinite (run until stopped)"
else
    echo "  Operations:  ${OPS} per client"
fi
echo "  Batch size:  ${BATCHSIZE} operations per RPC"
echo ""
echo "Object Distribution:"
echo "  Independent: ${INDEP_RATIO}% (fast path, leaderless)"
echo "  Common:      ${COMMON_RATIO}% (slow path, leader-based)"
echo "  Hot/Conflict:${CONFLICT_RATE}% (slow path, serialized for conflicts)"
echo ""
echo "System:"
echo "  Eval Type:   ${EVAL_TYPE} (0=plain msg, 1=mongodb)"
echo "  Mode:        ${MODE} (0=localhost, 1=distributed)"
echo "  Priority:    ${ENABLE_PRIORITY} (cabinet mode)"
echo "  Log Level:   ${LOG_LEVEL}"
echo "=============================================="
echo ""
echo "Monitor logs with:"
echo "  # Leader logs:"
echo "  tail -f ${LOG_DIR}/server0/output.log"
echo ""
echo "  # Follower logs:"
echo "  tail -f ${LOG_DIR}/server1/output.log"
echo ""
echo "  # Client logs:"
echo "  tail -f ${LOG_DIR}/client${NUM_SERVERS}/output.log"
echo ""
echo "View all errors:"
echo "  grep -i error ${LOG_DIR}/*/output.log"
echo ""

# Cleanup function for infinite mode
cleanup_infinite() {
    echo ""
    echo "Interrupt received, stopping all processes..."
    for pid in "${CLIENT_PIDS[@]}"; do
        kill $pid 2>/dev/null || true
    done
    for pid in "${SERVER_PIDS[@]}"; do
        kill $pid 2>/dev/null || true
    done
    sleep 2
    echo "All processes stopped."
    exit 0
}

# Wait for clients to finish (or run indefinitely)
if [ ${OPS} -le 0 ]; then
    echo "Clients running in infinite mode..."
    echo "Press Ctrl+C to stop all processes"
    echo ""
    # Set trap for user interrupt
    trap cleanup_infinite SIGINT SIGTERM
    wait
else
    echo "Waiting for clients to complete..."
    for pid in "${CLIENT_PIDS[@]}"; do
        wait $pid 2>/dev/null || {
            echo "Client PID $pid finished with error (or was killed)"
        }
    done

    echo ""
    echo "=============================================="
    echo "All clients completed!"
    echo "=============================================="
fi
echo "Check logs in:"
echo "  ${LOG_DIR}/server*/output.log"
echo "  ${LOG_DIR}/client*/output.log"
echo ""
echo "Servers are still running..."
echo "=============================================="
echo ""

# Optionally keep servers running or kill them
read -p "Press Enter to stop servers and exit (or Ctrl+C to keep them running)..."

# Cleanup
echo ""
echo "Stopping servers..."
for pid in "${SERVER_PIDS[@]}"; do
    kill $pid 2>/dev/null || echo "  Server PID $pid already stopped"
done

sleep 2

echo "Cleaning up any remaining processes..."
pkill -f "$BINARY" > /dev/null 2>&1 || true

echo ""
echo "=============================================="
echo "All processes stopped. Done."
echo "=============================================="