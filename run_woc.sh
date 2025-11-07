#!/bin/bash
# Configuration
declare -i NUM_SERVERS=5
declare -i NUM_CLIENTS=3
declare -i OPS=0       # Operations per client (set to 0 for infinite mode)
declare -i THRESHOLD=1      # Quorum threshold (f = threshold + 1)
declare -i EVAL_TYPE=0      # 0=plain msg, 1=mongodb
declare -i BATCHSIZE=10000    # Batch size (operations per RPC) - LARGE batches for atomic processing
declare -i MSG_SIZE=512     # Message size for plain msg
declare -i MODE=0           # 0=localhost, 1=distributed
declare -i CONFLICT_RATE=0 # Hot object conflict rate (0-100%) - minimal conflicts
INDEP_RATIO=0.0           # 98% independent objects (fast path)
COMMON_RATIO=100.0          # 1% common objects (slow path)
BATCH_COMPOSITION="mixed"  # "mixed" = all object types in one batch | "object-specific" = separate batches
PIPELINE_MODE="false"      # "true" = send batches without waiting (high throughput) | "false" = sequential (ordered)
declare -i MAX_INFLIGHT=3  # Maximum concurrent batches per client (only if PIPELINE_MODE=true)
CONFIG_PATH="./config/cluster_localhost.conf"
LOG_DIR="./logs"
BINARY="./bin/woc"
LOG_LEVEL="debug"          # Temporarily "debug" to see slow path routing
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
    PIPELINE_MODE="${PIPELINE_MODE}" MAX_INFLIGHT="${MAX_INFLIGHT}" "$BINARY" \
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
        -bcomp="${BATCH_COMPOSITION}" \
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
echo "  Batch comp:  ${BATCH_COMPOSITION}"
if [ "${PIPELINE_MODE}" = "true" ]; then
    echo "  Pipeline:    ENABLED (max ${MAX_INFLIGHT} concurrent batches)"
else
    echo "  Pipeline:    DISABLED (sequential mode)"
fi
echo ""
echo "Object Distribution:"
echo "  Independent: ${INDEP_RATIO}% (fast path, leaderless)"
echo "  Common:      ${COMMON_RATIO}% (slow path, leader-based)"
echo "  Hot/Conflict:${CONFLICT_RATE}% (slow path, serialized for conflicts)"
echo ""
echo "Batch Composition Mode:"
if [ "${BATCH_COMPOSITION}" == "mixed" ]; then
    echo "  ✓ MIXED: Each batch contains diverse objects & types"
    echo "    - Each of the ${BATCHSIZE} ops can target different objects"
    echo "    - Hot/Independent/Common distributed per ratios above"
    echo "    - Parallel processing on server side"
else
    echo "  ✓ OBJECT-SPECIFIC: Each batch targets ONE object"
    echo "    - All ${BATCHSIZE} ops in a batch write to same object"
    echo "    - Object type chosen per batch (Hot/Indep/Common)"
    echo "    - Sequential processing on server side"
fi
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
