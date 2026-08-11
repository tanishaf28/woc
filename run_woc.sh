#!/bin/bash
###############################################################
# WOC Local Cluster Parameters (Synced with start_cluster.sh)
###############################################################

NUM_SERVERS=5                # Number of servers
NUM_CLIENTS=2               # Number of clients (increased for parallelism)
OPS=0                       # Operations per client (0 for infinite mode)
THRESHOLD=1                  # Quorum threshold (f = threshold + 1)
EVAL_TYPE=0                  # 0=plain msg, 1=mongodb
BATCHSIZE=1                  # Batch size
MSG_SIZE=512                 # Message size for plain msg
MODE=0                       # 0=localhost, 1=distributed
INDEP_RATIO=00.0             # % independent objects (fast path)
NUM_OBJECTS=1000             # size of the fixed, hash-ring-mapped object pool
BATCH_COMPOSITION="object-specific" # "mixed" or "object-specific"
PIPELINE_MODE="true"         # "true"=pipeline, "false"=sequential
MAX_INFLIGHT=3               # Adaptive limiter: starts at 2, auto-adjusts up to 3
CONFIG_PATH="./config/cluster_localhost.conf"
LOG_DIR="./logs"
BINARY="./woc"
LOG_LEVEL="info"             # "info" for production, "debug" for verbose logs
ENABLE_PRIORITY="true"       # "true"=cabinet, "false"=raft
LATENCY_DEBUG="false"        # "true"=Cabinet-style latency breakdown (fast/slow/forward)
SERVER_BATCHING="false"       # Batch requests before consensus

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
    
    SERVER_BATCHING="${SERVER_BATCHING}" \
    LATENCY_DEBUG="${LATENCY_DEBUG}" \
    GOGC=50 GOMEMLIMIT=1GiB GOMAXPROCS=4 \
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
        -numobjects=${NUM_OBJECTS} \
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
echo "Waiting for server initialization..."
echo "============================================"
echo "Leader establishing connections to followers..."
echo "All servers pre-warming the ${NUM_OBJECTS}-object pool..."
echo "This may take up to 20 seconds..."
sleep 20  # ✅ INCREASED from 15s - gives time for connections + pre-warming

# Start clients (role=1)
echo ""
echo "============================================"
echo "Starting Clients"
echo "============================================"
for ((i=0; i<NUM_CLIENTS; i++)); do
    client_id=$((NUM_SERVERS + i))  # Client IDs start after server IDs
    pin_server=$((i % NUM_SERVERS))  # -pinserver is required; spread clients round-robin at launch
    mkdir -p "${LOG_DIR}/client${client_id}"

    echo "Starting Client ${client_id} (pinserver=${pin_server})..."
    PIPELINE_MODE="${PIPELINE_MODE}" \
    MAX_INFLIGHT="${MAX_INFLIGHT}" \
    GOGC=50 GOMEMLIMIT=1GiB GOMAXPROCS=2 \
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
        -numobjects=${NUM_OBJECTS} \
        -bcomp="${BATCH_COMPOSITION}" \
        -ms=${MSG_SIZE} \
        -mode=${MODE} \
        -pinserver=${pin_server} \
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
    echo "  Pipeline:    ENABLED (adaptive: starts at 2, max ${MAX_INFLIGHT} concurrent)"
else
    echo "  Pipeline:    DISABLED (sequential mode)"
fi
echo ""
echo "Object Distribution:"
echo "  Independent: ${INDEP_RATIO}% (fast path, leaderless)"
echo "  Dependent:   $((100 - ${INDEP_RATIO%.*}))% (slow path, leader-based)"
echo "  Object pool: ${NUM_OBJECTS} objects (hash-ring mapped to replicas)"
echo ""
echo "Batch Composition Mode:"
if [ "${BATCH_COMPOSITION}" == "mixed" ]; then
    echo "   MIXED: Each batch contains diverse objects & types"
    echo "    - Each of the ${BATCHSIZE} ops can target different objects"
    echo "    - Parallel processing on server side"
else
    echo "   OBJECT-SPECIFIC: Each batch targets ONE object"
    echo "    - All ${BATCHSIZE} ops in a batch write to same object"
    echo "    - Sequential processing on server side"
fi
echo ""
echo "Adaptive Concurrency Control:"
if [ "${NO_LIMITER}" = "true" ]; then
    echo "  Limiter:       NONE (zero overhead) - LOCALHOST ONLY ⚡"
    echo "  ⚠️  WARNING: Only use NO_LIMITER for localhost testing!"
    echo "  Concurrency:   UNLIMITED (no backpressure control)"
elif [ "${USE_SIMPLE_LIMITER}" = "true" ]; then
    echo "  Limiter:       SIMPLE (lock-free semaphore) - OPTIMIZED ✓"
    echo "  Fixed limit:   ${MAX_INFLIGHT} concurrent batches per client"
else
    echo "  Limiter:       ADAPTIVE (dynamic adjustment)"
    echo "  Initial limit: 2 concurrent batches per client"
    echo "  Max limit:     ${MAX_INFLIGHT} concurrent batches per client"
    echo "  Auto-adjusts based on fast path ratio and latency"
fi
echo ""
echo "System:"
echo "  Eval Type:   ${EVAL_TYPE} (0=plain msg, 1=mongodb)"
echo "  Mode:        ${MODE} (0=localhost, 1=distributed)"
echo "  Priority:    ${ENABLE_PRIORITY} (cabinet mode)"
echo "  Log Level:   ${LOG_LEVEL}"
if [ "${LATENCY_DEBUG}" = "true" ]; then
    echo "  Latency:     DETAILED BREAKDOWN ENABLED"
    echo "               (Client RPC, Server Processing, Consensus phases)"
else
    echo "  Latency:     Standard logging (set LATENCY_DEBUG=true for detailed breakdown)"
fi
echo ""
echo "Optimizations:"
if [ "${NO_LIMITER}" = "true" ]; then
    echo "  ⚡ NoOpLimiter enabled (ZERO overhead - localhost only!)"
    echo "     → Matches Cabinet latency (~0.6-1.0ms)"
elif [ "${USE_SIMPLE_LIMITER}" = "true" ]; then
    echo "  ✓ SimpleLimiter enabled (300-600μs savings per request)"
    echo "     → Lock-free channel, ~1.2-1.8ms latency"
else
    echo "  ○ Using AdaptiveLimiter (set USE_SIMPLE_LIMITER=true for optimization)"
    echo "     → Mutex-based, ~2.0-3.0ms latency"
fi
echo "  ✓ Batched metrics recording (100-300μs savings)"
echo "  ✓ Sequential slow path worker (consensus requires ordering)"
echo "  ✓ Latency breakdown instrumentation active"
if [ ${MODE} -eq 1 ]; then
    echo "  Mode:        Distributed/Cloud (conservative tuning)"
else
    echo "  Mode:        Localhost (aggressive tuning)"
fi
echo "=============================================="
echo ""
echo "Monitor logs with:"
echo "  # Leader logs:"
echo "  tail -f ${LOG_DIR}/server0/output.log"
echo ""
echo "  # Follower logs:"
echo "  tail -f ${LOG_DIR}/server1/output.log"
echo ""
echo "  # Client logs (watch adaptive limit changes):"
echo "  tail -f ${LOG_DIR}/client${NUM_SERVERS}/output.log | grep -E '(⬆|⬇|Limit=)'"
echo ""
echo "View all errors:"
echo "  grep -i error ${LOG_DIR}/*/output.log"
echo ""

# Cleanup function for infinite mode
cleanup_infinite() {
    echo ""
    echo "Interrupt received, stopping all processes..."
    echo "Waiting for graceful shutdown (up to 30s)..."
    
    # Send SIGTERM to clients (triggers graceful shutdown)
    for pid in "${CLIENT_PIDS[@]}"; do
        kill -TERM $pid 2>/dev/null || true
    done
    
    # Wait up to 30s for clients to finish
    timeout=30
    elapsed=0
    while [ $elapsed -lt $timeout ]; do
        alive=0
        for pid in "${CLIENT_PIDS[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                alive=$((alive + 1))
            fi
        done
        
        if [ $alive -eq 0 ]; then
            echo "All clients stopped gracefully"
            break
        fi
        
        echo "Waiting for $alive clients to finish... (${elapsed}s)"
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    # Force kill any remaining clients
    for pid in "${CLIENT_PIDS[@]}"; do
        kill -9 $pid 2>/dev/null || true
    done
    
    # Stop servers gracefully (SIGINT for Go signal handler)
    echo "Sending shutdown signal to servers..."
    for pid in "${SERVER_PIDS[@]}"; do
        kill -INT $pid 2>/dev/null || true
    done
    
    # Wait for servers to save metrics (up to 15 seconds)
    echo "Waiting for servers to save metrics..."
    timeout=15
    elapsed=0
    while [ $elapsed -lt $timeout ]; do
        alive=0
        for pid in "${SERVER_PIDS[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                alive=$((alive + 1))
            fi
        done
        
        if [ $alive -eq 0 ]; then
            echo "✓ All servers stopped gracefully"
            break
        fi
        
        if [ $elapsed -eq 0 ] || [ $((elapsed % 3)) -eq 0 ]; then
            echo "  Waiting for $alive servers to save metrics... (${elapsed}s)"
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    
    # Force kill any remaining servers
    for pid in "${SERVER_PIDS[@]}"; do
        kill -9 $pid 2>/dev/null || true
    done
    
    sleep 1
    echo "All processes stopped."
    
    # Show where metrics were saved
    echo ""
    echo "=============================================="
    echo "Metrics saved to:"
    echo "  Client metrics: ./eval/client*/"
    echo "  Server metrics: ./eval/server*/"
    echo "=============================================="
    exit 0
}

# Wait for clients to finish (or run indefinitely)
if [ ${OPS} -le 0 ]; then
    echo "Clients running in infinite mode..."
    echo "Press Ctrl+C to trigger graceful shutdown"
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
echo "Check metrics in:"
echo "  ./eval/client*/client*_eval_*.csv"
echo ""
echo "Servers are still running..."
echo "=============================================="
echo ""

# Optionally keep servers running or kill them
read -p "Press Enter to stop servers and exit (or Ctrl+C to keep them running)..."

# Cleanup
echo ""
echo "Stopping servers gracefully..."
for pid in "${SERVER_PIDS[@]}"; do
    kill -INT $pid 2>/dev/null || echo "  Server PID $pid already stopped"
done

echo "Waiting for servers to save metrics (up to 15s)..."
timeout=15
elapsed=0
while [ $elapsed -lt $timeout ]; do
    alive=0
    for pid in "${SERVER_PIDS[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            alive=$((alive + 1))
        fi
    done
    
    if [ $alive -eq 0 ]; then
        echo "✓ All servers stopped gracefully"
        break
    fi
    
    sleep 1
    elapsed=$((elapsed + 1))
done

# Force kill any remaining
for pid in "${SERVER_PIDS[@]}"; do
    kill -9 $pid 2>/dev/null || true
done

sleep 1

echo "Cleaning up any remaining processes..."
pkill -f "$BINARY" > /dev/null 2>&1 || true

echo ""
echo "=============================================="
echo "All processes stopped. Done."
echo "=============================================="