#!/bin/bash
# ================================================================
# EVAL 6: Batch Size Sweep x Cluster Size (PlainMsg)
#
# Sweeps BATCHSIZE (1,10,50,100,500,1000,2000 -- same points as
# eval_2_batching.sh) for each cluster size in CLUSTER_SIZES (3,5,7,11
# servers), using the fixed 11-IP server pool already used by
# config/cluster_hetero_{3,5,7,11}n_*.conf, paired with a dedicated
# 10-IP client pool (config/cluster_hetero_{n}n_10c.conf).
# Not MongoDB-backed (-et=0), same as eval_4_read_ratio.sh.
#
# Fixed across the sweep: INDEP_RATIO=90.0, MSG_SIZE=512.
#
# Delegates cluster lifecycle to start_cluster_hetero.sh/stop_cluster_hetero.sh
# via env-var injection instead of driving SSH/scp/nohup inline here -- same
# refactor as eval_5_i2d_size_sweep.sh, mirroring EPaxos's
# run_hetero_batchsize_sweep_10c.sh port of this script.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/woc"
RESULT_ROOT="${SCRIPT_DIR}/results/eval6_batchsize_size_sweep"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false

# Same 11-server / 10-client pools as start_cluster_hetero.sh/
# stop_cluster_hetero.sh -- only needed here for the pre-sweep stale-process
# purge below; the start/stop scripts do their own IP slicing internally
# from NUM_SERVERS/NUM_CLIENTS.
ALL_SERVER_IPS=(
    "192.168.73.59"  "192.168.73.243" "192.168.73.117" "192.168.73.16"
    "192.168.73.94"  "192.168.73.222" "192.168.73.250" "192.168.73.5"
    "192.168.73.237" "192.168.73.85"  "192.168.73.65"
)
ALL_CLIENT_IPS=(
    "192.168.73.159" "192.168.73.84"  "192.168.73.218" "192.168.73.219"
    "192.168.73.25"  "192.168.73.117" "192.168.73.16"  "192.168.73.94"
    "192.168.73.173" "192.168.73.71"
)

ALL_CLUSTER_SIZES=(3 5 7 11)
if [ -n "${CLUSTER_SIZES:-}" ]; then
    read -r -a ALL_CLUSTER_SIZES <<< "$CLUSTER_SIZES"
fi

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
# A number (<=10, the dedicated client pool size), or the literal string
# "match" to run clients=servers for each size in the sweep (capped at 10
# since this repo's start_cluster_hetero.sh doesn't cycle/reuse client VMs
# beyond the pool it's given).
CLIENT_COUNT="${CLIENT_COUNT:-10}"
TEST_CASES=(1 10 50 100 500 1000 2000)

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash eval_6_batchsize_size_sweep.sh

Sweeps BATCHSIZE over 1,10,50,100,500,1000,2000 (matches
run_hetero_batchsize_sweep_10c.sh's EPaxos port of this script) with
INDEP_RATIO=90.0, MSG_SIZE=512 fixed, across cluster sizes n=3,5,7,11,
against a dedicated 10-VM client pool (config/cluster_hetero_{n}n_10c.conf).

Environment overrides:
  RUNTIME_SECONDS=30          wall-clock seconds per run
  CLIENT_COUNT=10             client count (<=10), or "match" to run
                               clients=servers for each size (capped at 10)
  CLUSTER_SIZES="3 5 7 11"    override the cluster-size sweep

Results archived under: results/eval6_batchsize_size_sweep/<timestamp>/n<size>/<label>/
EOF
    exit 0
fi

if [ "$CLIENT_COUNT" != "match" ] && [ "$CLIENT_COUNT" -gt "${#ALL_CLIENT_IPS[@]}" ]; then
    echo "CLIENT_COUNT=$CLIENT_COUNT exceeds client pool size (${#ALL_CLIENT_IPS[@]})" >&2
    exit 1
fi

mkdir -p "$RUN_DIR"
touch "${RUN_DIR}/.run_start_marker"

remote_exec() {
    local host=$1
    shift
    ssh -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 "$USER@$host" "$*"
}

cleanup_all_nodes() {
    echo "=================================================="
    echo " Pre-sweep cleanup: purging stale WOC processes (all sizes)..."
    echo "=================================================="
    local all_ips=()
    printf '%s\n' "${ALL_SERVER_IPS[@]}" "${ALL_CLIENT_IPS[@]}" | sort -u > /tmp/eval6_ip_pool.$$
    mapfile -t all_ips < /tmp/eval6_ip_pool.$$
    rm -f /tmp/eval6_ip_pool.$$
    for ip in "${all_ips[@]}"; do
        remote_exec "$ip" "pkill -9 -x woc 2>/dev/null" &
    done
    wait
}

archive_latest_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/n${CURRENT_N}/${label}/merged"
    mkdir -p "$dest_dir"
    local marker="${RUN_DIR}/.last_archive_ts"
    local find_args=()
    if [ -f "$marker" ]; then
        find_args=(-newer "$marker")
    fi

    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" "${find_args[@]}" \
            -exec cp {} "$dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$dest_dir"/*.csv 2>/dev/null)" ]; then
            cp "$merged_dir"/*.csv "$dest_dir/" 2>/dev/null || true
        fi
    fi
    touch "$marker"
    echo "  Archived results to: $dest_dir"
    ls -1 "$dest_dir"/*.csv 2>/dev/null | sed 's|.*/|    |' || echo "  (no CSVs found)"
}

start_workload_cluster() {
    echo "Starting WOC heterogeneous cluster (n=${CURRENT_N}, clients=${CURRENT_CLIENT_COUNT})..."
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

stop_workload_cluster() {
    SERVER_COUNT="$CURRENT_N" CLIENT_COUNT="$CURRENT_CLIENT_COUNT" bash "$STOP_SCRIPT"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_workload_cluster || true
    fi
}
trap cleanup EXIT

run_case() {
    local label=$1
    local runtime=$2

    echo "=================================================="
    echo "Running: $label"
    echo "  n=${CURRENT_N}  threshold=${CURRENT_T}  clients=${CURRENT_CLIENT_COUNT}  runtime=${runtime}s"
    echo "=================================================="

    CLUSTER_ACTIVE=true
    start_workload_cluster
    sleep "$runtime"
    stop_workload_cluster
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"

    echo "  Cooling down to release TCP ports..."
    sleep 5
}

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     BATCHSIZE SWEEP x CLUSTER SIZE, 10-CLIENT POOL (WOC)       ║"
echo "║         Cluster sizes: n = 3, 5, 7, 11 (heterogeneous)         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

for n in "${ALL_CLUSTER_SIZES[@]}"; do
    t=$(( (n - 1) / 2 ))
    if [ "$CLIENT_COUNT" = "match" ]; then
        CURRENT_CLIENT_COUNT=$(( n < 10 ? n : 10 ))
    else
        CURRENT_CLIENT_COUNT=$CLIENT_COUNT
    fi
    CURRENT_N="$n"
    CURRENT_T="$t"

    echo ""
    echo "=================================================="
    echo " Cluster size n=${CURRENT_N} (threshold=${CURRENT_T}, clients=${CURRENT_CLIENT_COUNT})"
    echo "=================================================="

    for batchsize in "${TEST_CASES[@]}"; do
        BASE_ENV=(
            "NUM_SERVERS=${n}" "NUM_CLIENTS=${CURRENT_CLIENT_COUNT}" "THRESHOLD=${t}"
            "BATCHSIZE=${batchsize}" "INDEP_RATIO=90.0" "NUM_OBJECTS=1000"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=${MAX_INFLIGHT:-5}"
            "USE_ADAPTIVE_LIMITER=${USE_ADAPTIVE_LIMITER:-false}"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true"
        )
        run_case "n${n}_batch_${batchsize}" "$RUNTIME_SECONDS"
    done
done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR"

echo ""
echo "=================================================="
echo " Batchsize sweep (10-client pool) complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
echo "Summary CSV: $RUN_DIR/extracted_metrics.csv"
