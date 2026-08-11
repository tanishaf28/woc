#!/bin/bash
# ================================================================
# WOC MongoDB Batch Size Sweep, HETEROGENEOUS 5-NODE cluster.
#
# Sweeps BATCHSIZE over 1,10,50,100,500,1000,2000 (mirrors this repo's
# eval_6_batchsize_size_sweep.sh / epaxos port's
# run_mongodb_batchsize_sweep_5n.sh) with INDEP_RATIO=90 fixed, against
# the MongoDB-backed cluster (start_mongodb_hetero.sh, -et=1).
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_mongodb_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_mongodb_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
RESULT_ROOT="${SCRIPT_DIR}/results/mongodb_batchsize_sweep_5n"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false

# Must match start_mongodb_hetero.sh's SERVER_IPS / CLIENT_HOST_IPS --
# only used here for the pre-sweep stale-process purge.
ALL_NODE_IPS=(
"192.168.73.59" "192.168.73.243" "192.168.73.117" "192.168.73.16" "192.168.73.94"
"192.168.73.159" "192.168.73.84" "192.168.73.218" "192.168.73.219" "192.168.73.25"
)

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
WORKLOAD="${WORKLOAD:-a}"
TEST_CASES=(1 10 50 100 500 1000 2000)

if ! [[ "$RUNTIME_SECONDS" =~ ^[0-9]+$ ]] || [ "$RUNTIME_SECONDS" -lt 1 ]; then
    echo "WARNING: RUNTIME_SECONDS=${RUNTIME_SECONDS} is invalid. Using 30."
    RUNTIME_SECONDS=30
fi

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_mongodb_batchsize_sweep_5n.sh

Sweeps BATCHSIZE over 1,10,50,100,500,1000,2000 with INDEP_RATIO=90
fixed, against the heterogeneous 5-node WOC MongoDB replica set
(start_mongodb_hetero.sh, -et=1).

Environment overrides:
  RUNTIME_SECONDS=30   wall-clock seconds per run
  WORKLOAD=a           YCSB workload letter (a-f)

Results archived under: results/mongodb_batchsize_sweep_5n/<timestamp>/<label>/
EOF
    exit 0
fi

mkdir -p "$RUN_DIR"
touch "${RUN_DIR}/.run_start_marker"
# Pre-create the archive marker so even case #1 of the sweep only
# picks up CSVs written during this run, not every historical
# merged_*.csv ever produced (archive_latest_result's -newer filter
# is a no-op until this file exists).
touch "${RUN_DIR}/.last_archive_ts"

cleanup_all_nodes() {
    echo "=================================================="
    echo " Pre-sweep cleanup: purging stale woc/mongod processes..."
    echo "=================================================="
    for ip in "${ALL_NODE_IPS[@]}"; do
        ssh -o ConnectTimeout=5 -i "$SSH_KEY" "$USER@$ip" \
            "pkill -9 -x woc 2>/dev/null; pkill -9 -x mongod 2>/dev/null" &
    done
    wait
}

archive_latest_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}/merged"
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

run_case() {
    local label=$1
    local batch_size=$2

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "  n=5 (2s+3w)  batch=${batch_size}  workload=${WORKLOAD}  runtime=${RUNTIME_SECONDS}s"
    echo "=================================================="

    CLUSTER_ACTIVE=true
    INDEP_RATIO=90.0 BATCHSIZE="$batch_size" NUM_OBJECTS=1000 READ_RATIO=0.0 \
        bash "$START_SCRIPT" "$WORKLOAD"
    sleep "$RUNTIME_SECONDS"
    bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"

    echo "  Cooling down to release TCP ports..."
    sleep 5
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   WOC MONGODB BATCH SIZE SWEEP, HETEROGENEOUS 5-NODE           ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

for batch_size in "${TEST_CASES[@]}"; do
    run_case "n5_woc_mongo_batch_${batch_size}" "$batch_size"
done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " WOC MongoDB batch size sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
echo "Summary CSV: $RUN_DIR/extracted_metrics.csv"
