#!/bin/bash
# ================================================================
# MongoDB Batch Size Sweep x Cluster Size (WOC)
#
# Sweeps BATCHSIZE over 1,10,50,100,500,1000,2000 (same points as
# run_mongodb_batchsize_sweep_5n.sh / eval_6_batchsize_size_sweep.sh) for
# each cluster size in CLUSTER_SIZES (3,5,7,11 servers), against the
# MongoDB-backed cluster (start_mongodb_hetero_nsel.sh, -et=1).
# INDEP_RATIO=90.0 and MSG_SIZE=512 fixed throughout, matching the
# PlainMsg batch sweep and this repo's own run_mongodb_ratio_sweep_nsel.sh
# sibling script.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_mongodb_hetero_nsel.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_mongodb_hetero_nsel.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
RESULT_ROOT="${SCRIPT_DIR}/results/mongodb_batchsize_sweep_nsel"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false

ALL_CLUSTER_SIZES=(3 5 7 11)
if [ -n "${CLUSTER_SIZES:-}" ]; then
    read -r -a ALL_CLUSTER_SIZES <<< "$CLUSTER_SIZES"
fi

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
WORKLOAD="${WORKLOAD:-a}"
TEST_CASES=(1 10 50 100 500 1000 2000)

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_mongodb_batchsize_sweep_nsel.sh

Sweeps BATCHSIZE over 1,10,50,100,500,1000,2000 with INDEP_RATIO=90.0,
MSG_SIZE=512 fixed, across cluster sizes n=3,5,7,11, against the WOC
MongoDB replica set (start_mongodb_hetero_nsel.sh, -et=1).

Environment overrides:
  RUNTIME_SECONDS=30       wall-clock seconds per run
  WORKLOAD=a               YCSB workload letter (a-f)
  CLUSTER_SIZES="3 5 7 11" override the cluster-size sweep

Results archived under: results/mongodb_batchsize_sweep_nsel/<timestamp>/n<size>/<label>/
EOF
    exit 0
fi

mkdir -p "$RUN_DIR"
touch "${RUN_DIR}/.run_start_marker"
touch "${RUN_DIR}/.last_archive_ts"

cleanup_all_nodes() {
    echo "=================================================="
    echo " Pre-sweep cleanup: purging stale woc/mongod processes (all sizes)..."
    echo "=================================================="
    local all_ips=()
    for f in cluster_hetero_3n_2s_1w cluster_hetero_new cluster_hetero_7n_3s_4w cluster_hetero_11n_4s_7w; do
        awk 'NF >= 2 { print $2 }' "${REPO_ROOT}/config/${f}.conf" 2>/dev/null
    done | sort -u > /tmp/mongobatch_ip_pool.$$
    mapfile -t all_ips < /tmp/mongobatch_ip_pool.$$
    rm -f /tmp/mongobatch_ip_pool.$$
    for ip in "${all_ips[@]}"; do
        ssh -o ConnectTimeout=5 -i "$SSH_KEY" "$USER@$ip" "pkill -9 -x woc 2>/dev/null; pkill -9 -x mongod 2>/dev/null" &
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
}

run_case() {
    local label=$1
    local batch_size=$2

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "  n=${CURRENT_N}  batch=${batch_size}  indep=90.0  workload=${WORKLOAD}  runtime=${RUNTIME_SECONDS}s"
    echo "=================================================="

    CLUSTER_ACTIVE=true
    NUM_SERVERS="$CURRENT_N" INDEP_RATIO=90.0 BATCHSIZE="$batch_size" MSG_SIZE=512 NUM_OBJECTS=1000 READ_RATIO=0.0 \
        bash "$START_SCRIPT" "$WORKLOAD"
    sleep "$RUNTIME_SECONDS"
    NUM_SERVERS="$CURRENT_N" bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"

    echo "  Cooling down to release TCP ports..."
    sleep 5
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        NUM_SERVERS="$CURRENT_N" bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║    WOC MONGODB BATCH SIZE SWEEP x CLUSTER SIZE (n=3,5,7,11)      ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

for n in "${ALL_CLUSTER_SIZES[@]}"; do
    CURRENT_N="$n"
    echo ""
    echo "=================================================="
    echo " Cluster size n=${CURRENT_N}"
    echo "=================================================="
    for batch_size in "${TEST_CASES[@]}"; do
        run_case "n${n}_mongo_batch_${batch_size}" "$batch_size"
    done
done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR"

echo ""
echo "=================================================="
echo " WOC MongoDB batch size sweep x cluster size complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
echo "Summary CSV: $RUN_DIR/extracted_metrics.csv"
