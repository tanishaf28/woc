#!/bin/bash
# ================================================================
# MongoDB Failure-Threshold Sweep, n=11 (WOC)
#
# Sweeps THRESHOLD (t) at the fixed 11-node heterogeneous cluster, with
# INDEP_RATIO=90, BATCHSIZE=1, MSG_SIZE=512 held fixed throughout - only t
# changes. t ranges 1..5 (floor((11-1)/2) = 5, the full valid range for
# n=11 per paper §3.2/§4.1: 1 <= t <= f = floor((n-1)/2)).
#
# This is WOC's/Cabinet's side of the threshold-sweep comparison - EPaxos
# and Raft have no per-run-tunable threshold, so their equivalent eval runs
# once at n=11's natural majority (t=5) instead of sweeping it; see the
# sibling script in epaxos/cabinet's scripts/ dir.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_mongodb_hetero_nsel.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_mongodb_hetero_nsel.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
RESULT_ROOT="${SCRIPT_DIR}/results/mongodb_threshold_sweep_n11"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false
NUM_SERVERS=11
MAX_T=$(( (NUM_SERVERS - 1) / 2 ))

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
WORKLOAD="${WORKLOAD:-a}"
INDEP_RATIO_FIXED="${INDEP_RATIO_FIXED:-90.0}"

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_mongodb_threshold_sweep_n11.sh

Sweeps failure threshold t=1..5 at the fixed n=11 heterogeneous cluster,
with INDEP_RATIO=90, BATCHSIZE=1, MSG_SIZE=512 fixed, against the WOC
MongoDB replica set.

Environment overrides:
  RUNTIME_SECONDS=30           wall-clock seconds per run
  WORKLOAD=a                   YCSB workload letter (a-f)
  INDEP_RATIO_FIXED=90.0       fixed indep ratio for every case

Results archived under: results/mongodb_threshold_sweep_n11/<timestamp>/<label>/
EOF
    exit 0
fi

mkdir -p "$RUN_DIR"
touch "${RUN_DIR}/.run_start_marker"
touch "${RUN_DIR}/.last_archive_ts"

cleanup_all_nodes() {
    echo "=================================================="
    echo " Pre-sweep cleanup: purging stale woc/mongod processes..."
    echo "=================================================="
    mapfile -t all_ips < <(awk 'NF >= 2 { print $2 }' "${REPO_ROOT}/config/cluster_hetero_11n_4s_7w.conf")
    for ip in "${all_ips[@]}"; do
        ssh -o ConnectTimeout=5 -i "$SSH_KEY" "$USER@$ip" "pkill -9 -x woc 2>/dev/null; pkill -9 -x mongod 2>/dev/null" &
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
}

run_case() {
    local label=$1
    local t=$2

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "  n=${NUM_SERVERS}  t=${t}  indep=${INDEP_RATIO_FIXED}  workload=${WORKLOAD}  runtime=${RUNTIME_SECONDS}s"
    echo "=================================================="

    CLUSTER_ACTIVE=true
    NUM_SERVERS="$NUM_SERVERS" THRESHOLD="$t" INDEP_RATIO="$INDEP_RATIO_FIXED" BATCHSIZE=1 MSG_SIZE=512 NUM_OBJECTS=1000 READ_RATIO=0.0 \
        bash "$START_SCRIPT" "$WORKLOAD"
    sleep "$RUNTIME_SECONDS"
    NUM_SERVERS="$NUM_SERVERS" bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"

    echo "  Cooling down to release TCP ports..."
    sleep 5
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        NUM_SERVERS="$NUM_SERVERS" bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║        WOC MONGODB THRESHOLD SWEEP, n=11 (t=1..5)               ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

for ((t = 1; t <= MAX_T; t++)); do
    run_case "n11_mongo_t${t}" "$t"
done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 11

echo ""
echo "=================================================="
echo " WOC MongoDB threshold sweep (n=11) complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
echo "Summary CSV: $RUN_DIR/extracted_metrics.csv"
