#!/bin/bash
# ================================================================
# MongoDB Workload Sweep (WOC), HETEROGENEOUS 5-NODE (2 Strong + 3 Weak),
# standalone mongod per node.
#
# Sweeps YCSB WORKLOAD over a,b,c,d,e,f with INDEP_RATIO fixed at 90/10,
# BATCHSIZE=1, against the MongoDB-backed cluster (start_mongodb_hetero.sh,
# -et=1). Mirrors EPaxos's run_mongodb_workload_sweep_5n.sh but sweeps the
# orthogonal workload-type dimension instead of the indep/dependent ratio.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_mongodb_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_mongodb_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
RESULT_ROOT="${SCRIPT_DIR}/results/mongodb_workload_sweep_5n"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false

# Must match start_mongodb_hetero.sh's SERVER_IPS / CLIENT_HOST_IPS -- only
# used here for the pre-sweep stale-process purge.
ALL_NODE_IPS=(
"192.168.73.59" "192.168.73.243" "192.168.73.117" "192.168.73.16" "192.168.73.94"
"192.168.73.159" "192.168.73.84" "192.168.73.218" "192.168.73.219" "192.168.73.25"
)

# 60s (not the 30s used by the ratio/batch sweeps, which don't collect
# timeseries) - this eval now enables ENABLE_TIMESERIES per case, and a
# timeseries needs enough samples to show a real trend, not just the
# first-few-seconds ramp-up.
RUNTIME_SECONDS="${RUNTIME_SECONDS:-60}"
INDEP_RATIO_FIXED="${INDEP_RATIO_FIXED:-90}"
WORKLOADS=(a b c d e f)

if ! [[ "$RUNTIME_SECONDS" =~ ^[0-9]+$ ]] || [ "$RUNTIME_SECONDS" -lt 1 ]; then
    echo "WARNING: RUNTIME_SECONDS=${RUNTIME_SECONDS} is invalid. Using 60."
    RUNTIME_SECONDS=60
fi

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_mongodb_workload_sweep_5n.sh

Sweeps YCSB WORKLOAD over a,b,c,d,e,f with INDEP_RATIO fixed (default 90),
BATCHSIZE=1, against the heterogeneous 5-node (2 strong + 3 weak) MongoDB
cluster (start_mongodb_hetero.sh, -et=1).

Environment overrides:
  RUNTIME_SECONDS=30      wall-clock seconds per run
  INDEP_RATIO_FIXED=90    fixed INDEP_RATIO for every workload case

Results archived under: results/mongodb_workload_sweep_5n/<timestamp>/<label>/
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
    for ip in "${ALL_NODE_IPS[@]}"; do
        ssh -o ConnectTimeout=5 -i "$SSH_KEY" "$USER@$ip" \
            "pkill -9 -x woc 2>/dev/null; pkill -9 -x mongod 2>/dev/null" &
    done
    wait
}

archive_latest_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}"
    local merged_dest_dir="${dest_dir}/merged"
    mkdir -p "$merged_dest_dir"
    local marker="${RUN_DIR}/.last_archive_ts"
    local find_args=()
    if [ -f "$marker" ]; then
        find_args=(-newer "$marker")
    fi

    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" "${find_args[@]}" \
            -exec cp {} "$merged_dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$merged_dest_dir"/*.csv 2>/dev/null)" ]; then
            cp "$merged_dir"/*.csv "$merged_dest_dir/" 2>/dev/null || true
        fi
    fi

    # Timeseries CSVs go directly into dest_dir (not merged/) - this is the
    # layout plot_timeseries.py's find_timeline_csvs() scans for. Same
    # proven idiom as run_hetero5_crash_eval.sh's archive_latest_result.
    local timeline_src="${SCRIPT_DIR}/eval"
    if [ -d "$timeline_src" ]; then
        find "$timeline_src" -name "tps_timeline_*.csv" "${find_args[@]}" \
            -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi

    touch "$marker"
    echo "  Archived results to: $dest_dir"
    ls -1 "$merged_dest_dir"/*.csv 2>/dev/null | sed 's|.*/|    |' || echo "  (no merged CSVs found)"
    if ls "$dest_dir"/tps_timeline_*.csv >/dev/null 2>&1; then
        echo "    timeseries: $(ls "$dest_dir"/tps_timeline_*.csv | wc -l) file(s)"
    else
        echo "    WARNING: no tps_timeline_*.csv collected for ${label}"
    fi
}

run_case() {
    local label=$1
    local workload=$2

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "  n=5 (2s+3w)  workload=${workload}  indep=${INDEP_RATIO_FIXED}  runtime=${RUNTIME_SECONDS}s"
    echo "=================================================="

    CLUSTER_ACTIVE=true
    ENABLE_TIMESERIES=true TPS_TIMELINE_INTERVAL_MS="${TPS_TIMELINE_INTERVAL_MS:-500}" \
        INDEP_RATIO="$INDEP_RATIO_FIXED" BATCHSIZE=1 NUM_OBJECTS=1000 READ_RATIO=0.0 \
        bash "$START_SCRIPT" "$workload"
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
echo "║     MONGODB WORKLOAD SWEEP, HETEROGENEOUS 5-NODE (2s+3w) WOC    ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

for workload in "${WORKLOADS[@]}"; do
    run_case "n5_woc_mongo_workload_${workload}" "$workload"
done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "Generating timeseries plots (one per workload)..."
if python3 "${REPO_ROOT}/plot_timeseries.py" "$RUN_DIR"; then
    echo "  Plots written to: $RUN_DIR/plots/"
else
    echo "  WARNING: plot_timeseries.py failed or found no timeseries data - check tps_timeline_*.csv collection above."
fi

echo ""
echo "=================================================="
echo " MongoDB workload sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
echo "Summary CSV: $RUN_DIR/extracted_metrics.csv"
echo "Timeseries plots: $RUN_DIR/plots/"
