#!/bin/bash
# ================================================================
# HETERO-5 RATIO SWEEP (WOC): NUM_SERVERS=5, NUM_CLIENTS=5,
# BATCHSIZE=1, MAX_INFLIGHT=5. Sweeps INDEP_RATIO over the standard
# 100/90/80/60/40/20/10/0 points used across the repo's other sweep
# scripts. No netem delay, no MongoDB -- plain steady-state per case,
# mirrors run_plain_b1_r90_5c5s.sh but looped over ratio.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_ratio_sweep_b1_5c5s"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
CLUSTER_ACTIVE=false

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=5"
    "BATCHSIZE=1"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "USE_ADAPTIVE_LIMITER=${USE_ADAPTIVE_LIMITER:-false}"
    "LOG_LEVEL=info"
)

TEST_CASES=(100.0 90.0 80.0 60.0 40.0 20.0 10.0 0.0)

mkdir -p "$RUN_DIR"

archive_case() {
    local label=$1
    local marker=$2
    local dest_dir="${RUN_DIR}/${label}/merged"
    mkdir -p "$dest_dir"
    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -newer "$marker" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi
    echo "  Archived results to: $dest_dir"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        SERVER_COUNT=5 CLIENT_COUNT=5 bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

run_case() {
    local indep=$1
    local label=$2

    touch "${RUN_DIR}/.marker_${label}"
    local marker="${RUN_DIR}/.marker_${label}"

    CLUSTER_ACTIVE=true
    env "${BASE_ENV[@]}" "INDEP_RATIO=${indep}" bash "$START_SCRIPT"

    echo "  Running for ${RUNTIME_SECONDS}s..."
    sleep "$RUNTIME_SECONDS"

    SERVER_COUNT=5 CLIENT_COUNT=5 bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false

    archive_case "$label" "$marker"
}

echo "================================================================"
echo " HETERO-5 RATIO SWEEP (WOC): batch=1, maxinflight=5, 5s/5c"
echo "================================================================"
echo "Result archive: $RUN_DIR"
echo "Sweep test cases (INDEP_RATIO): ${TEST_CASES[*]}"

case_num=1
for indep in "${TEST_CASES[@]}"; do
    echo ""
    echo "--- Sweep case ${case_num}/${#TEST_CASES[@]}: INDEP_RATIO=${indep} ---"
    run_case "$indep" "indep_${indep}"
    case_num=$((case_num + 1))
done

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 ratio sweep complete (WOC)"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
