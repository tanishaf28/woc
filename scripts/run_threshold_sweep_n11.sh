#!/bin/bash
# ================================================================
# Plain-msg Failure-Threshold Sweep, n=11 (WOC)
#
# Sweeps THRESHOLD (t) at the fixed 11-node heterogeneous cluster
# (config/cluster_hetero_11n_10c.conf, plain-msg/-et=0), with
# INDEP_RATIO=90.0, BATCHSIZE=1, MSG_SIZE=512 held fixed throughout -
# only t changes. t ranges 1..5 (floor((11-1)/2) = 5, the full valid
# range for n=11 per paper §3.2/§4.1: 1 <= t <= f = floor((n-1)/2)).
#
# Plain-msg counterpart of run_mongodb_threshold_sweep_n11.sh -- same t
# range/fixed params, against the plain-msg cluster (start_cluster_hetero.sh)
# instead of the MongoDB one. WOC's/Cabinet's side of the comparison
# (both have a real per-run-tunable threshold); EPaxos and Raft have no
# tunable per-run threshold, so their equivalent evals run once at n=11's
# natural majority (t=5) instead of sweeping it -- see
# run_threshold_baseline_n11.sh (epaxos) / run_threshold_sweep_n11_raft.sh
# (cabinet/raft).
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
CONFIG_PATH="${REPO_ROOT}/config/cluster_hetero_11n_10c.conf"

RESULT_ROOT="${SCRIPT_DIR}/results/threshold_sweep_n11"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

NUM_SERVERS=11
MAX_T=$(( (NUM_SERVERS - 1) / 2 ))
NUM_CLIENTS="${NUM_CLIENTS:-10}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
INDEP_RATIO_FIXED="${INDEP_RATIO_FIXED:-90.0}"
CLUSTER_ACTIVE=false

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_threshold_sweep_n11.sh

Sweeps failure threshold t=1..5 at the fixed n=11 heterogeneous cluster,
with INDEP_RATIO=90.0, BATCHSIZE=1, MSG_SIZE=512 fixed, against the WOC
plain-msg cluster (start_cluster_hetero.sh, -et=0).

Environment overrides:
  RUNTIME_SECONDS=30           wall-clock seconds per run
  NUM_CLIENTS=10                client count (10-VM pool)
  INDEP_RATIO_FIXED=90.0       fixed indep ratio for every case

Results archived under: results/threshold_sweep_n11/<timestamp>/<label>/
EOF
    exit 0
fi

mkdir -p "$RUN_DIR"

archive_case() {
    local label=$1
    local marker=$2
    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"
    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -newer "$marker" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi
    echo "  Archived results to: $dest_dir"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        CLIENT_COUNT="$NUM_CLIENTS" CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

run_case() {
    local t=$1
    local label=$2

    touch "${RUN_DIR}/.marker_${label}"
    local marker="${RUN_DIR}/.marker_${label}"

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "  n=${NUM_SERVERS}  t=${t}  indep=${INDEP_RATIO_FIXED}  runtime=${RUNTIME_SECONDS}s"
    echo "=================================================="

    CLUSTER_ACTIVE=true
    NUM_SERVERS="$NUM_SERVERS" NUM_CLIENTS="$NUM_CLIENTS" CONFIG_PATH="${CONFIG_PATH}" THRESHOLD="$t" \
        BATCHSIZE=1 MSG_SIZE=512 INDEP_RATIO="$INDEP_RATIO_FIXED" NUM_OBJECTS=1000 \
        PIPELINE_MODE=true MAX_INFLIGHT=5 LOG_LEVEL=info \
        bash "$START_SCRIPT"

    echo "  Running for ${RUNTIME_SECONDS}s..."
    sleep "$RUNTIME_SECONDS"

    CLIENT_COUNT="$NUM_CLIENTS" CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false

    archive_case "$label" "$marker"
}

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║        WOC PLAIN-MSG THRESHOLD SWEEP, n=11 (t=1..5)              ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"

for ((t = 1; t <= MAX_T; t++)); do
    run_case "$t" "n11_plain_t${t}"
done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 11

echo ""
echo "=================================================="
echo " WOC plain-msg threshold sweep (n=11) complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
