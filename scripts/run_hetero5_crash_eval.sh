#!/bin/bash
# ================================================================
# HETERO-5 CRASH EVAL (CORA/WOC): follower crashes at replica 2, 3, 4
# on the fixed 5-server heterogeneous cluster.
#
#   ./run_hetero5_crash_eval.sh [replica2|replica3|replica4|leader|all] [batchsize] [indep_ratio]
#
# Defaults to running all three follower cases. "leader" is supported but
# not part of the default sweep, since it isn't run by default for the
# other three protocols either - pass it explicitly to test it.
#
# NUM_CLIENTS=5, BATCHSIZE=100 (default), RUNTIME_SECONDS=60, and the
# 5-host CLIENT_IPS list below are shared byte-for-byte with epaxos's and
# cabinet's own crash-eval drivers (scripts/run_hetero5_crash_eval.sh in
# epaxos; scripts/run_hetero_crash_{cab,raft}.sh in cabinet) so all four
# protocols' crash evals run under identical offered load and are
# comparable. THRESHOLD stays CORA-native (2) rather than matching
# Cabinet's t=1 - it's a real fault-tolerance knob, not incidental drift,
# so it's left to the caller to override via env if a like-for-like t is
# wanted.
#
# Uses sampler_replacement.sh's run_crash_case_sampled (per-client
# in-process TPS timeline + event injection) -- this script is now its
# only caller.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# shellcheck source=../sampler_replacement.sh
source "${REPO_ROOT}/sampler_replacement.sh"

TARGET="${1:-all}"
BATCHSIZE_OVERRIDE="${2:-100}"
INDEP_RATIO_OVERRIDE="${3:-90.0}"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/woc"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_crash_eval"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
touch_marker="${RUN_DIR}/.run_start_marker"

CLUSTER_ACTIVE=false

RUNTIME_SECONDS="${RUNTIME_SECONDS:-60}"
CRASH_TRIGGER_SECONDS="${CRASH_TRIGGER_SECONDS:-10}"

NUM_CLIENTS="${NUM_CLIENTS:-5}"

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=${NUM_CLIENTS}"
    "THRESHOLD=2"
    "BATCHSIZE=${BATCHSIZE_OVERRIDE}"
    "MSG_SIZE=512"
    "INDEP_RATIO=${INDEP_RATIO_OVERRIDE}"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "ENABLE_TIMESERIES=true"
    "LOG_LEVEL=info"
    "ENABLE_PRIORITY=true"
)

SERVER_IPS=(
    "192.168.73.59"
    "192.168.73.243"
    "192.168.73.27"
    "192.168.73.157"
    "192.168.73.78"
)

# Same 5-host pool used by epaxos/cabinet/raft's crash scripts, sliced to
# NUM_CLIENTS so all four protocols' crash evals run on identical client
# VMs under an identical client count.
ALL_CLIENT_IPS=(
    "192.168.73.159"
    "192.168.73.84"
    "192.168.73.218"
    "192.168.73.219"
    "192.168.73.25"
)
CLIENT_IPS=("${ALL_CLIENT_IPS[@]:0:NUM_CLIENTS}")

mkdir -p "$RUN_DIR"
touch "$touch_marker"

stop_plain_cluster() {
    SERVER_COUNT=5 CLIENT_COUNT="${NUM_CLIENTS}" bash "$STOP_SCRIPT"
}

archive_latest_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}"
    local merged_dest_dir="${dest_dir}/merged"
    mkdir -p "$dest_dir" "$merged_dest_dir"

    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -newer "$touch_marker" -exec cp {} "$merged_dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$merged_dest_dir"/*.csv 2>/dev/null)" ]; then
            local newest
            newest=$(ls -t "$merged_dir"/*.csv 2>/dev/null | head -1)
            [ -n "$newest" ] && cp "$newest" "$merged_dest_dir/"
        fi
    fi

    local timeline_src="${SCRIPT_DIR}/eval"
    if [ -d "$timeline_src" ]; then
        find "$timeline_src" -name "tps_timeline_*.csv" -newer "$touch_marker" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi

    touch "$touch_marker"
    echo "  Archived results to: $dest_dir"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_plain_cluster || true
    fi
}
trap cleanup EXIT

run_case() {
    case "$1" in
        replica2) run_crash_case_sampled "case_replica2" "follower:2" ;;
        replica3) run_crash_case_sampled "case_replica3" "follower:3" ;;
        replica4) run_crash_case_sampled "case_replica4" "follower:4" ;;
        leader)   run_crash_case_sampled "case_leader" "leader" ;;
        *) echo "Usage: $0 [replica2|replica3|replica4|leader|all] [batchsize] [indep_ratio]"; exit 1 ;;
    esac
}

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  HETERO-5 CRASH EVAL (CORA/WOC)                                  ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Target: ${TARGET}  |  Batch: ${BATCHSIZE_OVERRIDE}  |  Indep ratio: ${INDEP_RATIO_OVERRIDE}"
echo "Result archive: $RUN_DIR"

if [ "$TARGET" = "all" ]; then
    run_case replica2
    run_case replica3
    run_case replica4
else
    run_case "$TARGET"
fi

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 crash eval complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
