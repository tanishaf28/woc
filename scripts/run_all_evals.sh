#!/bin/bash
# ================================================================
# MASTER MONGODB EVALUATION RUNNER
# Runs eval1 (independent ratio) + eval2 (batch size) sequentially on
# workload 'a', or supports a one-shot --workload passthrough mode.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║           COMPREHENSIVE MONGODB WORKLOAD A EVALUATION           ║"
echo "║                    5-Node Heterogeneous Cluster                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

EXIT_STATUS=0

START_SCRIPT="start_mongodb_hetero.sh"
STOP_SCRIPT="stop_mongodb_hetero.sh"
EVAL1_SCRIPT="eval_1_indep_ratio.sh"
EVAL2_SCRIPT="eval_2_batching.sh"

if [ "${1:-}" = "--help" ]; then
    cat <<'EOF'
Usage:
    bash run_all_evals.sh [eval1] [eval2]

        bash run_all_evals.sh --workload [a-f] [runtime_seconds]

Eval toggles (1=run, 0=skip):
  eval1: Independent Ratio sweep (default: 1)
  eval2: Batch Size sweep (default: 1)

Workload is always 'a' for eval1/eval2 (fixed).

Examples:
  bash run_all_evals.sh 1 0
  bash run_all_evals.sh --workload a 300

Env overrides:
  RUNTIME_SECONDS=0      (for --workload mode; 0 means leave running)
EOF
    exit 0
fi

if [ "${1:-}" = "--workload" ]; then
    WORKLOAD="${2:-a}"
    RUNTIME_SECONDS="${3:-0}"

    if [[ ! "$WORKLOAD" =~ ^[a-f]$ ]]; then
        echo "ERROR: workload must be one of: a b c d e f"
        exit 1
    fi

    if [ ! -f "$START_SCRIPT" ]; then
        echo "ERROR: missing $START_SCRIPT"
        exit 1
    fi

    bash "./$START_SCRIPT" "$WORKLOAD"

    if [ "$RUNTIME_SECONDS" -gt 0 ]; then
        echo "Running workload '$WORKLOAD' for ${RUNTIME_SECONDS}s..."
        sleep "$RUNTIME_SECONDS"
        if [ -f "$STOP_SCRIPT" ]; then
            echo "Stopping cluster after timed run..."
            bash "./$STOP_SCRIPT" || EXIT_STATUS=1
        else
            echo "Warning: stop script $STOP_SCRIPT not found"
            EXIT_STATUS=1
        fi
    else
        echo "Cluster is running workload '$WORKLOAD'. Stop manually with: bash ./$STOP_SCRIPT"
    fi

    exit $EXIT_STATUS
fi

RUN_EVAL1="${1:-1}"
RUN_EVAL2="${2:-1}"

for required in "$EVAL1_SCRIPT" "$EVAL2_SCRIPT"; do
    if [ ! -f "$required" ]; then
        echo "ERROR: required fixed script not found: $required"
        exit 1
    fi
done

RUN_TS="$(date +%Y%m%d_%H%M%S)"
COMBINED_DIR="${SCRIPT_DIR}/results/run_all_evals/${RUN_TS}"
mkdir -p "$COMBINED_DIR"
COMBINED_METRICS="${COMBINED_DIR}/extracted_metrics.csv"
COMBINED_METRICS_SERVERS="${COMBINED_DIR}/extracted_metrics_servers.csv"

START_TIME=$(date +%s)

# Append <latest_run_dir>/<metrics_filename> into <combined_path>, skipping
# the header after the first file.
append_metrics() {
    local label=$1
    local latest_run_dir=$2
    local metrics_filename=$3
    local combined_path=$4

    local latest_metrics="${latest_run_dir}${metrics_filename}"
    if [ -f "$latest_metrics" ]; then
        if [ ! -s "$combined_path" ]; then
            cp "$latest_metrics" "$combined_path"
        else
            tail -n +2 "$latest_metrics" >> "$combined_path"
        fi
    else
        echo "Warning: no ${metrics_filename} found for $label at $latest_metrics"
    fi
}

run_eval_script() {
    local label=$1
    local script=$2
    local results_subdir=$3

    if ! bash "$script"; then
        echo ""
        echo "Warning: $label failed. Continuing to the remaining steps."
        EXIT_STATUS=1
        return
    fi

    local latest_run_dir
    latest_run_dir=$(ls -td "${SCRIPT_DIR}/results/${results_subdir}"/*/ 2>/dev/null | head -1)
    append_metrics "$label" "$latest_run_dir" "extracted_metrics.csv" "$COMBINED_METRICS"
    append_metrics "$label" "$latest_run_dir" "extracted_metrics_servers.csv" "$COMBINED_METRICS_SERVERS"
}

# EVAL 1: Independent Ratio
if [ "$RUN_EVAL1" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 1: Independent Ratio"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 1" "$EVAL1_SCRIPT" "eval1_indep_ratio"
fi

# EVAL 2: Batch Size
if [ "$RUN_EVAL2" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 2: Batch Size"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 2" "$EVAL2_SCRIPT" "eval2_batching"
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                  ALL EVALUATIONS COMPLETE                       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Total execution time: $((DURATION / 3600))h $((DURATION % 3600 / 60))m $((DURATION % 60))s"
echo ""
echo "Combined client metrics: $COMBINED_METRICS"
echo "Combined server metrics: $COMBINED_METRICS_SERVERS"

exit $EXIT_STATUS
