#!/bin/bash
# ================================================================
# MASTER EVALUATION RUNNER
# Runs all 4 evaluation scripts sequentially
# ================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║           COMPREHENSIVE MONGODB WORKLOAD A EVALUATION           ║"
echo "║                    5-Node Heterogeneous Cluster                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

RUN_EVAL1="${1:-1}"
RUN_EVAL2="${2:-1}"
RUN_EVAL3="${3:-1}"
RUN_EVAL4="${4:-1}"
EXIT_STATUS=0

if [ "${1:-}" = "--help" ]; then
    echo "Usage: bash run_all_evals.sh [eval1] [eval2] [eval3] [eval4]"
    echo ""
    echo "Parameters (1=run, 0=skip):"
    echo "  eval1: Independent vs Common Ratio (default: 1)"
    echo "  eval2: Max Pipeline In-Flight (default: 1)"
    echo "  eval3: Fault Tolerance (default: 1)"
    echo "  eval4: Network Delay (default: 1)"
    echo ""
    echo "Example: bash run_all_evals.sh 1 1 0 0  # Run only eval1 and eval2"
    exit 0
fi

START_TIME=$(date +%s)

run_eval_script() {
    local label=$1
    local script=$2

    if ! bash "$script"; then
        echo ""
        echo "Warning: $label failed. Continuing to the remaining steps."
        EXIT_STATUS=1
    fi
}

# EVAL 1: Independent vs Common Ratio
if [ "$RUN_EVAL1" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 1: Independent vs Common Ratio"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 1" "eval_1_indep_common_ratio.sh"
fi

# EVAL 2: Max Pipeline In-Flight
if [ "$RUN_EVAL2" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 2: Max Pipeline In-Flight"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 2" "eval_2_max_inflight.sh"
fi

# EVAL 3: Fault Tolerance
if [ "$RUN_EVAL3" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 3: Fault Tolerance"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 3" "eval_3_fault_tolerance.sh"
fi

# EVAL 4: Network Delay
if [ "$RUN_EVAL4" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 4: Network Delay Impact"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 4" "eval_4_network_delay.sh"
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
echo "Collecting and organizing results..."
echo ""

# Run result collection script
if [ -f "collect_eval_results.sh" ]; then
    if ! bash collect_eval_results.sh; then
        EXIT_STATUS=1
    fi
else
    echo "Note: collect_eval_results.sh not found. Manual collection:"
    echo "  ssh -i /path/to/tani.pem ubuntu@192.168.73.159 'ls -lah /home/ubuntu/woc/eval/'"
fi

exit $EXIT_STATUS
