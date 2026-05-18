#!/bin/bash
# Master runner for all YCSB evaluation groups
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RUN1=${1:-1}
RUN2=${2:-1}
RUN3=${3:-1}
RUN4=${4:-1}
RUN5=${5:-1}

if [ "$RUN1" -eq 1 ]; then
    bash "$SCRIPT_DIR/run_ycsb_group1.sh"
fi

if [ "$RUN2" -eq 1 ]; then
    bash "$SCRIPT_DIR/run_hetero_composition.sh"
fi

if [ "$RUN3" -eq 1 ]; then
    bash "$SCRIPT_DIR/run_fault_tolerance_ycsb.sh"
fi

if [ "$RUN4" -eq 1 ]; then
    bash "$SCRIPT_DIR/run_network_latency_ycsb.sh"
fi

if [ "$RUN5" -eq 1 ]; then
    bash "$SCRIPT_DIR/run_throughput_scaling.sh"
fi

echo "All requested YCSB evals launched (check remote logs)."
