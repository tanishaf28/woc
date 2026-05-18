#!/bin/bash
# One-command runner for MongoDB workloads on heterogeneous cluster.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

WORKLOAD="${1:-a}"
RUNTIME_SECONDS="${2:-0}"

if [[ ! "$WORKLOAD" =~ ^[a-f]$ ]]; then
    echo "ERROR: workload must be one of: a b c d e f"
    exit 1
fi

bash ./start_mongodb_hetero.sh "$WORKLOAD"

if [ "$RUNTIME_SECONDS" -gt 0 ]; then
    echo "Running workload '${WORKLOAD}' for ${RUNTIME_SECONDS}s..."
    sleep "$RUNTIME_SECONDS"
    echo "Stopping cluster after timed run..."
    bash ./stop_mongodb_hetero.sh
else
    echo "Cluster is running workload '${WORKLOAD}'. Stop manually with: ./stop_mongodb_hetero.sh"
fi
