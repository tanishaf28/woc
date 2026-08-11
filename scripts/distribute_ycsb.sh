#!/bin/bash
# ================================================================
# Distribute YCSB workload data files to all WOC nodes.
# Run this ONCE before starting the MongoDB cluster.
#
# Prerequisites:
#   1. Run genData.sh locally first to generate the .dat files
#   2. The .dat files should be at ./ycsb/workData/ locally
#
# Usage:
#   bash distribute_ycsb_data.sh
# ================================================================

set -euo pipefail

SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/woc"

# Server nodes need the data files for initMongoDB.
SERVER_IPS=(
    "192.168.73.59"
    "192.168.73.243"
    "192.168.73.117"
    "192.168.73.16"
    "192.168.73.94"
)

# Client nodes also need the run_workload*.dat files because RunClient reads
# them directly at runtime.
CLIENT_IPS=(
    "192.168.73.159"
    "192.168.73.84"
    "192.168.73.218"
    "192.168.73.219"
    "192.168.73.25"
)

LOCAL_DATA_DIR="./ycsb/workData"
LEGACY_DATA_DIR="./ycsb/scripts/workData"
REMOTE_DATA_DIR="${REMOTE_DIR}/ycsb/workData"

SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no)

if [ -f "${LOCAL_DATA_DIR}/workload.dat" ]; then
    SOURCE_DATA_DIR="${LOCAL_DATA_DIR}"
elif [ -f "${LEGACY_DATA_DIR}/workload.dat" ]; then
    SOURCE_DATA_DIR="${LEGACY_DATA_DIR}"
else
    echo "ERROR: no workload.dat found."
    echo "Expected either ${LOCAL_DATA_DIR}/workload.dat or ${LEGACY_DATA_DIR}/workload.dat"
    echo "Run genData.sh first to generate workload files."
    exit 1
fi

LOCAL_DATA_DIR="${SOURCE_DATA_DIR}"

echo "Checking for run workload files..."
for w in a b c d e f; do
    if [ -f "${LOCAL_DATA_DIR}/run_workload${w}.dat" ]; then
        size=$(du -sh "${LOCAL_DATA_DIR}/run_workload${w}.dat" | cut -f1)
        echo "  ✓ run_workload${w}.dat  ($size)"
    else
        echo "  ✗ run_workload${w}.dat  MISSING"
    fi
done
echo ""

TARGET_IPS=("${SERVER_IPS[@]}" "${CLIENT_IPS[@]}")

echo "Distributing to ${#TARGET_IPS[@]} WOC nodes..."
for ip in "${TARGET_IPS[@]}"; do
    (
        echo "  → $ip: creating remote dir..."
        ssh "${SSH_OPTS[@]}" "$USER@$ip" "mkdir -p '$REMOTE_DATA_DIR'"

        echo "  → $ip: copying data files..."
        scp "${SSH_OPTS[@]}" -r "$LOCAL_DATA_DIR"/*.dat "$USER@$ip:$REMOTE_DATA_DIR/"

        echo "  ✓ $ip done"
    ) &
done
wait

echo ""
echo "Verifying on all nodes..."
for ip in "${TARGET_IPS[@]}"; do
    count=$(ssh "${SSH_OPTS[@]}" "$USER@$ip" \
        "ls '$REMOTE_DATA_DIR'/*.dat 2>/dev/null | wc -l" || echo 0)
    echo "  $ip: $count .dat files present"
done

echo ""
echo "✓ Distribution complete."
echo ""
