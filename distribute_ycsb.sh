#!/bin/bash
# ================================================================
# Distribute YCSB workload data files to all server nodes.
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

# All nodes that run WOC servers (need the data files for initMongoDB)
SERVER_IPS=(
    "192.168.73.159"
    "192.168.73.84"
    "192.168.73.69"
    "192.168.73.235"
    "192.168.73.194"
)

LOCAL_DATA_DIR="./ycsb/workData"
REMOTE_DATA_DIR="${REMOTE_DIR}/ycsb/workData"

SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no)

# Verify local data files exist
if [ ! -f "${LOCAL_DATA_DIR}/workload.dat" ]; then
    echo "ERROR: ${LOCAL_DATA_DIR}/workload.dat not found."
    echo "Run genData.sh first to generate workload files."
    exit 1
fi

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

echo "Distributing to ${#SERVER_IPS[@]} server nodes..."
for ip in "${SERVER_IPS[@]}"; do
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
for ip in "${SERVER_IPS[@]}"; do
    count=$(ssh "${SSH_OPTS[@]}" "$USER@$ip" \
        "ls '$REMOTE_DATA_DIR'/*.dat 2>/dev/null | wc -l" || echo 0)
    echo "  $ip: $count .dat files present"
done

echo ""
echo "✓ Distribution complete."
echo ""
echo "IMPORTANT: Only server 0 (192.168.73.159) should load data into MongoDB."
echo "The replica set will replicate it automatically to servers 1-4."
echo "The race condition in initMongoDB (all servers loading simultaneously)"
echo "is harmless but wastes time — MongoDB replica replication handles it."