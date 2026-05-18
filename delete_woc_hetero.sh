#!/bin/bash
# ================================================================
# Script: delete_woc.sh  (Heterogeneous cluster)
# IDs 0-10: cb16-60gb-560 (c16) | IDs 11-20: cb4-15gb-140 (c4)
# Run on jumper node
# Deletes ~/woc entirely on all VMs.
# Usage: ./delete_woc.sh [-n NUM_NODES] [-k SSH_KEY]
# ================================================================

set -euo pipefail

C16_VMS=(
    "192.168.73.159"
    "192.168.73.84"
    "192.168.73.218"
    "192.168.73.219"
    "192.168.73.25"
    "192.168.73.117"
    "192.168.73.16"
    "192.168.73.94"
    "192.168.73.173"
    "192.168.73.71"
    "192.168.73.42"
)

C4_VMS=(
    "192.168.73.69"
    "192.168.73.235"
    "192.168.73.194"
    "192.168.73.7"
    "192.168.73.27"
    "192.168.73.157"
    "192.168.73.78"
    "192.168.73.39"
    "192.168.73.77"
    "192.168.73.83"
)

ALL_VMS=("${C16_VMS[@]}" "${C4_VMS[@]}")

N=${#ALL_VMS[@]}
SSH_KEY="$HOME/.ssh/tani.pem"

while getopts "n:k:" opt; do
    case $opt in
        n) N=$OPTARG ;;
        k) SSH_KEY=$OPTARG ;;
        *) echo "Usage: $0 [-n NUM_NODES] [-k SSH_KEY]"; exit 1 ;;
    esac
done

VMS=("${ALL_VMS[@]:0:$N}")

if [ ! -f "$SSH_KEY" ]; then
    echo "ERROR: SSH key not found at $SSH_KEY"
    exit 1
fi

echo "Deleting ~/woc on $N nodes..."
echo "Using SSH key: $SSH_KEY"

for IP in "${VMS[@]}"; do
    echo "------------------------------------------------"
    echo ">> Connecting to $IP"

    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no ubuntu@"$IP" bash << 'EOF'
        set -e
        if [ -d "$HOME/woc" ]; then
            echo "Deleting $HOME/woc"
            rm -rf "$HOME/woc"
        else
            echo "$HOME/woc does not exist"
        fi
EOF

    echo ">> Done on $IP"
done

echo "================================================"
echo "All $N VMs cleaned."
