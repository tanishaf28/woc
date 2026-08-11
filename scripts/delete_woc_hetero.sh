#!/bin/bash
# ================================================================
# Script: delete_woc.sh  (Heterogeneous cluster)
# Run on jumper node
# Deletes ~/woc entirely on all VMs.
# Usage: ./delete_woc.sh [-n NUM_NODES] [-k SSH_KEY]
# ================================================================

set -euo pipefail

ALL_VMS=(
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
    "192.168.73.106"
    "192.168.73.224"
    "192.168.73.167"
    "192.168.73.137"
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
    "192.168.73.120"
    "192.168.73.9"
    "192.168.73.150"
    "192.168.73.124"
    "192.168.73.204"
    "192.168.73.59"
    "192.168.73.243"
    "192.168.73.192"
    "192.168.73.134"
    "192.168.73.132"
    "192.168.73.222"
    "192.168.73.250"
    "192.168.73.5"
    "192.168.73.237"
    "192.168.73.85"
    "192.168.73.65"
)

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
