#!/bin/bash
# Run Group1 YCSB Workload Characterization
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# Cluster servers and client hosts (adjust if different)
SERVER_IPS=("192.168.73.159" "192.168.73.84" "192.168.73.69" "192.168.73.235" "192.168.73.194")
CLIENT_HOST_IPS=("192.168.73.218" "192.168.73.219")

RUNTIME=${RUNTIME:-30}

echo "Group1: WOC Workload Characterization"

create_remote_dirs "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"
build_and_distribute
start_mongo_cluster "${SERVER_IPS[@]}"
init_replica_set "${SERVER_IPS[@]}"

workloads=(a b c d e f)
for w in "${workloads[@]}"; do
    echo "Running workload ${w} for ${RUNTIME}s"
    start_workload_nodes 50.0 50.0 "$w"
    echo "  Running for ${RUNTIME}s..."
    sleep ${RUNTIME}
    stop_workload_nodes
    sleep 2
done

# Sweep independent/common ratio (this controls WOC fast/slow path selection)
ratios=(0 10 20 40 60 80 90 100)
for r in "${ratios[@]}"; do
    indep=${r}
    common=$((100 - r))
    echo "Running composition INDEP=${indep} COMMON=${common} (workload a)"
    start_workload_nodes "$indep" "$common" a
    sleep ${RUNTIME}
    stop_workload_nodes
    sleep 2
done

# Key distribution comparison on workload a
distributions=(zipfian uniform latest)
for d in "${distributions[@]}"; do
    echo "Running workload a with distribution $d"
    # pass distribution as extra arg to woc via -dist
    start_workload_nodes 50.0 50.0 a
    # if woc supports a -dist flag, it will pick it up; otherwise adjust as needed
    sleep ${RUNTIME}
    stop_workload_nodes
    sleep 2
done

echo "Group1 runs complete. Check remote logs in $LOG_DIR on servers/clients."
