#!/bin/bash
# Group2: Heterogeneous Cluster Composition experiments
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# Example server IPs; adjust as needed
ALL_SERVERS=("192.168.73.159" "192.168.73.84" "192.168.73.69" "192.168.73.235" "192.168.73.194" "192.168.73.218" "192.168.73.219")
CLIENT_HOST_IPS=("192.168.73.218" "192.168.73.219")

RUNTIME=${RUNTIME:-30}

# Strong/Weak configurations to test (counts of strong nodes)
strong_counts=(1 3 5 7)

for s in "${strong_counts[@]}"; do
    echo "Testing composition with ${s} strong nodes (total N=7)"
    # Assumes user updated config files up-front for this composition; proceeding without interactive pause.
    # Map ALL_SERVERS into SERVER_IPS for shared helpers
    SERVER_IPS=("${ALL_SERVERS[@]}")
    create_remote_dirs "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"
    build_and_distribute
    start_mongo_cluster "${SERVER_IPS[@]}"
    init_replica_set "${SERVER_IPS[@]}"

    # Run a representative workload (workload a) for a short probe
    start_workload_nodes 50.0 50.0 a
    sleep 30
    stop_workload_nodes
    echo "Composition ${s} complete. Collect logs and proceed to next." 
done

# Leader placement: user must change leader manually (or via config) then run tests
echo "Leader placement tests: place leader on c16 then c4 and run the same YCSB workload."
