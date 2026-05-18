#!/bin/bash
# Group5: Throughput scalability and batch-size experiments
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

SERVER_IPS=("192.168.73.159" "192.168.73.84" "192.168.73.69" "192.168.73.235" "192.168.73.194")
CLIENT_HOST_IPS=("192.168.73.218" "192.168.73.219")

RUNTIME=${RUNTIME:-30}

client_counts=(1 2 4 8 16 32)
batch_sizes=(1 5 10 20 50)

create_remote_dirs "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"
build_and_distribute
start_mongo_cluster "${SERVER_IPS[@]}"
init_replica_set "${SERVER_IPS[@]}"

echo "Starting servers for scaling tests"
start_workload_nodes 50.0 50.0 a

# Kill the default clients started by start_workload_nodes so we can launch controlled numbers
for ip in "${CLIENT_HOST_IPS[@]}"; do
    remote_exec "$ip" "pkill -SIGTERM -f '$BINARY' 2>/dev/null || true"
done

echo "Client scaling test"
num_hosts=${#CLIENT_HOST_IPS[@]}
for c in "${client_counts[@]}"; do
    echo "Running with $c clients"
    launched=0
    per_host=$(( (c + num_hosts - 1) / num_hosts ))
    for host in "${CLIENT_HOST_IPS[@]}"; do
        for i in $(seq 1 $per_host); do
            if [ "$launched" -ge "$c" ]; then break 2; fi
            run_woc_client "$host" "-workload=a -threadcount=16 -maxexecutiontime=30"
            launched=$((launched + 1))
        done
    done
    echo " Launched $launched clients"
    sleep ${RUNTIME}
    # stop the launched clients
    for ip in "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -SIGTERM -f '$BINARY' 2>/dev/null || true"
    done
    sleep 5
done

echo "Batch size tests"
for b in "${batch_sizes[@]}"; do
    echo "Running batch size $b"
    launched=0
    for host in "${CLIENT_HOST_IPS[@]}"; do
        run_woc_client "$host" "-workload=a -batch_size=${b} -maxexecutiontime=30"
        launched=$((launched + 1))
    done
    echo " Launched $launched batch-size clients"
    sleep ${RUNTIME}
    for ip in "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -SIGTERM -f '$BINARY' 2>/dev/null || true"
    done
    sleep 5
done

echo "Throughput scaling experiments complete."
