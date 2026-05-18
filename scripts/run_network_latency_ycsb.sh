#!/bin/bash
# Group4: Network Latency experiments for YCSB
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

SERVER_IPS=("192.168.73.159" "192.168.73.84" "192.168.73.69" "192.168.73.235" "192.168.73.194")
CLIENT_HOST_IPS=("192.168.73.218" "192.168.73.219")

RUNTIME=${RUNTIME:-30}

DELAYS=(0 5 10 25 50 100)

create_remote_dirs "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"
build_and_distribute
start_mongo_cluster "${SERVER_IPS[@]}"
init_replica_set "${SERVER_IPS[@]}"

for d in "${DELAYS[@]}"; do
    echo "Applying ${d}ms symmetric delay and running workload a"
    for ip in "${SERVER_IPS[@]}"; do
        remote_exec "$ip" "sudo tc qdisc del dev eth0 root 2>/dev/null || true; if [ $d -gt 0 ]; then sudo tc qdisc add dev eth0 root netem delay ${d}ms; fi"
    done

    start_workload_nodes 50.0 50.0 a
    sleep ${RUNTIME}
    stop_workload_nodes

    for ip in "${SERVER_IPS[@]}"; do remote_exec "$ip" "sudo tc qdisc del dev eth0 root 2>/dev/null || true"; done
done

echo "Asymmetric latency: apply delay only to weak (c4) nodes"
# Adjust the list below to match your weak node IPs
WEAK_NODES=("192.168.73.69" "192.168.73.235" "192.168.73.194")
for d in "${DELAYS[@]}"; do
    echo "Applying ${d}ms to weak nodes only"
    for ip in "${WEAK_NODES[@]}"; do
        remote_exec "$ip" "sudo tc qdisc del dev eth0 root 2>/dev/null || true; if [ $d -gt 0 ]; then sudo tc qdisc add dev eth0 root netem delay ${d}ms; fi"
    done

    start_workload_nodes 50.0 50.0 a
    sleep ${RUNTIME}
    stop_workload_nodes

    for ip in "${WEAK_NODES[@]}"; do remote_exec "$ip" "sudo tc qdisc del dev eth0 root 2>/dev/null || true"; done
done

echo "Network latency experiments complete."
