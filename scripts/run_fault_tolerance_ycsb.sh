#!/bin/bash
# Group3: Fault Tolerance experiments
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

SERVER_IPS=("192.168.73.159" "192.168.73.84" "192.168.73.69" "192.168.73.235" "192.168.73.194")
CLIENT_HOST_IPS=("192.168.73.218" "192.168.73.219")

RUNTIME=${RUNTIME:-30}

create_remote_dirs "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"
build_and_distribute
start_mongo_cluster "${SERVER_IPS[@]}"
init_replica_set "${SERVER_IPS[@]}"

echo "Baseline run"
start_workload_nodes 50.0 50.0 a
sleep ${RUNTIME}
stop_workload_nodes

echo "Kill one strong WOC server (server index 0)"
remote_exec "${SERVER_IPS[0]}" "pkill -SIGTERM -f '$BINARY' || true"
sleep 5

echo "Restart WOC on strong node"
remote_exec "${SERVER_IPS[0]}" "nohup '$REMOTE_DIR/$BINARY' -id=0 -conf='$CONFIG_PATH' -role=0 -et=1 -mongodb -workload=a > '$LOG_DIR/server_0_restart_$(date +%s).log' 2>&1 &" || true
sleep 5

echo "Injecting slow node (simulate with tc delay on a weak node)"
remote_exec "${SERVER_IPS[4]}" "sudo tc qdisc add dev eth0 root netem delay 100ms" || true
start_workload_nodes 50.0 50.0 a
sleep ${RUNTIME}
stop_workload_nodes
remote_exec "${SERVER_IPS[4]}" "sudo tc qdisc del dev eth0 root || true"

echo "Fault tolerance tests complete."
