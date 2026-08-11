#!/bin/bash
# ================================================================
# MongoDB Cluster Stopper - HETEROGENEOUS, SELECTABLE SIZE (WOC)
# Pairs with start_mongodb_hetero_nsel.sh - reads the same NUM_SERVERS/
# CONFIG_PATH selection so it targets the right nodes.
# ================================================================

set -euo pipefail

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
REMOTE_EVAL_DIR="${REMOTE_DIR}/eval"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
LOCAL_EVAL_DIR="${SCRIPT_DIR}/eval"
MERGED_DIR="${LOCAL_EVAL_DIR}/merged"
MERGE_SCRIPT="${REPO_ROOT}/merge_eval.py"
BINARY_NAME="woc"

NUM_SERVERS="${NUM_SERVERS:-5}"
case "$NUM_SERVERS" in
    3)  DEFAULT_CONFIG="cluster_hetero_3n_2s_1w.conf" ;;
    5)  DEFAULT_CONFIG="cluster_hetero_new.conf" ;;
    7)  DEFAULT_CONFIG="cluster_hetero_7n_3s_4w.conf" ;;
    11) DEFAULT_CONFIG="cluster_hetero_11n_4s_7w.conf" ;;
    *)  echo "ERROR: unsupported NUM_SERVERS=${NUM_SERVERS}. Supported: 3, 5, 7, 11" >&2; exit 1 ;;
esac
CONFIG_PATH="${CONFIG_PATH:-${REPO_ROOT}/config/${DEFAULT_CONFIG}}"

mapfile -t ALL_IPS < <(awk 'NF >= 2 { print $2 }' "$CONFIG_PATH")
if [ "${#ALL_IPS[@]}" -lt "$NUM_SERVERS" ]; then
    echo "ERROR: ${CONFIG_PATH} does not contain enough IPs for ${NUM_SERVERS} servers" >&2
    exit 1
fi
SERVER_IPS=("${ALL_IPS[@]:0:NUM_SERVERS}")
CLIENT_POOL_IPS=("${ALL_IPS[@]:NUM_SERVERS}")
DEFAULT_NUM_CLIENTS="${#CLIENT_POOL_IPS[@]}"
NUM_CLIENTS="${NUM_CLIENTS:-$DEFAULT_NUM_CLIENTS}"
CLIENT_HOST_IPS=()
for ((k = 0; k < NUM_CLIENTS; k++)); do
    CLIENT_HOST_IPS+=("${CLIENT_POOL_IPS[$((k % ${#CLIENT_POOL_IPS[@]}))]}")
done

CLIENT_START_ID="$NUM_SERVERS"

is_local_ip() {
    local ip="$1"
    hostname -I 2>/dev/null | tr ' ' '\n' | grep -Fxq "$ip"
}

copy_eval_dir() {
    local ip=$1
    local remote_subdir=$2
    local local_subdir=$3

    if is_local_ip "$ip"; then
        if [ ! -d "${REMOTE_EVAL_DIR}/${remote_subdir}" ]; then
            echo " WARNING: Missing local directory ${REMOTE_EVAL_DIR}/${remote_subdir} on ${ip}"
            return 1
        fi
        rm -rf "${LOCAL_EVAL_DIR:?}/${local_subdir}"
        cp -r "${REMOTE_EVAL_DIR}/${remote_subdir}" "${LOCAL_EVAL_DIR}/"
        return 0
    fi

    rm -rf "${LOCAL_EVAL_DIR:?}/${local_subdir}"
    scp -q -o ConnectTimeout=10 -o StrictHostKeyChecking=no -i "$SSH_KEY" -r \
        "$USER@$ip:${REMOTE_EVAL_DIR}/${remote_subdir}" "${LOCAL_EVAL_DIR}/" 2>/dev/null || {
            echo " WARNING: Failed to collect ${remote_subdir} from ${ip}"
            return 1
        }
    return 0
}

kill_on_node() {
    local ip=$1
    local type=$2
    local grace_seconds
    [ "$type" = "Client" ] && grace_seconds=45 || grace_seconds=60

    echo ""
    echo "-> Stopping ${type} on ${ip}"

    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
        "$USER@$ip" "pkill -TERM -x ${BINARY_NAME} 2>/dev/null; pkill -TERM -x mongod 2>/dev/null" || true

    local count=0 elapsed=0
    while [ "$elapsed" -lt "$grace_seconds" ]; do
        count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
            "$USER@$ip" "(pgrep -x ${BINARY_NAME} 2>/dev/null; pgrep -x mongod 2>/dev/null) | wc -l" || echo 0)
        count=$(echo "$count" | tr -d ' \n')
        [ "$count" -eq 0 ] && break
        sleep 1
        elapsed=$((elapsed + 1))
    done

    if [ "$count" -gt 0 ]; then
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
            "$USER@$ip" "pkill -9 -x ${BINARY_NAME} 2>/dev/null; pkill -9 -x mongod 2>/dev/null" || true
        sleep 1
    fi

    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
        "$USER@$ip" "rm -f '$REMOTE_DIR/mongodb_data/mongod.lock' '$REMOTE_DIR/mongodb_data/WiredTiger.lock' 2>/dev/null || true" || true
    echo "   ${type} on ${ip} stop attempted"
}

echo "=================================================="
echo " WOC MongoDB ${NUM_SERVERS}-node Cluster Shutdown"
echo "=================================================="

for ip in "${CLIENT_HOST_IPS[@]}"; do
    kill_on_node "$ip" "Client" &
done
wait

echo "Waiting 5 seconds for servers to flush metrics..."
sleep 5

for ip in "${SERVER_IPS[@]}"; do
    kill_on_node "$ip" "Server" &
done
wait
sleep 2

mkdir -p "${LOCAL_EVAL_DIR}" "${MERGED_DIR}"
for i in "${!CLIENT_HOST_IPS[@]}"; do
    client_id=$((CLIENT_START_ID + i))
    copy_eval_dir "${CLIENT_HOST_IPS[$i]}" "client${client_id}" "client${client_id}" || true
done
for i in "${!SERVER_IPS[@]}"; do
    copy_eval_dir "${SERVER_IPS[$i]}" "server${i}" "server${i}" || true
done

CLIENT_ID_FILTER="${CLIENT_START_ID}-$((CLIENT_START_ID + ${#CLIENT_HOST_IPS[@]} - 1))"
SERVER_ID_FILTER="0-$((NUM_SERVERS - 1))"

if [ -f "${MERGE_SCRIPT}" ]; then
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --ids "${CLIENT_ID_FILTER}" || echo " Error merging client CSVs"
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --servers --ids "${SERVER_ID_FILTER}" || echo " Error merging server CSVs"
else
    echo " merge_eval.py not found at ${MERGE_SCRIPT}"
fi

echo ""
echo "Cluster stopped and results collected. Merged dir: ${MERGED_DIR}"
