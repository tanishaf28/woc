#!/bin/bash
# ================================================================
# WOC Cloud Cluster Stopper - HETEROGENEOUS CLUSTER
# ================================================================

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
REMOTE_EVAL_DIR="${REMOTE_DIR}/eval"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_EVAL_DIR="${SCRIPT_DIR}/eval"
MERGED_DIR="${LOCAL_EVAL_DIR}/merged"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"
RUN_TS="$(date +%Y%m%d_%H%M%S)"

ALL_SERVER_IPS=(
"192.168.73.59"
"192.168.73.243"
"192.168.73.192"
"192.168.73.134"
"192.168.73.132"
)

ALL_CLIENT_IPS=(
"192.168.73.218"
"192.168.73.219"
)

# Heterogeneous cluster configuration
SERVER_COUNT="${SERVER_COUNT:-5}"
CLIENT_COUNT="${CLIENT_COUNT:-2}"

if [ "$SERVER_COUNT" -lt 1 ] || [ "$SERVER_COUNT" -gt "${#ALL_SERVER_IPS[@]}" ]; then
    echo "ERROR: SERVER_COUNT must be between 1 and ${#ALL_SERVER_IPS[@]}"
    exit 1
fi

if [ "$CLIENT_COUNT" -lt 0 ] || [ "$CLIENT_COUNT" -gt 100 ]; then
    echo "ERROR: CLIENT_COUNT must be between 0 and 100 (got $CLIENT_COUNT)"
    exit 1
fi

SERVER_IPS=("${ALL_SERVER_IPS[@]:0:$SERVER_COUNT}")
CLIENT_IPS=("${ALL_CLIENT_IPS[@]}")

SERVER_ID_FILTER="0-$((${#SERVER_IPS[@]} - 1))"
CLIENT_START_ID="${#SERVER_IPS[@]}"
CLIENT_END_ID="$((CLIENT_START_ID + CLIENT_COUNT - 1))"
CLIENT_ID_FILTER="${CLIENT_START_ID}-${CLIENT_END_ID}"
BINARY_NAME="woc"

is_local_ip() {
    local ip="$1"
    hostname -I 2>/dev/null | tr ' ' '\n' | grep -Fxq "$ip"
}

# ---------------------------------------------------------------
# FUNCTION: Copy eval directory from remote node to local eval/
# ---------------------------------------------------------------
copy_eval_dir() {
    local ip=$1
    local remote_subdir=$2
    local local_subdir=$3

    if is_local_ip "$ip"; then
        echo " Detected local source for ${local_subdir} (${ip}), skipping SCP refresh"
        mkdir -p "${LOCAL_EVAL_DIR}/${local_subdir}"
        return 0
    fi

    # Refresh local copy to avoid stale files from previous runs.
    rm -rf "${LOCAL_EVAL_DIR:?}/${local_subdir}"

    # Copy full directory so structure stays consistent.
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "test -d ${REMOTE_EVAL_DIR}/${remote_subdir}" >/dev/null 2>&1 || {
            echo " WARNING: Missing remote directory ${REMOTE_EVAL_DIR}/${remote_subdir} on ${ip}"
            return 1
        }

    echo " Collecting ${USER}@${ip}:${REMOTE_EVAL_DIR}/${remote_subdir} -> ${LOCAL_EVAL_DIR}/"
    scp -q -o ConnectTimeout=10 -o StrictHostKeyChecking=no -i $SSH_KEY -r \
        "$USER@$ip:${REMOTE_EVAL_DIR}/${remote_subdir}" "${LOCAL_EVAL_DIR}/" 2>/dev/null || {
            echo " WARNING: Failed to collect ${remote_subdir} from ${ip}"
            return 1
        }

    return 0
}

echo "=================================================="
echo " WOC HETEROGENEOUS Cluster Shutdown"
echo " Clients → Servers"
echo "=================================================="

# ---------------------------------------------------------------
# FUNCTION: Kill processes on a node
# ---------------------------------------------------------------
kill_on_node() {
    local ip=$1
    local type=$2   # "Client" or "Server"
    local grace_seconds

    if [ "$type" = "Client" ]; then
        grace_seconds=45
    else
        grace_seconds=60
    fi

    echo ""
    echo "→ Stopping ${type} on ${ip}"

    # Send SIGTERM first (exact binary match avoids false positives).
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pkill -TERM -x ${BINARY_NAME} 2>/dev/null" || true

    echo "  Waiting up to ${grace_seconds}s for graceful shutdown..."
    local count=0
    local elapsed=0
    while [ "$elapsed" -lt "$grace_seconds" ]; do
        count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
        count=$(echo "$count" | tr -d ' \n')
        if [ "$count" -eq 0 ]; then
            break
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    # Check if still running
    if [ "$count" -gt 0 ]; then
        echo "  Still running -> Killing $count process(es) on $ip"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
            $USER@$ip "pkill -9 -x ${BINARY_NAME} 2>/dev/null" || true
        sleep 1
    fi

    # Final check
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
    count=$(echo "$count" | tr -d ' \n')

    if [ "$count" -eq 0 ]; then
        echo "   ${type} on ${ip} stopped"
    else
        echo "  WARNING: $count process(es) still active on ${ip}"
    fi
}

# ---------------------------------------------------------------
# STEP 1 — STOP CLIENTS (IN PARALLEL)
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 1: Stopping Clients (${CLIENT_COUNT} total)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for ip in "${CLIENT_IPS[@]}"; do
    kill_on_node "$ip" "Client" &
done

# Wait for all parallel client shutdowns to complete
wait

echo ""
echo "Waiting 5 seconds for servers to flush metrics..."
sleep 5

# ---------------------------------------------------------------
# STEP 2 — STOP SERVERS
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 2: Stopping Servers (${#SERVER_IPS[@]} nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for ip in "${SERVER_IPS[@]}"; do
    kill_on_node "$ip" "Server" &
done
wait

sleep 2

# ---------------------------------------------------------------
# FINAL VERIFICATION
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Verification (checking if ANY woc processes remain)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

any_left=false

for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
    count=$(echo "$count" | tr -d ' \n')
    if [ "$count" -gt 0 ]; then
        echo " ${ip}: ${count} processes STILL running"
        any_left=true
    fi
done

if [ "$any_left" = false ]; then
    echo " All WOC processes stopped on all nodes."
else
    echo " Some processes remain. Use manual pkill if needed."
fi

# ---------------------------------------------------------------
# STEP 2.5 — CLEAN UP STALE EVAL DIRECTORIES
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 2.5: Cleaning up stale eval directories"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

mkdir -p "${LOCAL_EVAL_DIR}" "${MERGED_DIR}"

for server_dir in "${LOCAL_EVAL_DIR}"/server*/; do
    [ -d "$server_dir" ] || continue
    server_name=$(basename "$server_dir")
    server_id="${server_name#server}"
    if [ "$server_id" -ge "${#SERVER_IPS[@]}" ]; then
        echo " Removing stale ${server_name}"
        rm -rf "$server_dir"
    fi
done

for client_dir in "${LOCAL_EVAL_DIR}"/client*/; do
    [ -d "$client_dir" ] || continue
    client_name=$(basename "$client_dir")
    client_id="${client_name#client}"
    if [ "$client_id" -lt "${#SERVER_IPS[@]}" ] || [ "$client_id" -ge "$((CLIENT_START_ID + CLIENT_COUNT))" ]; then
        echo " Removing stale ${client_name}"
        rm -rf "$client_dir"
    fi
done

# ---------------------------------------------------------------
# STEP 3 — COLLECT EVAL DIRECTORIES FROM REMOTE NODES
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 3: Collecting client eval directories"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for client_id in $(seq $CLIENT_START_ID $CLIENT_END_ID); do
    vm_index=$((client_id - CLIENT_START_ID))
    copy_eval_dir "${CLIENT_IPS[$vm_index]}" "client${client_id}" "client${client_id}" || true
done

echo ""
echo "Client CSV collection check:"
for client_id in $(seq $CLIENT_START_ID $CLIENT_END_ID); do
    if ls "${LOCAL_EVAL_DIR}/client${client_id}"/*.csv >/dev/null 2>&1; then
        echo " client${client_id}: CSV found"
    else
        echo " WARNING: client${client_id}: no CSV found"
    fi
done

# ---------------------------------------------------------------
# STEP 4 — COLLECT SERVER EVAL DIRECTORIES
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 4: Collecting server eval directories"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for i in "${!SERVER_IPS[@]}"; do
    copy_eval_dir "${SERVER_IPS[$i]}" "server${i}" "server${i}" || true
done

echo ""
echo "Server CSV collection check:"
for i in "${!SERVER_IPS[@]}"; do
    if ls "${LOCAL_EVAL_DIR}/server${i}"/*.csv >/dev/null 2>&1; then
        echo " server${i}: CSV found"
    else
        echo " WARNING: server${i}: no CSV found"
    fi
done

# ---------------------------------------------------------------
# STEP 5 — MERGE CLIENT CSVs
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 5: Merging client CSVs locally"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -f "${MERGE_SCRIPT}" ]; then
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --ids "${CLIENT_ID_FILTER}"
    if [ $? -eq 0 ]; then
        echo " ✓ Merged client CSV written to: ${MERGED_DIR}/merged_woc_clients_*.csv"
    else
        echo " ✗ Error merging client CSVs"
    fi
else
    echo " ✗ merge_eval.py not found at ${MERGE_SCRIPT}"
fi

# ---------------------------------------------------------------
# STEP 6 — MERGE SERVER CSVs
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 6: Merging server CSVs locally"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -f "${MERGE_SCRIPT}" ]; then
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --servers --ids "${SERVER_ID_FILTER}"
    if [ $? -eq 0 ]; then
        echo " ✓ Merged server CSV written to: ${MERGED_DIR}/merged_woc_servers_*.csv"
    else
        echo " ✗ Error merging server CSVs"
    fi
else
    echo " ✗ merge_eval.py not found at ${MERGE_SCRIPT}"
fi

echo ""
echo "=================================================="
echo " HETEROGENEOUS CLUSTER SHUTDOWN COMPLETE"
echo "=================================================="
