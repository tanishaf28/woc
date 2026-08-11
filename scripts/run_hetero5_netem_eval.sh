#!/bin/bash
# ================================================================
# HETERO-5 NETEM EVAL: 10ms ± 5ms jitter, applied to server EGRESS only
#
# Single fixed-size (5 server + 2 client) heterogeneous cluster run with a
# light delay applied via `tc qdisc ... netem` on each SERVER's own
# default-route interface. Unlike the old D1/D2/D3/D4 sweep (deleted along
# with run_hetero_plainmsg_evals.sh), this only ever applies one delay
# profile, and no `tc` rule is ever installed on a client machine's
# interface. NOTE: this is not the same as "client-observed latency is
# unaffected" - a server's egress interface carries ALL of its outbound
# traffic, including RPC replies going back to clients and server-to-server
# traffic (fast-path broadcasts, forwards, slow-path rounds), so the delay
# shows up in client-measured round-trip latency too. Treat this as "delay
# injected into the server side of the network path," not as an
# isolated/one-directional delay.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_netem_eval"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

DELAY_MS="${DELAY_MS:-10}"
JITTER_MS="${JITTER_MS:-5}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
CLUSTER_ACTIVE=false

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=2"
    "THRESHOLD=1"
    "BATCHSIZE=1"
    "INDEP_RATIO=90.0"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "LOG_LEVEL=info"
    "ENABLE_PRIORITY=true"
)

SERVER_IPS=(
    "192.168.73.59"
    "192.168.73.243"
    "192.168.73.192"
    "192.168.73.134"
    "192.168.73.132"
)

CLIENT_IPS=(
    "192.168.73.167"
    "192.168.73.137"
)

mkdir -p "$RUN_DIR"

remote_exec() {
    local host=$1
    shift
    ssh -i "$SSH_KEY" "$USER@$host" "$*"
}

detect_interface() {
    local host=$1
    remote_exec "$host" "ip route show default 2>/dev/null | awk '{print \$5; exit}'"
}

# apply_server_only_delay: installs the tc qdisc rule on SERVER_IPS only -
# no CLIENT_IPS interface is ever touched. This delays everything egressing
# a server's own interface, though, which includes replies to clients and
# server-to-server traffic - see the file header note above before reading
# client-side latency numbers as isolated from this delay.
apply_server_only_delay() {
    local delay_ms=$1
    local jitter_ms=$2
    echo "  [netem] Applying ${delay_ms}ms ±${jitter_ms}ms to server links only..."
    for ip in "${SERVER_IPS[@]}"; do
        local iface
        iface=$(detect_interface "$ip")
        [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
        remote_exec "$ip" \
            "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
             sudo tc qdisc add dev '$iface' root netem delay ${delay_ms}ms ${jitter_ms}ms distribution normal" \
            || true
    done
    sleep 1
}

remove_server_delay() {
    echo "  [netem] Removing server-side delay..."
    for ip in "${SERVER_IPS[@]}"; do
        local iface
        iface=$(detect_interface "$ip")
        [ -z "$iface" ] && continue
        remote_exec "$ip" "sudo tc qdisc del dev '$iface' root 2>/dev/null || true" || true
    done
    sleep 1
}

archive_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}/merged"
    mkdir -p "$dest_dir"
    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -newer "${RUN_DIR}" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$dest_dir"/*.csv 2>/dev/null)" ]; then
            local newest
            newest=$(ls -t "$merged_dir"/*.csv 2>/dev/null | head -1)
            [ -n "$newest" ] && cp "$newest" "$dest_dir/"
        fi
    fi
    echo "  Archived results to: $dest_dir"
}

cleanup() {
    remove_server_delay || true
    if [ "$CLUSTER_ACTIVE" = true ]; then
        bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   HETERO-5 NETEM EVAL: ${DELAY_MS}ms ±${JITTER_MS}ms (server-side only)        ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"

apply_server_only_delay "$DELAY_MS" "$JITTER_MS"

CLUSTER_ACTIVE=true
env "${BASE_ENV[@]}" bash "$START_SCRIPT"

echo "Running for ${RUNTIME_SECONDS}s..."
sleep "$RUNTIME_SECONDS"

remove_server_delay
bash "$STOP_SCRIPT"
CLUSTER_ACTIVE=false

archive_result "netem_${DELAY_MS}ms_${JITTER_MS}ms_server_only"

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 netem eval complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
