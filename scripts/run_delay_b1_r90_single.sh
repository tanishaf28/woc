#!/bin/bash
# ================================================================
# HETERO-5 NETEM SINGLE DELAY RUN + TIMELINE (WOC): one steady-state
# run at INDEP_RATIO=90, BATCHSIZE=1, under a fixed server-side-only
# netem delay profile (DELAY_MS +-JITTER_MS), producing a tps_timeline
# CSV (ENABLE_TIMESERIES=true). No ratio sweep -- single case only.
#
# Ported from epaxos's run_delay_b1_r90_single.sh. SERVER_IPS is derived
# from CONFIG_PATH at runtime rather than hardcoded -- see
# run_hetero5_ratio_delay_5c_b100.sh's header for why.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
CONFIG_PATH="${REPO_ROOT}/config/cluster_hetero_5n_10c.conf"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_delay_b1_r90_single"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

DELAY_MS="${DELAY_MS:-5}"
JITTER_MS="${JITTER_MS:-5}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-60}"
DELAY_APPLIED=false
CLUSTER_ACTIVE=false

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=5"
    "CONFIG_PATH=${CONFIG_PATH}"
    "THRESHOLD=2"
    "BATCHSIZE=1"
    "INDEP_RATIO=90.0"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "ENABLE_TIMESERIES=true"
    "LOG_LEVEL=info"
)

mapfile -t SERVER_IPS < <(awk 'NF >= 2 {print $2}' "$CONFIG_PATH" | head -5)

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

apply_server_only_delay() {
    local delay_ms=$1
    local jitter_ms=$2
    echo "  [netem] Applying ${delay_ms}ms +-${jitter_ms}ms to server links only..."
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

archive_case() {
    local label=$1
    local case_dir="${RUN_DIR}/${label}"
    mkdir -p "$case_dir"
    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        cp "$merged_dir"/*.csv "${case_dir}/" 2>/dev/null || true
    fi
    find "${SCRIPT_DIR}/eval" -mindepth 2 -maxdepth 2 -name "tps_timeline_*.csv" -exec cp {} "${case_dir}/" \; 2>/dev/null || true
    echo "  Archived results to: $case_dir"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        CLIENT_COUNT=5 CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT" || true
    fi
    if [ "$DELAY_APPLIED" = true ]; then
        remove_server_delay || true
    fi
}
trap cleanup EXIT

echo "================================================================"
echo " HETERO-5 NETEM SINGLE DELAY RUN (WOC): ${DELAY_MS}ms +-${JITTER_MS}ms, indep=90, batch=1"
echo "================================================================"
echo "Result archive: $RUN_DIR"

apply_server_only_delay "$DELAY_MS" "$JITTER_MS"
DELAY_APPLIED=true

CLUSTER_ACTIVE=true
env "${BASE_ENV[@]}" bash "$START_SCRIPT"

echo "  Running for ${RUNTIME_SECONDS}s..."
sleep "$RUNTIME_SECONDS"

CLIENT_COUNT=5 CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT"
CLUSTER_ACTIVE=false

archive_case "delay_${DELAY_MS}ms_jitter_${JITTER_MS}ms_indep90_b1"

remove_server_delay
DELAY_APPLIED=false

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 netem single delay run complete (WOC)"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
