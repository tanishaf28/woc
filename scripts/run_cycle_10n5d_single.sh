#!/bin/bash
# ================================================================
# HETERO-5 NETEM CYCLE RUN (WOC): calm/burst delay cycling -- normal
# operation for NORMAL_SECONDS, then server-side-only netem delay
# (DELAY_MS +-JITTER_MS) for BURST_SECONDS, repeated NUM_CYCLES times,
# against one continuous run at INDEP_RATIO=100, BATCHSIZE=10
# (ENABLE_TIMESERIES=true throughout).
#
# Ported from epaxos's run_cycle_10n5d_single.sh. SERVER_IPS is derived
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

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_delay_b10_r100_single"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

DELAY_MS="${DELAY_MS:-5}"
JITTER_MS="${JITTER_MS:-5}"
DELAY_APPLIED=false
CLUSTER_ACTIVE=false

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=5"
    "CONFIG_PATH=${CONFIG_PATH}"
    "THRESHOLD=2"
    "BATCHSIZE=10"
    "INDEP_RATIO=100.0"
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
    local netem_args="delay ${delay_ms}ms"
    if [ "$jitter_ms" -gt 0 ]; then
        netem_args="delay ${delay_ms}ms ${jitter_ms}ms distribution normal"
    fi
    echo "  [netem] Applying ${delay_ms}ms +-${jitter_ms}ms to server links only..."
    for ip in "${SERVER_IPS[@]}"; do
        local iface
        iface=$(detect_interface "$ip")
        [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
        remote_exec "$ip" \
            "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
             sudo tc qdisc add dev '$iface' root netem ${netem_args}" \
            || true
        local check
        check=$(remote_exec "$ip" "tc qdisc show dev '$iface' | grep -i netem" || true)
        if [ -z "$check" ]; then
            echo "  ERROR: netem did not apply on $ip ($iface)"
        else
            echo "  $ip ($iface): $check"
        fi
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

NORMAL_SECONDS="${NORMAL_SECONDS:-10}"
BURST_SECONDS="${BURST_SECONDS:-5}"
NUM_CYCLES="${NUM_CYCLES:-4}"

echo "================================================================"
echo " HETERO-5 NETEM CYCLE RUN (WOC): ${NORMAL_SECONDS}s normal / ${DELAY_MS}ms+-${JITTER_MS}ms for ${BURST_SECONDS}s, x${NUM_CYCLES}, indep=100, batch=10"
echo "================================================================"
echo "Result archive: $RUN_DIR"

CLUSTER_ACTIVE=true
env "${BASE_ENV[@]}" bash "$START_SCRIPT"
t0_epoch=$(date +%s.%N)
cycle_meta="${RUN_DIR}/cycle_meta.txt"
echo "t0_epoch=${t0_epoch}" > "$cycle_meta"

for (( c=1; c<=NUM_CYCLES; c++ )); do
    echo "  [cycle ${c}/${NUM_CYCLES}] normal for ${NORMAL_SECONDS}s..."
    t_normal_start=$(date +%s.%N)
    sleep "$NORMAL_SECONDS"
    apply_server_only_delay "$DELAY_MS" "$JITTER_MS"
    DELAY_APPLIED=true
    t_delay_start=$(date +%s.%N)
    echo "  [cycle ${c}/${NUM_CYCLES}] delay ${DELAY_MS}ms+-${JITTER_MS}ms for ${BURST_SECONDS}s..."
    sleep "$BURST_SECONDS"
    remove_server_delay
    DELAY_APPLIED=false
    t_delay_end=$(date +%s.%N)
    awk -v t0="$t0_epoch" -v ts="$t_normal_start" -v td="$t_delay_start" -v te="$t_delay_end" \
        -v c="$c" 'BEGIN{printf "cycle_%d_normal_start_s=%.3f\ncycle_%d_delay_start_s=%.3f\ncycle_%d_delay_end_s=%.3f\n", c,(ts-t0), c,(td-t0), c,(te-t0)}' \
        >> "$cycle_meta"
done

CLIENT_COUNT=5 CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT"
CLUSTER_ACTIVE=false

archive_case "cycle_10n5d_${DELAY_MS}ms_jitter_${JITTER_MS}ms_indep100_b10"
cp "$cycle_meta" "${RUN_DIR}/cycle_10n5d_${DELAY_MS}ms_jitter_${JITTER_MS}ms_indep100_b10/" 2>/dev/null || true

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 netem cycle run complete (WOC)"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
