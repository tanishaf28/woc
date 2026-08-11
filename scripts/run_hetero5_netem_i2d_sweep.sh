#!/bin/bash
# ================================================================
# HETERO-5 NETEM I2D SWEEP: independent-ratio sweep (100 -> 0) under a
# single fixed server-side-only netem delay profile.
#
# Delay is applied ONCE before the first case and removed ONCE after the
# last case (not toggled per case) -- mirrors run_hetero5_netem_eval.sh's
# apply_server_only_delay/remove_server_delay, looped across INDEP_RATIO
# test cases the same way eval_1_indep_ratio.sh sweeps INDEP_RATIO for the
# plain (non-netem) MongoDB workload.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_netem_i2d_sweep"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

DELAY_MS="${DELAY_MS:-10}"
JITTER_MS="${JITTER_MS:-5}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
DELAY_APPLIED=false
CLUSTER_ACTIVE=false

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=2"
    "THRESHOLD=1"
    "BATCHSIZE=1"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "LOG_LEVEL=info"
    "ENABLE_PRIORITY=true"
)

# Same sweep points as eval_1_indep_ratio.sh, 100 (all independent) -> 0
# (all dependent).
TEST_CASES=(100.0 90.0 80.0 60.0 40.0 20.0 10.0 0.0)

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
# a server's own interface, though, including replies to clients and
# server-to-server traffic, so it still shows up in client-observed
# round-trip latency - "server-side only" describes where the tc rule is
# installed, not which side of the RTT it affects (see
# run_hetero5_netem_eval.sh's header for the fuller explanation).
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

# archive_case copies only merged CSVs newer than this case's marker, so
# each case's folder gets just its own run (stop_cluster_hetero.sh never
# cleans old merged_woc_*.csv files, they only accumulate). Files land in a
# 'merged' subdir, not the case dir directly -- extract_metrics.py globs
# for <run_dir>/**/merged/merged_woc_{clients,servers}_*.csv and derives the
# case label from the parent of that 'merged' dir, so without this nesting
# the final rollup silently finds nothing.
archive_case() {
    local label=$1
    local marker=$2
    local dest_dir="${RUN_DIR}/${label}/merged"
    mkdir -p "$dest_dir"
    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -newer "$marker" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$dest_dir"/*.csv 2>/dev/null)" ]; then
            local newest
            newest=$(ls -t "$merged_dir"/*.csv 2>/dev/null | head -1)
            [ -n "$newest" ] && cp "$newest" "$dest_dir/"
        fi
    fi
    echo "  Archived results to: $dest_dir"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        bash "$STOP_SCRIPT" || true
    fi
    if [ "$DELAY_APPLIED" = true ]; then
        remove_server_delay || true
    fi
}
trap cleanup EXIT

run_case() {
    local indep=$1
    local case_num=$2
    local label="indep_${indep}"
    local marker="${RUN_DIR}/.marker_${label}"

    echo ""
    echo "--- Case ${case_num}/${#TEST_CASES[@]}: INDEP_RATIO=${indep} ---"

    touch "$marker"

    CLUSTER_ACTIVE=true
    env "${BASE_ENV[@]}" "INDEP_RATIO=${indep}" bash "$START_SCRIPT"

    echo "  Running for ${RUNTIME_SECONDS}s..."
    sleep "$RUNTIME_SECONDS"

    bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false

    archive_case "$label" "$marker"
}

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   HETERO-5 NETEM I2D SWEEP: ${DELAY_MS}ms ±${JITTER_MS}ms (server-side only)  ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo "Test cases (INDEP_RATIO): ${TEST_CASES[*]}"

apply_server_only_delay "$DELAY_MS" "$JITTER_MS"
DELAY_APPLIED=true

case_num=1
for indep in "${TEST_CASES[@]}"; do
    run_case "$indep" "$case_num"
    case_num=$((case_num + 1))
done

remove_server_delay
DELAY_APPLIED=false

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 netem i2d sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
