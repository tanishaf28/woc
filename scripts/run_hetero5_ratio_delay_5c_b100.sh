#!/bin/bash
# ================================================================
# HETERO-5 NETEM RATIO SWEEP + TIMELINE (WOC): independent-ratio sweep
# (100 -> 0) under a fixed server-side-only netem delay profile, with a
# 5-client pool and BATCHSIZE=100, followed by a single INDEP_RATIO=90
# timeline run (ENABLE_TIMESERIES=true).
#
# Ported from epaxos's run_hetero5_ratio_delay_5c_b100.sh. SERVER_IPS is
# derived from CONFIG_PATH at runtime (awk over the conf file) rather than
# hardcoded, unlike the epaxos original -- that script's hardcoded IP list
# no longer matches its own cluster_hetero_5n_10c.conf (IPs drifted after
# the pool was regenerated), so its netem delay silently lands on hosts
# outside the running cluster. Deriving from CONFIG_PATH avoids the same
# staleness bug here.
#
# Delay is applied ONCE before the first case and removed ONCE after the
# very last case (sweep + timeline), not toggled per case.
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

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_ratio_delay_5c_b100"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

DELAY_MS="${DELAY_MS:-10}"
JITTER_MS="${JITTER_MS:-10}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
DELAY_APPLIED=false
CLUSTER_ACTIVE=false

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=5"
    "CONFIG_PATH=${CONFIG_PATH}"
    "THRESHOLD=2"
    "BATCHSIZE=100"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "LOG_LEVEL=info"
)

# Same sweep points as eval1 in run_plainmsg_evals.sh, 100 (all
# independent) -> 0 (all dependent).
TEST_CASES=(100.0 90.0 80.0 60.0 40.0 20.0 10.0 0.0)

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

# apply_server_only_delay: delay+jitter on SERVER_IPS only. CLIENT_IPS are
# never touched, satisfying the "server-side only" requirement.
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

# archive_case copies only merged CSVs newer than this case's marker, so
# each case's folder gets just its own run (stop_cluster_hetero.sh never
# cleans old merged_*.csv files, they only accumulate).
archive_case() {
    local label=$1
    local marker=$2
    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"
    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -newer "$marker" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$dest_dir"/*.csv 2>/dev/null)" ]; then
            local newest
            newest=$(ls -t "$merged_dir"/*.csv 2>/dev/null | head -1) || true
            [ -n "$newest" ] && cp "$newest" "$dest_dir/"
        fi
    fi
    # Per-client tps_timeline_*.csv (only produced when ENABLE_TIMESERIES=true)
    # go flat into dest_dir -- plot_timeseries.py globs the case dir directly.
    find "${SCRIPT_DIR}/eval" -mindepth 2 -maxdepth 2 -name "tps_timeline_*.csv" -newer "$marker" -exec cp {} "${dest_dir}/" \; 2>/dev/null || true
    echo "  Archived results to: $dest_dir"
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

run_case() {
    local indep=$1
    local label=$2
    local extra_env=("${@:3}")

    touch "${RUN_DIR}/.marker_${label}"
    local marker="${RUN_DIR}/.marker_${label}"

    CLUSTER_ACTIVE=true
    env "${BASE_ENV[@]}" "INDEP_RATIO=${indep}" "${extra_env[@]}" bash "$START_SCRIPT"

    echo "  Running for ${RUNTIME_SECONDS}s..."
    sleep "$RUNTIME_SECONDS"

    CLIENT_COUNT=5 CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false

    archive_case "$label" "$marker"
}

echo "================================================================"
echo " HETERO-5 NETEM RATIO SWEEP + TIMELINE (WOC): ${DELAY_MS}ms +-${JITTER_MS}ms, 5 clients, batch=100"
echo "================================================================"
echo "Result archive: $RUN_DIR"
echo "Sweep test cases (INDEP_RATIO): ${TEST_CASES[*]}"

apply_server_only_delay "$DELAY_MS" "$JITTER_MS"
DELAY_APPLIED=true

case_num=1
for indep in "${TEST_CASES[@]}"; do
    echo ""
    echo "--- Sweep case ${case_num}/${#TEST_CASES[@]}: INDEP_RATIO=${indep} ---"
    run_case "$indep" "indep_${indep}"
    case_num=$((case_num + 1))
done

echo ""
echo "--- Timeline case: INDEP_RATIO=90 (ENABLE_TIMESERIES=true) ---"
run_case "90.0" "indep_90_timeline" "ENABLE_TIMESERIES=true"

remove_server_delay
DELAY_APPLIED=false

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 netem ratio sweep + timeline complete (WOC)"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
