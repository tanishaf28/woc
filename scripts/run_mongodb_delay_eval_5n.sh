#!/bin/bash
# ================================================================
# MongoDB Network Delay Eval (WOC), n=5 heterogeneous, with timeseries
#
# Sweeps injected server-egress delay (0,5,10,20,50,100,200ms, via tc
# qdisc netem - see run_hetero5_netem_eval.sh for the same mechanism and
# its "server-side only" caveat: delay lands on each server's own egress
# interface, which also carries replies back to clients) against the WOC
# MongoDB replica set (start_mongodb_hetero.sh, -et=1, fixed n=5).
#
# ENABLE_TIMESERIES=true throughout, using the same collection pattern as
# run_hetero5_crash_eval.sh (the one existing eval script that already
# does this correctly): client.go writes tps_timeline_<clientID>.csv into
# its own eval dir as it runs; after each case, tps_timeline_*.csv files
# newer than this run's start marker are copied straight into that case's
# result dir (not nested under merged/), which is exactly the layout
# plot_timeseries.py's find_timeline_csvs() scans for. plot_timeseries.py
# is invoked automatically at the end against the whole RUN_DIR, so every
# case with a valid timeseries gets its own throughput-over-time plot
# without a manual follow-up step.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_mongodb_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_mongodb_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"

RESULT_ROOT="${SCRIPT_DIR}/results/mongodb_delay_eval_5n"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
TOUCH_MARKER="${RUN_DIR}/.run_start_marker"
CLUSTER_ACTIVE=false

# n=5 heterogeneous server pool (matches start_mongodb_hetero.sh /
# cluster_hetero_new.conf).
SERVER_IPS=(
    "192.168.73.59"
    "192.168.73.243"
    "192.168.73.117"
    "192.168.73.16"
    "192.168.73.94"
)

DELAYS=(0 5 10 20 50 100 200)
WORKLOAD="${WORKLOAD:-a}"
# Timeseries evals need enough samples to show a real trend, not just the
# first-few-seconds ramp-up - fixed at 60s (not the shorter 30s used by
# the ratio/threshold/batch sweeps, which don't collect timeseries).
RUNTIME_SECONDS="${RUNTIME_SECONDS:-60}"

mkdir -p "$RUN_DIR"
touch "$TOUCH_MARKER"

remote_exec() {
    local host=$1
    shift
    ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$USER@$host" "$*"
}

detect_interface() {
    local host=$1
    remote_exec "$host" "ip route show default 2>/dev/null | awk '{print \$5; exit}'"
}

apply_server_only_delay() {
    local delay_ms=$1
    if [ "$delay_ms" -eq 0 ]; then
        return 0
    fi
    echo "  [netem] Applying ${delay_ms}ms to server links only..."
    for ip in "${SERVER_IPS[@]}"; do
        local iface
        iface=$(detect_interface "$ip")
        [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
        remote_exec "$ip" \
            "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
             sudo tc qdisc add dev '$iface' root netem delay ${delay_ms}ms" \
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
    local dest_dir="${RUN_DIR}/${label}"
    local merged_dest_dir="${dest_dir}/merged"
    mkdir -p "$dest_dir" "$merged_dest_dir"

    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -newer "$TOUCH_MARKER" -exec cp {} "$merged_dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$merged_dest_dir"/*.csv 2>/dev/null)" ]; then
            local newest
            newest=$(ls -t "$merged_dir"/*.csv 2>/dev/null | head -1)
            [ -n "$newest" ] && cp "$newest" "$merged_dest_dir/"
        fi
    fi

    # Timeseries CSVs go directly into dest_dir (not merged/) - this is the
    # layout plot_timeseries.py's find_timeline_csvs() scans for.
    local timeline_src="${SCRIPT_DIR}/eval"
    if [ -d "$timeline_src" ]; then
        find "$timeline_src" -name "tps_timeline_*.csv" -newer "$TOUCH_MARKER" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi
    touch "$TOUCH_MARKER"

    echo "  Archived results to: $dest_dir"
    if ls "$dest_dir"/tps_timeline_*.csv >/dev/null 2>&1; then
        echo "    timeseries: $(ls "$dest_dir"/tps_timeline_*.csv | wc -l) file(s)"
    else
        echo "    WARNING: no tps_timeline_*.csv collected for ${label}"
    fi
}

cleanup() {
    remove_server_delay || true
    if [ "$CLUSTER_ACTIVE" = true ]; then
        bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   WOC MONGODB DELAY EVAL, n=5 (server-side only, with timeseries) ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

for delay in "${DELAYS[@]}"; do
    label="delay_${delay}ms"
    echo ""
    echo "=================================================="
    echo "Running: $label (workload=${WORKLOAD}, runtime=${RUNTIME_SECONDS}s)"
    echo "=================================================="

    apply_server_only_delay "$delay"

    CLUSTER_ACTIVE=true
    ENABLE_TIMESERIES=true TPS_TIMELINE_INTERVAL_MS="${TPS_TIMELINE_INTERVAL_MS:-500}" \
        BATCHSIZE=1 INDEP_RATIO="${INDEP_RATIO:-90.0}" NUM_OBJECTS=1000 READ_RATIO=0.0 \
        bash "$START_SCRIPT" "$WORKLOAD"

    sleep "$RUNTIME_SECONDS"

    remove_server_delay
    bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false
    archive_result "$label"

    echo "  Cooling down to release TCP ports..."
    sleep 5
done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "Generating timeseries plots..."
if python3 "${REPO_ROOT}/plot_timeseries.py" "$RUN_DIR"; then
    echo "  Plots written to: $RUN_DIR/plots/"
else
    echo "  WARNING: plot_timeseries.py failed or found no timeseries data - check tps_timeline_*.csv collection above."
fi

echo ""
echo "=================================================="
echo " WOC MongoDB delay eval complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
echo "Summary CSV: $RUN_DIR/extracted_metrics.csv"
echo "Timeseries plots: $RUN_DIR/plots/"
