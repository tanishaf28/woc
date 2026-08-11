#!/bin/bash
# ================================================================
# HOMOGENEOUS EVAL 5: Indep-to-Dependent Ratio Sweep x Cluster Size,
# 5-client pool
#
# Homogeneous-cluster counterpart of eval_5_i2d_size_sweep.sh (itself
# mirrored from EPaxos's run_hetero_ratio_sweep_10c.sh). Sweeps INDEP_RATIO
# over 100,90,80,60,40,20,10,0 (batch=1, msgsize=512) across cluster sizes
# n=3,5,7,11, against a fixed pool of 5 clients -- all servers/clients are
# sliced from the single flat config/cluster_homo.conf pool (server id i
# always maps to row i regardless of cluster size, so no per-size config
# files are needed, unlike the hetero sweep).
#
# Delegates cluster lifecycle to start_cluster_homo.sh/stop_cluster_homo.sh
# via env-var injection, same style as eval_5/eval_6's hetero refactor.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_homo.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_homo.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/woc"
CONFIG_LOCAL="${REPO_ROOT}/config/cluster_homo.conf"
RESULT_ROOT="${SCRIPT_DIR}/results/homo_ratio_sweep_5c"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false

mapfile -t ALL_IPS < <(awk 'NF >= 2 { print $2 }' "$CONFIG_LOCAL")

ALL_CLUSTER_SIZES=(3 5 7 11)
if [ -n "${CLUSTER_SIZES:-}" ]; then
    read -r -a ALL_CLUSTER_SIZES <<< "$CLUSTER_SIZES"
fi

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
CLIENT_COUNT="${CLIENT_COUNT:-5}"
TEST_CASES=(100.0 90.0 80.0 60.0 40.0 20.0 10.0 0.0)

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_homo_ratio_sweep_5c.sh

Sweeps INDEP_RATIO over 100,90,80,60,40,20,10,0 (matches
eval_5_i2d_size_sweep.sh / run_hetero_ratio_sweep_10c.sh) with BATCHSIZE=1,
MSG_SIZE=512 fixed, across cluster sizes n=3,5,7,11 (homogeneous), against a
fixed pool of 5 clients. All servers/clients are sliced from the single
flat config/cluster_homo.conf pool.

Environment overrides:
  RUNTIME_SECONDS=30          wall-clock seconds per run
  CLIENT_COUNT=5              client count for every size in the sweep
  CLUSTER_SIZES="3 5 7 11"    override the cluster-size sweep

Results archived under: results/homo_ratio_sweep_5c/<timestamp>/n<size>/<label>/
EOF
    exit 0
fi

max_n=0
for n in "${ALL_CLUSTER_SIZES[@]}"; do
    [ "$n" -gt "$max_n" ] && max_n=$n
done
required_ips=$((max_n + CLIENT_COUNT))
if [ "$required_ips" -gt "${#ALL_IPS[@]}" ]; then
    echo "CLIENT_COUNT=$CLIENT_COUNT with max cluster size n=$max_n needs $required_ips IPs, pool only has ${#ALL_IPS[@]}" >&2
    exit 1
fi

mkdir -p "$RUN_DIR"
touch "${RUN_DIR}/.run_start_marker"

remote_exec() {
    local host=$1
    shift
    ssh -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 "$USER@$host" "$*"
}

cleanup_all_nodes() {
    echo "=================================================="
    echo " Pre-sweep cleanup: purging stale WOC processes (all sizes)..."
    echo "=================================================="
    for ip in "${ALL_IPS[@]}"; do
        remote_exec "$ip" "pkill -9 -x woc 2>/dev/null" &
    done
    wait
}

archive_latest_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/n${CURRENT_N}/${label}/merged"
    mkdir -p "$dest_dir"
    local marker="${RUN_DIR}/.last_archive_ts"
    local find_args=()
    if [ -f "$marker" ]; then
        find_args=(-newer "$marker")
    fi

    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" "${find_args[@]}" \
            -exec cp {} "$dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$dest_dir"/*.csv 2>/dev/null)" ]; then
            cp "$merged_dir"/*.csv "$dest_dir/" 2>/dev/null || true
        fi
    fi
    touch "$marker"
    echo "  Archived results to: $dest_dir"
    ls -1 "$dest_dir"/*.csv 2>/dev/null | sed 's|.*/|    |' || echo "  (no CSVs found)"
}

start_workload_cluster() {
    echo "Starting WOC homogeneous cluster (n=${CURRENT_N}, clients=${CURRENT_CLIENT_COUNT})..."
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

stop_workload_cluster() {
    SERVER_COUNT="$CURRENT_N" CLIENT_COUNT="$CURRENT_CLIENT_COUNT" bash "$STOP_SCRIPT"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_workload_cluster || true
    fi
}
trap cleanup EXIT

run_case() {
    local label=$1
    local runtime=$2

    echo "=================================================="
    echo "Running: $label"
    echo "  n=${CURRENT_N}  threshold=${CURRENT_T}  clients=${CURRENT_CLIENT_COUNT}  runtime=${runtime}s"
    echo "=================================================="

    CLUSTER_ACTIVE=true
    start_workload_cluster
    sleep "$runtime"
    stop_workload_cluster
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"

    echo "  Cooling down to release TCP ports..."
    sleep 5
}

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     RATIO SWEEP x CLUSTER SIZE, 5-CLIENT POOL (WOC HOMO)       ║"
echo "║         Cluster sizes: n = 3, 5, 7, 11 (homogeneous)           ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

for n in "${ALL_CLUSTER_SIZES[@]}"; do
    t=$(( (n - 1) / 2 ))
    CURRENT_CLIENT_COUNT=$CLIENT_COUNT
    CURRENT_N="$n"
    CURRENT_T="$t"

    echo ""
    echo "=================================================="
    echo " Cluster size n=${CURRENT_N} (threshold=${CURRENT_T}, clients=${CURRENT_CLIENT_COUNT})"
    echo "=================================================="

    for indep in "${TEST_CASES[@]}"; do
        BASE_ENV=(
            "NUM_SERVERS=${n}" "NUM_CLIENTS=${CURRENT_CLIENT_COUNT}" "THRESHOLD=${t}"
            "BATCHSIZE=1" "INDEP_RATIO=${indep}" "NUM_OBJECTS=1000"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true"
        )
        run_case "n${n}_indep_${indep}" "$RUNTIME_SECONDS"
    done
done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR"

echo ""
echo "=================================================="
echo " Ratio sweep (5-client pool, homogeneous) complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
echo "Summary CSV: $RUN_DIR/extracted_metrics.csv"
