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

# shellcheck source=../sampler_replacement.sh
source "${REPO_ROOT}/sampler_replacement.sh"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/woc"
NUM_SERVERS=5

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_netem_eval"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

DELAY_MS="${DELAY_MS:-10}"
JITTER_MS="${JITTER_MS:-5}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
NUM_CLIENTS="${NUM_CLIENTS:-2}"
INDEP_RATIO="${INDEP_RATIO_FIXED:-90.0}"
CLUSTER_ACTIVE=false

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=${NUM_CLIENTS}"
    "THRESHOLD=1"
    "BATCHSIZE=1"
    "INDEP_RATIO=${INDEP_RATIO}"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "LOG_LEVEL=info"
    "ENABLE_PRIORITY=true"
    "ENABLE_TIMESERIES=true"
)

# Derived from the same cluster_hetero_5n_10c.conf pool that
# start_cluster_hetero.sh itself uses (NUM_SERVERS=5 -> that config file),
# so the hosts netem applies to / injects events into are guaranteed to be
# the same hosts the actual cluster gets started on. This previously drifted
# from a stale hardcoded list (59,243,27,157,78 servers / 167,137 clients)
# that only partially overlapped the real cluster - delay was silently only
# applied to 2 of 5 servers, and inject_event wrote to client hosts running
# no client process at all, so no event ever reached a real client's CSV.
CONFIG_PATH_POOL="${REPO_ROOT}/config/cluster_hetero_5n_10c.conf"
mapfile -t ALL_POOL_IPS < <(awk 'NF >= 2 {print $2}' "$CONFIG_PATH_POOL")
SERVER_IPS=("${ALL_POOL_IPS[@]:0:5}")
CLIENT_IPS=("${ALL_POOL_IPS[@]:5:NUM_CLIENTS}")

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

cache_server_ifaces() {
    _CACHED_SERVER_IFACES=()
    for ip in "${SERVER_IPS[@]}"; do
        _CACHED_SERVER_IFACES+=("$(detect_interface "$ip")")
    done
}

start_cluster_with_timeseries() {
    CLUSTER_ACTIVE=true
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

stop_cluster() {
    CLIENT_COUNT="${NUM_CLIENTS}" bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false
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
    # netem rejects "distribution normal" when jitter is 0ms ("distribution
    # specified but no latency and jitter values") - the qdisc add then
    # fails outright, and the `|| true` below silently swallows it, leaving
    # NO delay applied at all. Omit the jitter/distribution clause entirely
    # for jitter_ms=0 so a "uniform, no-jitter" delay actually takes effect.
    local netem_clause="delay ${delay_ms}ms"
    [ "$jitter_ms" -gt 0 ] && netem_clause="delay ${delay_ms}ms ${jitter_ms}ms distribution normal"
    for ip in "${SERVER_IPS[@]}"; do
        local iface
        iface=$(detect_interface "$ip")
        [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
        remote_exec "$ip" \
            "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
             sudo tc qdisc add dev '$iface' root netem ${netem_clause}" \
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
    # This script is single-shot (one netem case per invocation, eval/merged
    # cleared by the caller before each run), so every CSV present here
    # belongs to this run - copy all of them unconditionally rather than
    # filtering by mtime, which previously dropped whichever file (client
    # or server) tied with the dest_dir mkdir's own timestamp bump.
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi

    # ENABLE_TIMESERIES=true (BASE_ENV) makes each client write its own
    # tps_timeline_*.csv under eval/client<id>/ - archive those too, same
    # as epaxos's/cabinet's netem scripts.
    local timeline_src="${SCRIPT_DIR}/eval"
    if [ -d "$timeline_src" ]; then
        find "$timeline_src" -name "tps_timeline_*.csv" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi

    echo "  Archived results to: $dest_dir"
}

run_d4_burst_case() {
    local label=$1
    local calm_duration=$2
    local burst_duration=$3
    local burst_delay_ms="${4:-1000}"
    local burst_jitter_ms="${5:-100}"
    # See apply_server_only_delay's comment: netem rejects "distribution
    # normal" at jitter=0ms, which fails the qdisc add silently ("|| true"
    # below) and leaves the burst phase with NO delay applied at all.
    local burst_netem_clause="delay ${burst_delay_ms}ms"
    [ "$burst_jitter_ms" -gt 0 ] && burst_netem_clause="delay ${burst_delay_ms}ms ${burst_jitter_ms}ms distribution normal"

    echo ""
    echo "=================================================="
    echo "Running: $label  [D4 ${calm_duration}s calm / ${burst_duration}s burst @ ${burst_delay_ms}ms±${burst_jitter_ms}ms, server-only]"
    echo "=================================================="

    rm -rf "${SCRIPT_DIR}/eval"/client* "${SCRIPT_DIR}/eval"/server* "${SCRIPT_DIR}/eval/merged" 2>/dev/null || true

    cache_server_ifaces
    remove_server_delay
    start_cluster_with_timeseries
    inject_event "calm_start"

    local elapsed=0
    local cycle=0
    while [ "$elapsed" -lt "$RUNTIME_SECONDS" ]; do
        inject_event "calm_c${cycle}"
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true" || true &
        done
        wait

        local calm_sleep=$(( calm_duration < (RUNTIME_SECONDS - elapsed) ? calm_duration : (RUNTIME_SECONDS - elapsed) ))
        sleep "$calm_sleep"
        elapsed=$(( elapsed + calm_sleep ))
        [ "$elapsed" -ge "$RUNTIME_SECONDS" ] && break

        inject_event "burst_c${cycle}"
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '${_CACHED_SERVER_IFACES[$i]}' root netem ${burst_netem_clause}" \
                || true &
        done
        wait

        local burst_sleep=$(( burst_duration < (RUNTIME_SECONDS - elapsed) ? burst_duration : (RUNTIME_SECONDS - elapsed) ))
        sleep "$burst_sleep"
        elapsed=$(( elapsed + burst_sleep ))
        cycle=$(( cycle + 1 ))
    done

    inject_event "post_burst"
    remove_server_delay
    stop_cluster
    archive_result "$label"
}

cleanup() {
    remove_server_delay || true
    if [ "$CLUSTER_ACTIVE" = true ]; then
        CLIENT_COUNT="${NUM_CLIENTS}" bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

if [ "${BURST:-false}" = "true" ]; then
    RUNTIME_SECONDS="${RUNTIME_SECONDS:-60}"
    BURST_DELAY_MS="${BURST_DELAY_MS:-1000}"
    BURST_JITTER_MS="${BURST_JITTER_MS:-100}"
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║   HETERO-5 NETEM EVAL: burst ${BURST_DELAY_MS}ms±${BURST_JITTER_MS}ms (server-side only)   ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo "Result archive: $RUN_DIR"

    run_d4_burst_case "D4_burst_${BURST_DELAY_MS}ms" 15 10 "$BURST_DELAY_MS" "$BURST_JITTER_MS"
else
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
    CLIENT_COUNT="${NUM_CLIENTS}" bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false

    archive_result "netem_${DELAY_MS}ms_${JITTER_MS}ms_server_only"
fi

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 netem eval complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
