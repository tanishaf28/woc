#!/bin/bash
# ================================================================
# HETEROGENEOUS PLAIN-MSG EVALUATION RUNNER
# Runs the 5-node cluster across all requested test families.
# eval4 matches Cabinet paper netem methodology: D1/D2/D3/D4
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/woc"
RESULT_ROOT="${SCRIPT_DIR}/results/hetero_plainmsg"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false
ACTIVE_CLUSTER_KIND="plain"

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=2"
    "THRESHOLD=1"
    "OPS=0"
    "EVAL_TYPE=0"
    "BATCHSIZE=1"
    "MSG_SIZE=512"
    "MODE=1"
    "CONFLICT_RATE=0"
    "INDEP_RATIO=90.0"
    "COMMON_RATIO=10.0"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "USE_ADAPTIVE_LIMITER=false"
    "PARALLEL_FAST_PATH=true"
    "LOG_LEVEL=info"
    "ENABLE_PRIORITY=true"
    "SERVER_BATCHING=false"
)

SERVER_IPS=(
    "192.168.73.159"
    "192.168.73.84"
    "192.168.73.69"
    "192.168.73.235"
    "192.168.73.194"
)

CLIENT_IPS=(
    "192.168.73.218"
    "192.168.73.219"
)

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
FAULT_PRE_DELAY_SECONDS="${FAULT_PRE_DELAY_SECONDS:-5}"
EVAL_ONLY="${1:-all}"

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

# Pre-cache ALL interfaces into _CACHED_SERVER_IFACES / _CACHED_CLIENT_IFACES.
# Call BEFORE applying any netem so SSH is not subject to artificial delay.
cache_all_interfaces() {
    echo "  [iface] Pre-caching network interfaces..."
    _CACHED_SERVER_IFACES=()
    _CACHED_CLIENT_IFACES=()
    for ip in "${SERVER_IPS[@]}"; do
        local iface
        iface=$(detect_interface "$ip")
        _CACHED_SERVER_IFACES+=("$iface")
        echo "    server ${ip} -> ${iface}"
    done
    for ip in "${CLIENT_IPS[@]}"; do
        local iface
        iface=$(detect_interface "$ip")
        _CACHED_CLIENT_IFACES+=("$iface")
        echo "    client ${ip} -> ${iface}"
    done
}

archive_latest_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}"
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

    start_plain_cluster() {
        echo "Starting plain-message heterogeneous cluster..."
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"
    }

    stop_plain_cluster() {
        bash "$STOP_SCRIPT"
    }

    inject_failures() {
        local nodes_csv=$1
        if [ -z "$nodes_csv" ]; then
            return 0
        fi
        echo "Injecting failures on server nodes: $nodes_csv"
        IFS=',' read -r -a nodes <<< "$nodes_csv"
        for node_id in "${nodes[@]}"; do
            local ip="${SERVER_IPS[$node_id]}"
            kill_woc_on_node "$ip" "server${node_id}"
        done
    }

    kill_woc_on_node() {
        local ip=$1
        local label=${2:-woc}

        if remote_exec "$ip" "pgrep -x woc >/dev/null 2>&1"; then
            echo "  Killing ${label} on ${ip}..."
            remote_exec "$ip" "pkill -TERM -x woc 2>/dev/null || true" || true
            sleep 1
            if remote_exec "$ip" "pgrep -x woc >/dev/null 2>&1"; then
                remote_exec "$ip" "pkill -KILL -x woc 2>/dev/null || true" || true
                sleep 1
            fi
            if remote_exec "$ip" "pgrep -x woc >/dev/null 2>&1"; then
                echo "  WARNING: ${label} still running on ${ip} after SIGKILL"
                return 1
            fi
            echo "  Confirmed ${label} stopped on ${ip}"
        else
            echo "  Note: ${label} was not running on ${ip}"
        fi
    }

    log_qdisc_state() {
        local label=$1
        echo "  [netem] qdisc state check: ${label}"
        for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
            iface=$(detect_interface "$ip")
            if [ -z "$iface" ]; then
                echo "    $ip: interface not found"
                continue
            fi
            state=$(remote_exec "$ip" "tc qdisc show dev '$iface' 2>/dev/null | tr '\n' ' '" || true)
            echo "    $ip ($iface): ${state:-<no output>}"
        done
    }

    # ================================================================
    # NETEM APPLICATION FUNCTIONS
    # ================================================================

    # ----------------------------------------------------------------
    # apply_uniform_delay: same delay+jitter on ALL nodes (D1)
    # Args: delay_ms jitter_ms
    # ----------------------------------------------------------------
    apply_uniform_delay() {
        local delay_ms=$1
        local jitter_ms=$2

        if [ "$delay_ms" -eq 0 ]; then
            echo "  [netem D1] 0ms baseline — removing any existing netem rules..."
            remove_all_delay
            log_qdisc_state "D1 baseline after cleanup"
            return 0
        fi

        echo "  [netem D1] Applying ${delay_ms}ms ±${jitter_ms}ms uniform on all nodes..."
        for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
            iface=$(detect_interface "$ip")
            [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
            remote_exec "$ip" \
                "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '$iface' root netem delay ${delay_ms}ms ${jitter_ms}ms distribution normal" \
                || true
        done
        sleep 1
    }

    # ----------------------------------------------------------------
    # apply_skew_delay: declining delay per node (D2)
    # Server 0=1000ms → Server 4=100ms. Clients get no delay.
    # ----------------------------------------------------------------
    apply_skew_delay() {
        local -a SKEW_DELAYS=(1000 700 400 200 100)
        local -a SKEW_JITTERS=(200 140 80 40 20)
        echo "  [netem D2] Applying skewed delays (1000→100ms) across server nodes..."

        for i in "${!SERVER_IPS[@]}"; do
            local ip="${SERVER_IPS[$i]}"
            local d="${SKEW_DELAYS[$i]}"
            local j="${SKEW_JITTERS[$i]}"
            iface=$(detect_interface "$ip")
            [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
            remote_exec "$ip" \
                "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '$iface' root netem delay ${d}ms ${j}ms distribution normal" \
                || true
            echo "    server${i} (${ip}): ${d}ms ±${j}ms"
        done

        for ip in "${CLIENT_IPS[@]}"; do
            iface=$(detect_interface "$ip")
            [ -z "$iface" ] && continue
            remote_exec "$ip" \
                "sudo tc qdisc del dev '$iface' root 2>/dev/null || true" || true
        done
        sleep 1
    }

    # ----------------------------------------------------------------
    # apply_packet_loss: uniform loss on ALL nodes
    # Args: loss_pct (e.g. "0.1", "1", "5")
    # ----------------------------------------------------------------
    apply_packet_loss() {
        local loss_pct=$1
        echo "  [netem L] Applying ${loss_pct}% packet loss on all nodes..."
        for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
            iface=$(detect_interface "$ip")
            [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
            remote_exec "$ip" \
                "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '$iface' root netem loss ${loss_pct}%" \
                || true
        done
        sleep 1
    }

    # ----------------------------------------------------------------
    # apply_correlated_loss: Gilbert-Elliott burst loss model
    # Args: loss_pct  burst_stay_pct
    # netem gemodel: p=enter-loss r=leave-loss 1-h=loss-in-burst
    # ----------------------------------------------------------------
    apply_correlated_loss() {
        local loss_pct=$1
        local burst_pct=$2
        echo "  [netem L-burst] Applying correlated loss ${loss_pct}% burst-stay=${burst_pct}%..."
        for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
            iface=$(detect_interface "$ip")
            [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
            remote_exec "$ip" \
                "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '$iface' root netem loss gemodel ${loss_pct}% ${burst_pct}% 80% 0%" \
                || true
        done
        sleep 1
    }

    # ----------------------------------------------------------------
    # apply_bandwidth_cap: TBF rate limit on ALL nodes
    # Args: rate (e.g. "10mbit", "1mbit", "512kbit")
    # ----------------------------------------------------------------
    apply_bandwidth_cap() {
        local rate=$1
        echo "  [netem BW] Applying bandwidth cap ${rate} on all nodes..."
        for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
            iface=$(detect_interface "$ip")
            [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
            remote_exec "$ip" \
                "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '$iface' root handle 1: tbf rate ${rate} burst 32kbit latency 400ms" \
                || true
        done
        sleep 1
    }

    # ----------------------------------------------------------------
    # apply_jitter_only: pure jitter, no meaningful base delay.
    # netem requires non-zero base to accept jitter — use 1ms.
    # Args: jitter_ms
    # ----------------------------------------------------------------
    apply_jitter_only() {
        local jitter_ms=$1
        echo "  [netem J] Applying pure jitter ±${jitter_ms}ms on all nodes..."
        for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
            iface=$(detect_interface "$ip")
            [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
            remote_exec "$ip" \
                "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '$iface' root netem delay 1ms ${jitter_ms}ms distribution normal" \
                || true
        done
        sleep 1
    }

    # ----------------------------------------------------------------
    # remove_all_delay: strip netem/tbf from every node.
    # Uses cached interfaces if available (avoids SSH under netem).
    # ----------------------------------------------------------------
    remove_all_delay() {
        echo "  [netem] Removing all network impairments..."

        # Use cached ifaces if populated, otherwise fall back to detect
        local use_cache=false
        if declare -p _CACHED_SERVER_IFACES _CACHED_CLIENT_IFACES >/dev/null 2>&1 && \
           [ "${#_CACHED_SERVER_IFACES[@]}" -eq "${#SERVER_IPS[@]}" ] && \
           [ "${#_CACHED_CLIENT_IFACES[@]}" -eq "${#CLIENT_IPS[@]}" ]; then
            use_cache=true
        fi

        local all_ips=("${SERVER_IPS[@]}" "${CLIENT_IPS[@]}")

        if [ "$use_cache" = true ]; then
            local all_ifaces=("${_CACHED_SERVER_IFACES[@]}" "${_CACHED_CLIENT_IFACES[@]}")
            for idx in "${!all_ips[@]}"; do
                local ip="${all_ips[$idx]}"
                local iface="${all_ifaces[$idx]}"
                [ -z "$iface" ] && continue
                ssh -i "$SSH_KEY" "$USER@$ip" \
                    "sudo tc qdisc del dev '$iface' root 2>/dev/null || true" || true &
            done
            wait
        else
            for ip in "${all_ips[@]}"; do
                iface=$(detect_interface "$ip")
                [ -z "$iface" ] && continue
                remote_exec "$ip" "sudo tc qdisc del dev '$iface' root 2>/dev/null || true" || true
            done
        fi
        sleep 1
    }

    teardown_network_delay() {
        remove_all_delay
        sleep 1
    }

    # ================================================================
    # RUN CASE FUNCTIONS
    # ================================================================

    run_case() {
        local label=$1
        local runtime=$2

        echo ""
        echo "=================================================="
        echo "Running: $label"
        echo "=================================================="

        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"
        sleep "$runtime"
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    run_fault_case() {
        local label=$1
        local failed_nodes=$2

        echo ""
        echo "=================================================="
        echo "Running: $label"
        echo "=================================================="

        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"
        sleep "$FAULT_PRE_DELAY_SECONDS"
        inject_failures "$failed_nodes"
        sleep "$((RUNTIME_SECONDS - FAULT_PRE_DELAY_SECONDS))"
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ----------------------------------------------------------------
    # run_d1_case: D1 uniform delay
    # ----------------------------------------------------------------
    run_d1_case() {
        local label=$1
        local delay_ms=$2
        local jitter_ms=$3

        echo ""
        echo "=================================================="
        echo "Running: $label  [D1 uniform ${delay_ms}ms ±${jitter_ms}ms]"
        echo "=================================================="

        apply_uniform_delay "$delay_ms" "$jitter_ms"
        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"
        sleep "$RUNTIME_SECONDS"
        remove_all_delay
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ----------------------------------------------------------------
    # run_d2_case: D2 skewed delay
    # ----------------------------------------------------------------
    run_d2_case() {
        local label=$1

        echo ""
        echo "=================================================="
        echo "Running: $label  [D2 skewed 1000→100ms]"
        echo "=================================================="

        apply_skew_delay
        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"
        sleep "$RUNTIME_SECONDS"
        remove_all_delay
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ----------------------------------------------------------------
    # run_d3_case: D3 dynamically rotating delays.
    # NOTE: sets CLUSTER_ACTIVE directly (not in subshell) so that
    # the cleanup trap fires correctly on failure.
    # ----------------------------------------------------------------
    run_d3_case() {
        local label=$1
        local rotate_interval="${2:-8}"
        local runtime_override="${3:-$RUNTIME_SECONDS}"

        echo ""
        echo "=================================================="
        echo "Running: $label  [D3 dynamic rotating, interval=${rotate_interval}s, runtime=${runtime_override}s]"
        echo "=================================================="

        local -a SKEW_DELAYS=(1000 700 400 200 100)
        local -a SKEW_JITTERS=(200 140 80 40 20)

        cache_all_interfaces

        remove_all_delay
        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"

        local elapsed=0
        local rotation=0

        while [ "$elapsed" -lt "$runtime_override" ]; do
            echo "  [D3] Rotation ${rotation} at ${elapsed}s (offset=${rotation})"

            for i in "${!SERVER_IPS[@]}"; do
                local ip="${SERVER_IPS[$i]}"
                local iface="${_CACHED_SERVER_IFACES[$i]}"
                local idx=$(( (i + rotation) % ${#SKEW_DELAYS[@]} ))
                local d="${SKEW_DELAYS[$idx]}"
                local j="${SKEW_JITTERS[$idx]}"
                [ -z "$iface" ] && continue
                ssh -i "$SSH_KEY" "$USER@$ip" \
                    "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
                     sudo tc qdisc add dev '$iface' root netem delay ${d}ms ${j}ms distribution normal" \
                    || true &
            done
            wait

            rotation=$(( rotation + 1 ))
            sleep "$rotate_interval"
            elapsed=$(( elapsed + rotate_interval ))
        done

        echo "  [D3] Removing delays (cached interfaces)..."
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true" || true &
        done
        wait

        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ----------------------------------------------------------------
    # run_d4_case: D4 bursting delays.
    # Calm/burst cycle using cached interfaces throughout.
    # ----------------------------------------------------------------
    run_d4_case() {
        local label=$1
        local calm_duration="${2:-10}"
        local burst_duration="${3:-5}"
        local runtime_override="${4:-$RUNTIME_SECONDS}"

        echo ""
        echo "=================================================="
        echo "Running: $label  [D4 burst: ${calm_duration}s calm / ${burst_duration}s spike, runtime=${runtime_override}s]"
        echo "=================================================="

        cache_all_interfaces

        remove_all_delay
        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"

        local elapsed=0
        local cycle=0

        while [ "$elapsed" -lt "$runtime_override" ]; do
            # CALM: remove all impairments using cached ifaces (fast)
            echo "  [D4] Cycle ${cycle}: CALM (${calm_duration}s)"
            for i in "${!SERVER_IPS[@]}"; do
                ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                    "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true" || true &
            done
            for i in "${!CLIENT_IPS[@]}"; do
                ssh -i "$SSH_KEY" "$USER@${CLIENT_IPS[$i]}" \
                    "sudo tc qdisc del dev '${_CACHED_CLIENT_IFACES[$i]}' root 2>/dev/null || true" || true &
            done
            wait

            local calm_sleep=$(( calm_duration < (runtime_override - elapsed) ? calm_duration : (runtime_override - elapsed) ))
            sleep "$calm_sleep"
            elapsed=$(( elapsed + calm_sleep ))
            [ "$elapsed" -ge "$runtime_override" ] && break

            # BURST: apply 1000±100ms using cached ifaces (fast, in parallel)
            echo "  [D4] Cycle ${cycle}: BURST (${burst_duration}s, 1000±100ms)"
            for i in "${!SERVER_IPS[@]}"; do
                ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                    "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true; \
                     sudo tc qdisc add dev '${_CACHED_SERVER_IFACES[$i]}' root netem delay 1000ms 100ms distribution normal" \
                    || true &
            done
            for i in "${!CLIENT_IPS[@]}"; do
                ssh -i "$SSH_KEY" "$USER@${CLIENT_IPS[$i]}" \
                    "sudo tc qdisc del dev '${_CACHED_CLIENT_IFACES[$i]}' root 2>/dev/null || true; \
                     sudo tc qdisc add dev '${_CACHED_CLIENT_IFACES[$i]}' root netem delay 1000ms 100ms distribution normal" \
                    || true &
            done
            wait

            local burst_sleep=$(( burst_duration < (runtime_override - elapsed) ? burst_duration : (runtime_override - elapsed) ))
            sleep "$burst_sleep"
            elapsed=$(( elapsed + burst_sleep ))
            cycle=$(( cycle + 1 ))
        done

        remove_all_delay
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ----------------------------------------------------------------
    # run_loss_case: uniform packet loss (eval5-A)
    # ----------------------------------------------------------------
    run_loss_case() {
        local label=$1
        local loss_pct=$2

        echo ""
        echo "=================================================="
        echo "Running: $label  [packet loss ${loss_pct}%]"
        echo "=================================================="

        apply_packet_loss "$loss_pct"
        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"
        sleep "$RUNTIME_SECONDS"
        remove_all_delay
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ----------------------------------------------------------------
    # run_burst_loss_case: correlated burst loss (eval5-B)
    # ----------------------------------------------------------------
    run_burst_loss_case() {
        local label=$1
        local loss_pct=$2
        local burst_pct=$3

        echo ""
        echo "=================================================="
        echo "Running: $label  [burst loss ${loss_pct}% stay=${burst_pct}%]"
        echo "=================================================="

        apply_correlated_loss "$loss_pct" "$burst_pct"
        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"
        sleep "$RUNTIME_SECONDS"
        remove_all_delay
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ----------------------------------------------------------------
    # run_jitter_case: pure jitter sweep (eval6)
    # ----------------------------------------------------------------
    run_jitter_case() {
        local label=$1
        local jitter_ms=$2

        echo ""
        echo "=================================================="
        echo "Running: $label  [jitter ±${jitter_ms}ms]"
        echo "=================================================="

        apply_jitter_only "$jitter_ms"
        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"
        sleep "$RUNTIME_SECONDS"
        remove_all_delay
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ----------------------------------------------------------------
    # run_bw_case: bandwidth cap sweep (eval7)
    # ----------------------------------------------------------------
    run_bw_case() {
        local label=$1
        local rate=$2

        echo ""
        echo "=================================================="
        echo "Running: $label  [bandwidth cap ${rate}]"
        echo "=================================================="

        apply_bandwidth_cap "$rate"
        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"
        sleep "$RUNTIME_SECONDS"
        remove_all_delay
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ----------------------------------------------------------------
    # run_crash_case: crash fault injection on HETERO cluster.
    # node_spec: leader | follower:<id> | f_of_n:<count>
    # ----------------------------------------------------------------
    run_crash_case() {
        local label=$1
        local node_spec=$2
        local crash_trigger="${CRASH_TRIGGER_SECONDS:-10}"

        echo ""
        echo "=================================================="
        echo "Running: $label  [crash: ${node_spec} at t=${crash_trigger}s]"
        echo "=================================================="

        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"

        echo "  [crash] Waiting ${crash_trigger}s before fault injection..."
        sleep "$crash_trigger"

        local kind="${node_spec%%:*}"
        local arg="${node_spec#*:}"

        case "$kind" in
            leader)
                kill_woc_on_node "${SERVER_IPS[0]}" "leader"
                echo "  [crash] Leader killed at $(date '+%H:%M:%S')"
                ;;
            follower)
                [ -z "$arg" ] || [ "$arg" = "follower" ] && { echo "  ERROR: follower requires ID"; return 1; }
                kill_woc_on_node "${SERVER_IPS[$arg]}" "server${arg}"
                echo "  [crash] Follower ${arg} killed at $(date '+%H:%M:%S')"
                ;;
            f_of_n)
                [ -z "$arg" ] || [ "$arg" = "f_of_n" ] && { echo "  ERROR: f_of_n requires count"; return 1; }
                local available=()
                for i in "${!SERVER_IPS[@]}"; do
                    [ "$i" -eq 0 ] && continue
                    available+=("$i")
                done
                local killed=()
                for (( k=0; k<arg; k++ )); do
                    local pick=$(( RANDOM % ${#available[@]} ))
                    killed+=("${available[$pick]}")
                    available=("${available[@]:0:$pick}" "${available[@]:$(( pick+1 ))}")
                done
                echo "  [crash] Killing followers: ${killed[*]}"
                for fid in "${killed[@]}"; do
                    kill_woc_on_node "${SERVER_IPS[$fid]}" "server${fid}" &
                done
                wait
                echo "  [crash] Done at $(date '+%H:%M:%S')"
                ;;
            *)
                echo "  ERROR: unknown crash spec '$node_spec'"; return 1
                ;;
        esac

        echo "  [crash] Observing ${RUNTIME_SECONDS}s after fault..."
        sleep "$RUNTIME_SECONDS"
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ----------------------------------------------------------------
    # run_crash_plus_network_case: crash + active network impairment.
    # Key metrics: re-election time, availability gap, recovery curve.
    # net_type: delay | loss | bw
    # net_param: numeric ms for delay, pct for loss, rate string for bw
    # ----------------------------------------------------------------
    run_crash_plus_network_case() {
        local label=$1
        local node_spec=$2
        local net_type=$3
        local net_param=$4
        local crash_trigger="${CRASH_TRIGGER_SECONDS:-10}"

        echo ""
        echo "=================================================="
        echo "Running: $label  [crash:${node_spec} + ${net_type}=${net_param}]"
        echo "=================================================="

        case "$net_type" in
            delay) apply_uniform_delay "$net_param" "$(( net_param / 5 ))" ;;
            loss)  apply_packet_loss "$net_param" ;;
            bw)    apply_bandwidth_cap "$net_param" ;;
        esac

        CLUSTER_ACTIVE=true
        ACTIVE_CLUSTER_KIND="plain"
        env "${BASE_ENV[@]}" bash "$START_SCRIPT"

        echo "  [crash+net] Running ${crash_trigger}s under impairment before crash..."
        sleep "$crash_trigger"

        local kind="${node_spec%%:*}"
        local arg="${node_spec#*:}"

        case "$kind" in
            leader)
                kill_woc_on_node "${SERVER_IPS[0]}" "leader"
                ;;
            follower)
                kill_woc_on_node "${SERVER_IPS[$arg]}" "server${arg}"
                ;;
            f_of_n)
                local available=()
                for i in "${!SERVER_IPS[@]}"; do
                    [ "$i" -eq 0 ] && continue
                    available+=("$i")
                done
                local pick=$(( RANDOM % ${#available[@]} ))
                local fid="${available[$pick]}"
                kill_woc_on_node "${SERVER_IPS[$fid]}" "server${fid}"
                ;;
        esac

        echo "  [crash+net] Observing recovery for ${RUNTIME_SECONDS}s..."
        sleep "$RUNTIME_SECONDS"

        remove_all_delay
        stop_plain_cluster
        CLUSTER_ACTIVE=false
        archive_latest_result "$label"
    }

    # ================================================================
    # CLEANUP TRAP
    # ================================================================
    cleanup() {
        teardown_network_delay || true
        if [ "$CLUSTER_ACTIVE" = true ]; then
            stop_plain_cluster || true
        fi
    }

    trap cleanup EXIT

    # ================================================================
    # HELP
    # ================================================================
    if [[ "${1:-}" == "--help" ]]; then
        cat <<'EOF'
    Usage: bash run_hetero_plainmsg_evals.sh [selector]
    Usage: bash run_hetero_plainmsg_evals.sh [selector|--selector]

    Selectors (default: all):
      eval1          Independent vs Common ratio sweep
      eval2          Max inflight sweep
      eval3          Fault tolerance (static node failures)
      eval_batching  Batch size sweep
      eval_msgsize   Message size sweep
      eval_crash     Crash fault injection (leader/follower/random)
            eval4          Network delay: D1 uniform sweep + D4 burst stress

        eval4 (Cabinet §5.3):
            D1: uniform  0 / 5 / 10 / 20 / 50 / 100 / 200ms  (fixed MAX_INFLIGHT=10)
            D4: bursting 1000±100ms spikes, 10s calm / 5s burst  (fixed MAX_INFLIGHT=10, min 45s runtime)

    Environment overrides:
      RUNTIME_SECONDS=30          wall-clock seconds per run
      CRASH_TRIGGER_SECONDS=10    stable time before crash injection
      FAULT_PRE_DELAY_SECONDS=5   stable time before eval3 fault injection

    Results archived under: results/hetero_plainmsg/<timestamp>/
EOF
        exit 0
    fi

    # Accept both selector styles: eval3 and --eval3
    if [[ "${EVAL_ONLY}" == --* ]]; then
        EVAL_ONLY="${EVAL_ONLY#--}"
    fi

    case "${EVAL_ONLY}" in
        all|eval1|eval2|eval3|eval_batching|eval_msgsize|eval_crash|eval4)
            ;;
        *)
            echo "ERROR: unknown selector '${EVAL_ONLY}'"
            echo "Run with --help to see valid selectors."
            exit 1
            ;;
    esac

    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║      HETEROGENEOUS PLAIN-MSG EVALUATION RUNNER                ║"
    echo "║                5-Node Cluster + 2 Clients                     ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Result archive: $RUN_DIR"
    echo ""

    # ================================================================
    # EVAL 1: independent vs common ratio sweep
    # ================================================================
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval1" ]]; then
        for case in "100.0/0.0" "90.0/10.0" "80.0/20.0" "60.0/40.0" \
                    "40.0/60.0" "20.0/80.0" "10.0/90.0" "0.0/100.0"; do
            indep="${case%/*}"
            common="${case#*/}"
            BASE_ENV=(
                "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
                "CONFLICT_RATE=0" "INDEP_RATIO=${indep}" "COMMON_RATIO=${common}"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
                "USE_ADAPTIVE_LIMITER=false" "PARALLEL_FAST_PATH=true"
                "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "SERVER_BATCHING=false"
            )
            run_case "eval1_ratio_${indep}_common_${common}" "$RUNTIME_SECONDS"
        done
    fi

    # ================================================================
    # EVAL 2: max inflight sweep
    # ================================================================
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval2" ]]; then
        for value in 1 2 3 4 5 8 10 15 20 25 30 35 40 45 50; do
            BASE_ENV=(
                "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
                "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=${value}"
                "USE_ADAPTIVE_LIMITER=false" "PARALLEL_FAST_PATH=true"
                "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "SERVER_BATCHING=false"
            )
            run_case "eval2_max_inflight_${value}" "$RUNTIME_SECONDS"
        done
    fi

    # ================================================================
    # EVAL 3: static fault tolerance
    # ================================================================
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval3" ]]; then
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
            "USE_ADAPTIVE_LIMITER=false" "PARALLEL_FAST_PATH=true"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "SERVER_BATCHING=false"
        )
        for scenario in "no_failure|" "node0_fails|0" "node1_fails|1" \
                        "node4_fails|4" "node0_node1_fail|0,1"; do
            label="${scenario%%|*}"
            nodes="${scenario#*|}"
            if [ -z "$nodes" ]; then
                run_case "eval3_${label}" "$RUNTIME_SECONDS"
            else
                run_fault_case "eval3_${label}" "$nodes"
            fi
        done
    fi

    # ================================================================
    # EVAL batching: batch size sweep
    # ================================================================
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval_batching" ]]; then
        for batch_size in 1 10 50 100 500 1000 2000; do
            BASE_ENV=(
                "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=${batch_size}" "MSG_SIZE=512" "MODE=1"
                "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
                "USE_ADAPTIVE_LIMITER=false" "PARALLEL_FAST_PATH=true"
                "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "SERVER_BATCHING=false"
            )
            run_case "eval_batching_${batch_size}" "$RUNTIME_SECONDS"
        done
    fi

    # ================================================================
    # EVAL msgsize: message size sweep
    # ================================================================
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval_msgsize" ]]; then
        for msg_size in 64 512 1024 2048 4096; do
            BASE_ENV=(
                "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=${msg_size}" "MODE=1"
                "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
                "USE_ADAPTIVE_LIMITER=false" "PARALLEL_FAST_PATH=true"
                "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "SERVER_BATCHING=false"
            )
            run_case "eval_msgsize_${msg_size}" "$RUNTIME_SECONDS"
        done
    fi

    # ================================================================
    # EVAL crash: fault injection (hetero cluster)
    # ================================================================
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval_crash" ]]; then
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "CONFLICT_RATE=10" "INDEP_RATIO=70.0" "COMMON_RATIO=20.0"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
            "USE_ADAPTIVE_LIMITER=false" "PARALLEL_FAST_PATH=true"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "SERVER_BATCHING=false"
        )
        run_case             "eval_crash_no_failure"  "$RUNTIME_SECONDS"
        run_crash_case       "eval_crash_leader"      "leader"
        run_crash_case       "eval_crash_follower1"   "follower:1"
        run_crash_case       "eval_crash_follower4"   "follower:4"
        run_crash_case       "eval_crash_f_of_n1"     "f_of_n:1"
    fi

    # ================================================================
    # EVAL 4: Network delay — D1/D2/D3/D4 (Cabinet §5.3)
    # ================================================================
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval4" ]]; then

        echo ""
        echo "╔════════════════════════════════════════════════════════════════╗"
        echo "║  EVAL 4: Network Delay  (Cabinet §5.3  D1/D4)                ║"
        echo "╚════════════════════════════════════════════════════════════════╝"

        # D1: Uniform delay sweep with fixed MAX_INFLIGHT=10
        echo "── D1: Uniform delays ──────────────────────────────────────────"
        D1_DELAYS=(0 5 10 20 50 100 200)

        for delay_ms in "${D1_DELAYS[@]}"; do
            if [ "$delay_ms" -eq 0 ]; then
                jitter_ms=0
            else
                jitter_ms=$(( delay_ms / 5 ))
            fi
            BASE_ENV=(
                "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
                "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=10"
                "USE_ADAPTIVE_LIMITER=false" "PARALLEL_FAST_PATH=true"
                "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "SERVER_BATCHING=false"
            )
            run_d1_case "eval4_D1_${delay_ms}ms" "$delay_ms" "$jitter_ms"
        done

        # D4: Bursting
        echo "── D4: Bursting (10s calm / 5s spike at 1000ms) ────────────────"
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=10"
            "USE_ADAPTIVE_LIMITER=false" "PARALLEL_FAST_PATH=true"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "SERVER_BATCHING=false"
        )
        D4_RUNTIME=$(( RUNTIME_SECONDS < 45 ? 45 : RUNTIME_SECONDS ))
        run_d4_case "eval4_D4_burst" 10 5 "$D4_RUNTIME"

    fi  # end eval4

    echo ""
    echo "=================================================="
    echo " All evaluations complete"
    echo "=================================================="
    echo "Results archived in: $RUN_DIR"