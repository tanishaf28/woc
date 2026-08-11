#!/bin/bash
# =============================================================
# Shared crash-injection helpers for scripts/run_hetero5_crash_eval.sh
# (its only remaining caller).
#
# Sampling is done in-process by the clients: each client writes
# tps_timeline_<timestamp>.csv under ./eval/client<ID>/. This file only
# injects event labels and performs the actual kill.
#
# Caller must define before sourcing: SSH_KEY, USER, REMOTE_DIR,
# SERVER_IPS, CLIENT_IPS, BASE_ENV, START_SCRIPT, RUN_DIR, RUNTIME_SECONDS,
# and the functions stop_plain_cluster() / archive_latest_result().
# =============================================================

detect_interface() {
    local host=$1
    ssh -i "$SSH_KEY" "$USER@$host" "ip route show default 2>/dev/null | awk '{print \$5; exit}'"
}

kill_woc_on_node() {
    local ip=$1
    local label=${2:-woc}

    if ssh -i "$SSH_KEY" "$USER@$ip" "pgrep -x woc >/dev/null 2>&1"; then
        echo "  Killing ${label} on ${ip}..."
        ssh -i "$SSH_KEY" "$USER@$ip" "pkill -TERM -x woc 2>/dev/null || true" || true
        sleep 1
        if ssh -i "$SSH_KEY" "$USER@$ip" "pgrep -x woc >/dev/null 2>&1"; then
            ssh -i "$SSH_KEY" "$USER@$ip" "pkill -KILL -x woc 2>/dev/null || true" || true
            sleep 1
        fi
        if ssh -i "$SSH_KEY" "$USER@$ip" "pgrep -x woc >/dev/null 2>&1"; then
            echo "  WARNING: ${label} still running on ${ip} after SIGKILL"
            return 1
        fi
        echo "  Confirmed ${label} stopped on ${ip}"
    else
        echo "  Note: ${label} was not running on ${ip}"
    fi
}

# ----------------------------------------------------------------
# inject_event <label>
# ----------------------------------------------------------------
inject_event() {
    local label="$1"
    local num_servers="${NUM_SERVERS:-${#SERVER_IPS[@]}}"
    for i in "${!CLIENT_IPS[@]}"; do
        local cid=$(( num_servers + i ))
        local event_path="${REMOTE_DIR}/eval/client${cid}/.event"
        ssh -o ConnectTimeout=3 -o BatchMode=yes \
            -i "$SSH_KEY" "$USER@${CLIENT_IPS[$i]}" \
            "mkdir -p '${REMOTE_DIR}/eval/client${cid}' && printf '%s\n' '${label}' > '${event_path}'" \
            >/dev/null 2>&1 &
    done
    wait
}

# ================================================================
# run_crash_case_sampled
# node_spec: no_failure | leader | follower:<id> | f_of_n:<count>
# ================================================================
run_crash_case_sampled() {
    local label=$1
    local node_spec=$2
    local crash_trigger="${CRASH_TRIGGER_SECONDS:-10}"

    echo ""
    echo "=================================================="
    echo "Running (sampled): $label  [crash: ${node_spec} at t=${crash_trigger}s]"
    echo "=================================================="

    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"
    CLUSTER_ACTIVE=true
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
    inject_event "stable"

    echo "  [crash] Waiting ${crash_trigger}s before fault injection..."
    sleep "$crash_trigger"

    local kind="${node_spec%%:*}"
    local arg="${node_spec#*:}"

    case "$kind" in
        no_failure)
            inject_event "no_failure_baseline"
            echo "  [crash] No failure — baseline run"
            ;;
        leader)
            inject_event "crash_leader"
            kill_woc_on_node "${SERVER_IPS[0]}" "leader"
            echo "  [crash] Leader killed at $(date '+%H:%M:%S')"
            ;;
        follower)
            inject_event "crash_follower${arg}"
            kill_woc_on_node "${SERVER_IPS[$arg]}" "server${arg}"
            echo "  [crash] Follower ${arg} killed at $(date '+%H:%M:%S')"
            ;;
        f_of_n)
            local available=()
            for i in "${!SERVER_IPS[@]}"; do [ "$i" -eq 0 ] && continue; available+=("$i"); done
            local killed=()
            for (( k=0; k<arg; k++ )); do
                local pick=$(( RANDOM % ${#available[@]} ))
                killed+=("${available[$pick]}")
                available=("${available[@]:0:$pick}" "${available[@]:$(( pick+1 ))}")
            done
            inject_event "crash_f${arg}"
            for fid in "${killed[@]}"; do kill_woc_on_node "${SERVER_IPS[$fid]}" "server${fid}" & done
            wait
            echo "  [crash] Killed: ${killed[*]} at $(date '+%H:%M:%S')"
            ;;
        *) echo "  ERROR: unknown crash spec '$node_spec'"; return 1 ;;
    esac

    inject_event "post_crash"
    sleep "$RUNTIME_SECONDS"
    stop_plain_cluster
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"
}
