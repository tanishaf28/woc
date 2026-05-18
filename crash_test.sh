#!/bin/bash
# ================================================================
# WOC Crash Test Script
# ================================================================
# Usage:
#   ./crash_test.sh leader          # Kill server 0 (leader) mid-run
#   ./crash_test.sh follower <id>   # Kill a specific follower
#   ./crash_test.sh f_of_n <f>      # Kill f random non-leader servers
#   ./crash_test.sh restore <id>    # Restart a crashed server
#   ./crash_test.sh status          # Show which servers are alive
# ================================================================

set -e
USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
LOG_DIR="${REMOTE_DIR}/logs"

# ── Must match your launcher ──────────────────────────────────────────────────
NUM_SERVERS=5
THRESHOLD=2
CONFIG_PATH="${REMOTE_DIR}/config/cluster_homo.conf"
BATCHSIZE=10
MSG_SIZE=512
EVAL_TYPE=0
INDEP_RATIO=70.0
COMMON_RATIO=20.0
CONFLICT_RATE=10
PIPELINE_MODE="true"
MAX_INFLIGHT=5
LOG_LEVEL="info"
ENABLE_PRIORITY="true"
MODE=1

# Server IPs in ID order (must match config file)
SERVER_IPS=(
    "192.168.73.220"  # server 0 — LEADER (static)
    "192.168.73.240"  # server 1
    "192.168.73.108"  # server 2
    "192.168.73.179"  # server 3
    "192.168.73.154"  # server 4
)

# ─────────────────────────────────────────────────────────────────────────────
ssh_kill() {
    local SID=$1
    local IP="${SERVER_IPS[$SID]}"
    echo "  Killing server $SID at $IP ..."
    ssh -i $SSH_KEY $USER@$IP "pkill -SIGTERM woc 2>/dev/null; sleep 1; pkill -SIGKILL woc 2>/dev/null || true"
    echo "  Server $SID killed at $(date '+%H:%M:%S')"
}

ssh_start() {
    local SID=$1
    local IP="${SERVER_IPS[$SID]}"
    echo "  Restarting server $SID at $IP ..."
    ssh -i $SSH_KEY $USER@$IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/server${SID}
        nohup ./$BINARY \
            -id=${SID} \
            -n=${NUM_SERVERS} \
            -t=${THRESHOLD} \
            -path=${CONFIG_PATH} \
            -pd=true \
            -role=0 \
            -ops=0 \
            -b=${BATCHSIZE} \
            -indep=${INDEP_RATIO} \
            -common=${COMMON_RATIO} \
            -et=${EVAL_TYPE} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -log=${LOG_LEVEL} \
            -ep=${ENABLE_PRIORITY} \
            >> ${LOG_DIR}/server${SID}/output.log 2>&1 &
        echo \"  Server ${SID} restarted (PID=\$!)\"
    "
}

is_alive() {
    local SID=$1
    local IP="${SERVER_IPS[$SID]}"
    # Try ping RPC — if pgrep finds the woc process it's alive
    ssh -i $SSH_KEY -o ConnectTimeout=3 $USER@$IP "pgrep -x woc > /dev/null 2>&1 && echo alive || echo dead" 2>/dev/null || echo "unreachable"
}

# ─────────────────────────────────────────────────────────────────────────────
case "$1" in

  # ── Kill the leader (server 0) ────────────────────────────────────────────
  leader)
    DELAY=${2:-0}  # Optional delay in seconds before killing
    if [ "$DELAY" -gt 0 ] 2>/dev/null; then
        echo "Waiting ${DELAY}s before killing leader..."
        sleep $DELAY
    fi
    echo "=============================================="
    echo "CRASH TEST: Killing LEADER (server 0)"
    echo "Time: $(date '+%H:%M:%S')"
    echo "=============================================="
    ssh_kill 0
    echo ""
    echo "  Leader killed. Clients should now:"
    echo "  1. detectLeaderFailure() → ping timeout (2s)"
    echo "  2. handleLeaderFailure() → StartElection()"
    echo "  3. Fast-path ops continue unaffected"
    echo "  4. Slow-path ops stall until new leader elected"
    echo ""
    echo "  Monitor election:"
    echo "  ssh -i $SSH_KEY $USER@${SERVER_IPS[1]} 'grep -i \"election\\|leader\\|vote\" ${LOG_DIR}/server1/output.log | tail -20'"
    ;;

  # ── Kill a specific follower ───────────────────────────────────────────────
  follower)
    SID=${2:?Usage: $0 follower <server_id>}
    if [ "$SID" -eq 0 ]; then
        echo "Use '$0 leader' to kill server 0"
        exit 1
    fi
    echo "=============================================="
    echo "CRASH TEST: Killing FOLLOWER (server $SID)"
    echo "Time: $(date '+%H:%M:%S')"
    echo "=============================================="
    ssh_kill $SID
    echo ""
    echo "  Follower $SID killed."
    echo "  Fast-path: weight of server $SID removed from quorum"
    echo "  Slow-path: server $SID won't respond, timeout after 5s"
    echo "  System still live if remaining weight >= threshold"
    ;;

  # ── Kill f random non-leader servers ──────────────────────────────────────
  f_of_n)
    F=${2:?Usage: $0 f_of_n <f>}
    echo "=============================================="
    echo "CRASH TEST: Killing $F follower(s) randomly"
    echo "Time: $(date '+%H:%M:%S')"
    echo "=============================================="

    # Build list of follower IDs (exclude server 0)
    AVAILABLE=()
    for ((i=1; i<NUM_SERVERS; i++)); do
        AVAILABLE+=($i)
    done

    # Shuffle and pick first F
    KILLED=()
    for ((k=0; k<F; k++)); do
        IDX=$((RANDOM % ${#AVAILABLE[@]}))
        KILLED+=(${AVAILABLE[$IDX]})
        AVAILABLE=("${AVAILABLE[@]:0:$IDX}" "${AVAILABLE[@]:$((IDX+1))}")
    done

    echo "Killing servers: ${KILLED[*]}"
    for SID in "${KILLED[@]}"; do
        ssh_kill $SID
        sleep 0.5
    done
    echo ""
    echo "  Crashed: ${KILLED[*]}"
    echo "  Remaining: $((NUM_SERVERS - F - 1)) followers + leader"
    REMAINING=$((NUM_SERVERS - F - 1))
    if [ $REMAINING -lt $THRESHOLD ]; then
        echo "  WARNING: Below quorum! Slow path will STALL."
    else
        echo "  System should remain live (quorum intact)"
    fi
    ;;

  # ── Kill f_of_n INCLUDING leader ──────────────────────────────────────────
  f_plus_leader)
    F=${2:?Usage: $0 f_plus_leader <f>}
    echo "=============================================="
    echo "CRASH TEST: Killing LEADER + $F follower(s)"
    echo "Time: $(date '+%H:%M:%S')"
    echo "=============================================="
    ssh_kill 0
    sleep 1
    AVAILABLE=()
    for ((i=1; i<NUM_SERVERS; i++)); do
        AVAILABLE+=($i)
    done
    for ((k=0; k<F; k++)); do
        IDX=$((RANDOM % ${#AVAILABLE[@]}))
        SID=${AVAILABLE[$IDX]}
        ssh_kill $SID
        AVAILABLE=("${AVAILABLE[@]:0:$IDX}" "${AVAILABLE[@]:$((IDX+1))}")
        sleep 0.5
    done
    echo "  Total crashed: $((F+1)) servers"
    ;;

  # ── Restore a crashed server ───────────────────────────────────────────────
  restore)
    SID=${2:?Usage: $0 restore <server_id>}
    echo "=============================================="
    echo "RESTORE: Restarting server $SID"
    echo "Time: $(date '+%H:%M:%S')"
    echo "=============================================="
    ssh_start $SID
    echo ""
    echo "  Server $SID restarted. Wait ~15s for pre-warming."
    echo "  Monitor: ssh -i $SSH_KEY $USER@${SERVER_IPS[$SID]} 'tail -f ${LOG_DIR}/server${SID}/output.log'"
    ;;

  # ── Kill all servers cleanly ───────────────────────────────────────────────
  killall)
    echo "Killing all servers..."
    for ((i=0; i<NUM_SERVERS; i++)); do
        ssh_kill $i || true
        sleep 0.3
    done
    echo "All servers killed."
    ;;

  # ── Status check ──────────────────────────────────────────────────────────
  status)
    echo "=============================================="
    echo "CLUSTER STATUS at $(date '+%H:%M:%S')"
    echo "=============================================="
    for ((i=0; i<NUM_SERVERS; i++)); do
        STATUS=$(is_alive $i)
        ROLE="follower"
        [ $i -eq 0 ] && ROLE="leader"
        printf "  Server %-2d (%s) %-12s → %s\n" $i $ROLE "${SERVER_IPS[$i]}" "$STATUS"
    done
    ;;

  *)
    echo "Usage: $0 {leader|follower <id>|f_of_n <f>|f_plus_leader <f>|restore <id>|killall|status}"
    echo ""
    echo "Examples:"
    echo "  $0 status                  # Check which servers are alive"
    echo "  $0 leader                  # Kill leader immediately"
    echo "  $0 leader 30               # Kill leader after 30s delay"
    echo "  $0 follower 2              # Kill server 2"
    echo "  $0 f_of_n 2                # Kill 2 random followers"
    echo "  $0 f_plus_leader 1         # Kill leader + 1 follower"
    echo "  $0 restore 0               # Restart server 0"
    echo "  $0 killall                 # Kill all servers"
    exit 1
    ;;
esac
