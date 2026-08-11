#!/bin/bash
# ================================================================
# MongoDB Smoke Test (WOC)
#
# A fast, minimal sanity check that the whole MongoDB pipeline actually
# works end-to-end - NOT a performance eval. Run this before committing to
# any of the multi-hour ratio/threshold/batch/delay/workload sweeps, to
# catch fundamental breakage cheaply (wrong config path, mongod failing to
# start, WOC never connecting to Mongo, zero documents actually written,
# etc.) in under a minute instead of discovering it 20 cases into a sweep.
#
# What it checks, beyond "the script didn't crash":
#   1. mongod is actually reachable on every server (mongosh ping).
#   2. The WOC binary's MongoDB follower actually initialized (server logs
#      free of "mongodb follower not initialized"/"failed to apply" errors).
#   3. At least one document was actually written to each server's local
#      MongoDB (queried directly via mongosh, not inferred from client-
#      reported throughput - see service.go's writesToApply/handleReadBatch
#      fixes this session, which were specifically about writes/reads being
#      silently dropped while still being reported as successful).
#   4. The client actually reported nonzero successful commits.
#
# Uses the smallest/fastest cluster (n=3) and a short runtime (15s) - this
# is deliberately not a real workload measurement.
# ================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_mongodb_hetero_nsel.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_mongodb_hetero_nsel.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/woc"

NUM_SERVERS=3
RUNTIME_SECONDS="${RUNTIME_SECONDS:-15}"
WORKLOAD="${WORKLOAD:-a}"
CLUSTER_ACTIVE=false
FAILURES=0

ssh_q() {
    local host=$1
    shift
    ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no -i "$SSH_KEY" "$USER@$host" "$*"
}

fail() {
    echo "  [FAIL] $1"
    FAILURES=$((FAILURES + 1))
}

pass() {
    echo "  [ OK ] $1"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        echo ""
        echo "Cleaning up test cluster..."
        NUM_SERVERS="$NUM_SERVERS" bash "$STOP_SCRIPT" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              WOC MONGODB SMOKE TEST (n=3, ~30s total)            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

echo "Starting minimal cluster (n=${NUM_SERVERS}, workload=${WORKLOAD}, runtime=${RUNTIME_SECONDS}s)..."
CLUSTER_ACTIVE=true
if ! NUM_SERVERS="$NUM_SERVERS" NUM_CLIENTS=2 INDEP_RATIO=90.0 BATCHSIZE=1 NUM_OBJECTS=100 READ_RATIO=0.0 \
    bash "$START_SCRIPT" "$WORKLOAD"; then
    fail "start_mongodb_hetero_nsel.sh exited non-zero - cluster may not have come up at all"
fi

echo ""
echo "Letting the cluster run for ${RUNTIME_SECONDS}s..."
sleep "$RUNTIME_SECONDS"

echo ""
echo "=================================================="
echo " CHECK 1: mongod reachable on every server"
echo "=================================================="
mapfile -t SERVER_IPS < <(awk 'NF >= 2 { print $2 }' "${REPO_ROOT}/config/cluster_hetero_3n_2s_1w.conf" | head -n "$NUM_SERVERS")
for i in "${!SERVER_IPS[@]}"; do
    ip="${SERVER_IPS[$i]}"
    if ssh_q "$ip" "mongosh --quiet --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1"; then
        pass "server${i} (${ip}): mongod responds to ping"
    else
        fail "server${i} (${ip}): mongod did NOT respond to ping"
    fi
done

echo ""
echo "=================================================="
echo " CHECK 2: server logs free of MongoDB connection/apply errors"
echo "=================================================="
for i in "${!SERVER_IPS[@]}"; do
    ip="${SERVER_IPS[$i]}"
    err_count=$(ssh_q "$ip" "grep -Ec 'mongodb follower not initialized|failed to apply|MongoDB (follower|leader) execution failed' '${REMOTE_DIR}/logs/server_${i}.log' 2>/dev/null || true")
    err_count=$(echo "$err_count" | tr -d ' \n')
    if [ "${err_count:-0}" -eq 0 ] 2>/dev/null; then
        pass "server${i}: no MongoDB error strings in log"
    else
        fail "server${i}: found ${err_count} MongoDB error line(s) in ${REMOTE_DIR}/logs/server_${i}.log"
    fi
done

echo ""
echo "=================================================="
echo " CHECK 3: documents actually present in each server's local MongoDB"
echo "=================================================="
for i in "${!SERVER_IPS[@]}"; do
    ip="${SERVER_IPS[$i]}"
    # dbName convention from conns.go's initMongoDB: WOC branches on
    # mode==Localhost (per-server "ycsb<serverID>") vs else (dbID hardcoded
    # to 0). Every eval script here passes -mode=1 (Distributed), so every
    # server's *own local* mongod (separate VM, separate process, separate
    # disk - no replica set anywhere in this architecture) uses the same
    # logical name "ycsb". These are N independent, physically isolated
    # databases that happen to share a name, not one shared database - the
    # name coinciding doesn't create any cross-server collision, since each
    # server never sees another server's data regardless of naming. Cabinet
    # instead suffixes by server ID always (see the Cabinet smoke test) -
    # a cosmetic difference in that repo's convention, not a correctness
    # requirement either way, given the physical isolation already holds.
    dbname="ycsb"
    count=$(ssh_q "$ip" "mongosh --quiet '${dbname}' --eval 'db.usertable.countDocuments({})' 2>/dev/null" | tr -dc '0-9')
    if [ -n "$count" ] && [ "$count" -gt 0 ] 2>/dev/null; then
        pass "server${i} (db=${dbname}): usertable has ${count} document(s)"
    else
        fail "server${i} (db=${dbname}): usertable has 0 documents (or query failed)"
    fi
done

echo ""
echo "=================================================="
echo " CHECK 4: client(s) show logged batch activity"
echo "=================================================="
# Broad "[Client N] Batch" prefix, not a full line match: sequential mode
# logs "Batch %d | size=..." every op, pipelined mode (this script's
# default) logs "Batch %d | limit=... | fast=...%% | size=..." only every
# 100th batch (clockVal%100==0) - a full-line pattern tuned for one mode
# would false-negative on the other, and a short smoke-test run may not
# even reach 100 batches in pipelined mode. This is a fast pre-check only;
# CHECK 5 below (the actual merged throughput CSV) is the authoritative one.
mapfile -t CLIENT_IPS < <(awk 'NF >= 2 { print $2 }' "${REPO_ROOT}/config/cluster_hetero_3n_2s_1w.conf" | tail -n +$((NUM_SERVERS + 1)))
for i in "${!CLIENT_IPS[@]}"; do
    ip="${CLIENT_IPS[$i]}"
    client_id=$((NUM_SERVERS + i))
    if ssh_q "$ip" "grep -Eq '\[Client [0-9]+\] Batch' '${REMOTE_DIR}/logs/client_${i}.log' 2>/dev/null"; then
        pass "client${client_id}: batch activity logged"
    else
        echo "  [WARN] client${client_id}: no batch log lines yet (normal in pipelined mode for a short run) - deferring to CHECK 5"
    fi
done

echo ""
echo "Stopping cluster and collecting/merging results..."
NUM_SERVERS="$NUM_SERVERS" bash "$STOP_SCRIPT" >/dev/null 2>&1 || true
CLUSTER_ACTIVE=false

echo ""
echo "=================================================="
echo " CHECK 5: merged client CSV has nonzero data rows"
echo "=================================================="
merged_dir="${SCRIPT_DIR}/eval/merged"
found_data=false
if [ -d "$merged_dir" ]; then
    for f in "$merged_dir"/*.csv; do
        [ -e "$f" ] || continue
        lines=$(wc -l < "$f" | tr -d ' ')
        if [ "$lines" -gt 1 ]; then
            pass "$(basename "$f"): ${lines} lines (header + data)"
            found_data=true
        else
            echo "  [WARN] $(basename "$f"): only ${lines} line(s) (header only, no data rows)"
        fi
    done
fi
if [ "$found_data" = false ]; then
    fail "no merged CSV with actual data rows found in ${merged_dir}"
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
if [ "$FAILURES" -eq 0 ]; then
    echo "║  RESULT: PASS - MongoDB pipeline looks functional                ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    exit 0
else
    echo "║  RESULT: FAIL - ${FAILURES} check(s) failed, see above              ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    exit 1
fi
