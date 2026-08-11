#!/bin/bash
# ================================================================
# N=5 SMOKE TEST — indep ratio sweep (eval1) + batch size sweep (eval2),
# once with fixed clients (SCALE_CLIENTS=false, FIXED_CLIENT_COUNT=3) and
# once with scale clients (SCALE_CLIENTS=true, 5 pinned clients).
#
# Wraps run_plainmsg_evals.sh with CLUSTER_SIZES="5" so the full n=3/7/11
# sweep is skipped. Use this to sanity-check the cluster + client pinning
# on a single cluster size before committing to the full sweep.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_SCRIPT="${SCRIPT_DIR}/run_plainmsg_evals.sh"

export CLUSTER_SIZES="5"
export RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"

echo "################################################################"
echo "# WOC N=5 test — FIXED clients (FIXED_CLIENT_COUNT=3)"
echo "################################################################"
SCALE_CLIENTS=false FIXED_CLIENT_COUNT=3 bash "$RUN_SCRIPT" eval1
SCALE_CLIENTS=false FIXED_CLIENT_COUNT=3 bash "$RUN_SCRIPT" eval2

echo "################################################################"
echo "# WOC N=5 test — SCALE clients (5 pinned clients)"
echo "################################################################"
SCALE_CLIENTS=true bash "$RUN_SCRIPT" eval1
SCALE_CLIENTS=true bash "$RUN_SCRIPT" eval2

echo ""
echo "N=5 test complete. Results under: ${SCRIPT_DIR}/results/plainmsg_evals/<timestamp>/n5/"
