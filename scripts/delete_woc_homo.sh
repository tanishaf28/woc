#!/usr/bin/env bash

set -euo pipefail

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"

ALL_SERVER_IPS=(
  "192.168.73.220"
  "192.168.73.240"
  "192.168.73.108"
  "192.168.73.179"
  "192.168.73.154"
)

ALL_CLIENT_IPS=(
  "192.168.73.45"
  "192.168.73.229"
)

TARGET_IPS=("${ALL_SERVER_IPS[@]}" "${ALL_CLIENT_IPS[@]}")

if [[ "${CONFIRM_DELETE:-}" != "YES" ]]; then
  echo "Refusing to delete without confirmation."
  echo "Run: CONFIRM_DELETE=YES ./delete_woc_homo.sh"
  exit 1
fi

echo "=================================================="
echo " WOC Homogeneous Cluster Cleanup"
echo "=================================================="
echo "This will remove ${REMOTE_DIR} from ${#TARGET_IPS[@]} nodes."
echo ""

stop_woc_on_node() {
  local ip="$1"

  echo "→ Stopping WOC on ${ip}"
  ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
    "${USER}@${ip}" \
    "pkill -TERM -x woc 2>/dev/null || true"

  sleep 1

  ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
    "${USER}@${ip}" \
    "pkill -9 -x woc 2>/dev/null || true"
}

delete_woc_on_node() {
  local ip="$1"

  echo "→ Deleting ${REMOTE_DIR} on ${ip}"
  ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
    "${USER}@${ip}" \
    "rm -rf '${REMOTE_DIR}'"
}

for ip in "${TARGET_IPS[@]}"; do
  stop_woc_on_node "$ip"
done

echo ""
echo "Stopping complete. Removing directories now..."
echo ""

for ip in "${TARGET_IPS[@]}"; do
  delete_woc_on_node "$ip"
done

echo ""
echo "Cleanup complete."