#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
KEY_SRC="${HOME}/.ssh/tani.pem"
REMOTE_USER="${REMOTE_USER:-ubuntu}"

CONFIG_FILES=(
  "${REPO_ROOT}/config/cluster_hetero.conf"
  "${REPO_ROOT}/config/cluster_homo.conf"
)

if [[ ! -f "${KEY_SRC}" ]]; then
  echo "Source key not found: ${KEY_SRC}" >&2
  exit 1
fi

chmod 600 "${KEY_SRC}" || true

declare -A seen_hosts=()
hosts=()

add_hosts_from_config() {
  local config_file="$1"

  if [[ ! -f "${config_file}" ]]; then
    echo "Skipping missing config file: ${config_file}" >&2
    return
  fi

  while read -r line; do
    [[ -z "${line}" ]] && continue
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue

    read -r _ host _ _ <<< "${line}"
    if [[ -n "${host}" && -z "${seen_hosts[${host}]+x}" ]]; then
      seen_hosts["${host}"]=1
      hosts+=("${host}")
    fi
  done < "${config_file}"
}

for config_file in "${CONFIG_FILES[@]}"; do
  add_hosts_from_config "${config_file}"
done

if [[ ${#hosts[@]} -eq 0 ]]; then
  echo "No hosts found in config files." >&2
  exit 1
fi

echo "Copying ${KEY_SRC} to ${#hosts[@]} VMs..."

for host in "${hosts[@]}"; do
  echo "[${host}] preparing ~/.ssh"
  ssh \
    -i "${KEY_SRC}" \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=accept-new \
    -o ConnectTimeout=10 \
    "${REMOTE_USER}@${host}" \
    'mkdir -p ~/.ssh && chmod 700 ~/.ssh'

  echo "[${host}] copying tani.pem"
  scp \
    -i "${KEY_SRC}" \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=accept-new \
    -o ConnectTimeout=10 \
    "${KEY_SRC}" \
    "${REMOTE_USER}@${host}:~/.ssh/tani.pem"

  ssh \
    -i "${KEY_SRC}" \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=accept-new \
    -o ConnectTimeout=10 \
    "${REMOTE_USER}@${host}" \
    'chmod 600 ~/.ssh/tani.pem'
done

echo "Done. The key is now present on all listed VMs."