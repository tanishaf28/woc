#!/bin/bash
# Wrapper to stop MongoDB hetero runs using existing hetero shutdown logic.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

bash ./stop_cluster_hetero.sh
