#!/bin/bash
# Compatibility wrapper for the fixed 5-node heterogeneous MongoDB stopper.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

bash ./stop_mongodb_hetero_5n.sh "$@"
