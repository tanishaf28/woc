#!/bin/bash
# Compatibility wrapper for the fixed 5-node heterogeneous MongoDB launcher.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

bash ./start_mongodb_hetero_5n.sh "$@"
