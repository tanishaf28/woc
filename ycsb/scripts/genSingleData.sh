#!/bin/bash

set -euo pipefail

LOCAL_BIN_DIR=./.ycsb-bin
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YCSB_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKDATA_DIR="$YCSB_ROOT/workData"

mkdir -p "$WORKDATA_DIR" "$LOCAL_BIN_DIR"

while getopts :f:r:o: flag; do
    case $flag in
        f) file=${OPTARG} ;;
        r) rc=${OPTARG} ;;
        o) oc=${OPTARG} ;;
    esac
done

ensure_python_command() {
    if command -v python >/dev/null 2>&1; then
        return 0
    fi

    if command -v python3 >/dev/null 2>&1; then
        cat > "$LOCAL_BIN_DIR/python" <<'EOF'
#!/bin/sh
exec python3 "$@"
EOF
        chmod +x "$LOCAL_BIN_DIR/python"
        export PATH="$(pwd)/$LOCAL_BIN_DIR:$PATH"
        return 0
    fi

    echo "ERROR: neither python nor python3 is available"
    exit 1
}

ensure_python_command

cd ./YCSB
echo "generating load data..."
./bin/ycsb load basic -P ../config/workload$file \
            -p recordcount=$rc \
            > "$WORKDATA_DIR/workload.dat"

echo "generating run data..."
./bin/ycsb run basic -P ../config/workload$file \
            -p recordcount=$rc \
            -p operationcount=$oc \
            > "$WORKDATA_DIR/run_workload$file.dat"
cd ..