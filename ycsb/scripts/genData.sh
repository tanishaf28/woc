#!/bin/bash

set -euo pipefail

YCSB_DIR=./YCSB
MVN_DIR=./apache-maven-3.5.4
LOCAL_BIN_DIR=./.ycsb-bin
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YCSB_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKDATA_DIR="$YCSB_ROOT/workData"

mkdir -p "$WORKDATA_DIR" "$LOCAL_BIN_DIR"

if [ ! -d "$YCSB_DIR" ]; then
    echo "YCSB does not exist, downloading..."
    git clone https://github.com/brianfrankcooper/YCSB.git
fi

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

ensure_java_home() {
    if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
        return 0
    fi

    if command -v javac >/dev/null 2>&1; then
        export JAVA_HOME
        JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")
        return 0
    fi

    if command -v java >/dev/null 2>&1; then
        export JAVA_HOME
        JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")
        if [ -x "$JAVA_HOME/bin/java" ]; then
            return 0
        fi
    fi

    echo "ERROR: Java runtime not found; please install a JDK or JRE"
    exit 1
}

ensure_maven() {
    if command -v mvn >/dev/null 2>&1; then
        return 0
    fi

    echo "mvn not found, downloading Maven 3.5.4..."
    if [ ! -d "$MVN_DIR" ]; then
        wget --no-check-certificate "https://archive.apache.org/dist/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz"
        tar -xvf apache-maven-3.5.4-bin.tar.gz
        rm -f apache-maven-3.5.4-bin.tar.gz
    fi
    export M2_HOME="$(pwd)/apache-maven-3.5.4"
    export PATH="$M2_HOME/bin:$PATH"
}

ensure_python_command
ensure_java_home
ensure_maven

MVN_VER=$(mvn -v 2>/dev/null | head -n 1)
echo "current maven: ${MVN_VER}"

if [ -d "../config" ];
then
    CONFIG_DIR=../config
elif [ -d "./config" ];
then
    CONFIG_DIR=config
else
    CONFIG_DIR=YCSB/workloads
fi

cd "$YCSB_DIR"
echo "generating loading workload"
./bin/ycsb load basic -P "../$CONFIG_DIR/workloada" > "$WORKDATA_DIR/workload.dat"
for i in a b c d e f
do
    echo "generating running workload$i..."
    ./bin/ycsb run basic -P "../$CONFIG_DIR/workload$i" > "$WORKDATA_DIR/run_workload$i.dat"
done
cd ..
