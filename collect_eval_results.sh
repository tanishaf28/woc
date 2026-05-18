#!/bin/bash
# ================================================================
# Result Collection and Organization Script
# Gathers results from all 4 evaluation runs and organizes them
# ================================================================

set -u

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
FIRST_SERVER="192.168.73.159"

# Create main results folder
RESULTS_FOLDER="./eval_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_FOLDER"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              EVALUATION RESULTS COLLECTION                     ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Destination: $RESULTS_FOLDER"
echo ""

# Function to collect and organize eval results
collect_eval_results() {
    local eval_name=$1
    local eval_dir="$RESULTS_FOLDER/EVAL_${eval_name}"
    local eval_csv_dir="$eval_dir/csv_data"
    mkdir -p "$eval_dir"
    mkdir -p "$eval_csv_dir"
    
    echo "Collecting EVAL $eval_name..."
    
    # Create metadata based on eval type
    case $eval_name in
        1)
            cat > "$eval_dir/METADATA.txt" << 'EOF'
EVALUATION 1: Independent vs Common Ratio
=========================================
Test Cases: 8
Runtime per test: 30s
Test configurations: 100/0, 90/10, 80/20, 60/40, 40/60, 20/80, 10/90, 0/100

Purpose: Analyze impact of workload composition on system performance
Metrics: Throughput, Latency (P50/P95/P99), Cache hits
EOF
            ;;
        2)
            cat > "$eval_dir/METADATA.txt" << 'EOF'
EVALUATION 2: Max Pipeline In-Flight
=====================================
Test Cases: 15
Runtime per test: 30s
Test configurations: 1, 2, 3, 4, 5, 8, 10, 15, 20, 25, 30, 35, 40, 45, 50

Purpose: Optimize request pipelining depth
Metrics: Throughput, Latency, Queue depth, CPU utilization
EOF
            ;;
        3)
            cat > "$eval_dir/METADATA.txt" << 'EOF'
EVALUATION 3: Fault Tolerance
==============================
Test Cases: 5
Runtime per test: 30s
Test scenarios: No failures, Node0 fails, Node1 fails, Node4 fails, Nodes 0+1 fail

Purpose: Test system resilience under node failures
Metrics: Failover time, Request drop rate, Recovery time
EOF
            ;;
        4)
            cat > "$eval_dir/METADATA.txt" << 'EOF'
EVALUATION 4: Network Delay Impact
===================================
Test Cases: 7
Runtime per test: 30s
Test configurations: 0ms, 5ms, 10ms, 20ms, 50ms, 100ms, 200ms

Purpose: Quantify latency impact on performance
Metrics: Throughput, Latency distribution, Consensus overhead
EOF
            ;;
    esac
    
    # Collect logs from first server
    echo "  Downloading logs from $FIRST_SERVER..."
    ssh -i $SSH_KEY $USER@$FIRST_SERVER bash -c "
    cd /home/ubuntu/woc/logs
    case $eval_name in
        1) tar -czf eval${eval_name}_logs.tar.gz server_*_indep_* client_*_indep_* 2>/dev/null || true ;;
        2) tar -czf eval${eval_name}_logs.tar.gz server_*_inflight_* client_*_inflight_* 2>/dev/null || true ;;
        3) tar -czf eval${eval_name}_logs.tar.gz server_*_node*_* client_*_node*_* 2>/dev/null || true ;;
        4) tar -czf eval${eval_name}_logs.tar.gz server_*_delay_* client_*_delay_* 2>/dev/null || true ;;
    esac
    " 2>/dev/null || true
    
    scp -i $SSH_KEY $USER@$FIRST_SERVER:/home/ubuntu/woc/logs/eval${eval_name}_logs.tar.gz "$eval_dir/" 2>/dev/null || true
    
    # Download CSV files organized by server/client
    echo "  Downloading CSV results by server/client..."
    
    # Create server/client subdirectories locally
    for id in 0 1 2 3 4; do
        mkdir -p "$eval_csv_dir/server${id}"
        scp -i $SSH_KEY $USER@$FIRST_SERVER:/home/ubuntu/woc/eval/server${id}/*.csv "$eval_csv_dir/server${id}/" 2>/dev/null || true
    done
    
    for id in 0 1; do
        mkdir -p "$eval_csv_dir/client${id}"
        scp -i $SSH_KEY $USER@$FIRST_SERVER:/home/ubuntu/woc/eval/client${id}/*.csv "$eval_csv_dir/client${id}/" 2>/dev/null || true
    done
    
    # Merge all server CSVs together
    echo "  Merging server results..."
    merged_servers="$eval_csv_dir/merged_servers.csv"
    > "$merged_servers"  # Clear file
    
    for id in 0 1 2 3 4; do
        server_dir="$eval_csv_dir/server${id}"
        if [ -d "$server_dir" ] && [ -n "$(ls "$server_dir"/*.csv 2>/dev/null)" ]; then
            for csv in "$server_dir"/*.csv; do
                if [ -f "$csv" ]; then
                    cat "$csv" >> "$merged_servers"
                fi
            done
        fi
    done
    
    # Merge all client CSVs together
    echo "  Merging client results..."
    merged_clients="$eval_csv_dir/merged_clients.csv"
    > "$merged_clients"  # Clear file
    
    for id in 0 1; do
        client_dir="$eval_csv_dir/client${id}"
        if [ -d "$client_dir" ] && [ -n "$(ls "$client_dir"/*.csv 2>/dev/null)" ]; then
            for csv in "$client_dir"/*.csv; do
                if [ -f "$csv" ]; then
                    cat "$csv" >> "$merged_clients"
                fi
            done
        fi
    done
    
    # Create summary of downloaded files
    echo "  Organizing CSV metadata..."
    cat > "$eval_dir/CSV_STRUCTURE.txt" << 'CSVEOF'
CSV Files Downloaded, Organized, and Merged:
==============================================

MERGED FILES (Ready for Analysis):
----------------------------------
- merged_servers.csv  (All server results combined)
- merged_clients.csv  (All client results combined)

ORGANIZED BY ROLE/INSTANCE:
----------------------------
CSVEOF
    
    for id in 0 1 2 3 4; do
        if [ -d "$eval_csv_dir/server${id}" ]; then
            count=$(ls "$eval_csv_dir/server${id}"/*.csv 2>/dev/null | wc -l)
            if [ $count -gt 0 ]; then
                echo "" >> "$eval_dir/CSV_STRUCTURE.txt"
                echo "server${id}/ - $count files" >> "$eval_dir/CSV_STRUCTURE.txt"
                ls -lh "$eval_csv_dir/server${id}"/*.csv 2>/dev/null | awk '{print "  " $9, "(" $5 ")"}' >> "$eval_dir/CSV_STRUCTURE.txt"
            fi
        fi
    done
    
    for id in 0 1; do
        if [ -d "$eval_csv_dir/client${id}" ]; then
            count=$(ls "$eval_csv_dir/client${id}"/*.csv 2>/dev/null | wc -l)
            if [ $count -gt 0 ]; then
                echo "" >> "$eval_dir/CSV_STRUCTURE.txt"
                echo "client${id}/ - $count files" >> "$eval_dir/CSV_STRUCTURE.txt"
                ls -lh "$eval_csv_dir/client${id}"/*.csv 2>/dev/null | awk '{print "  " $9, "(" $5 ")"}' >> "$eval_dir/CSV_STRUCTURE.txt"
            fi
        fi
    done
    
    echo "  ✓ EVAL $eval_name collected and organized"
}

# Collect all 4 evaluations
for eval_num in 1 2 3 4; do
    collect_eval_results "$eval_num"
done

# Create summary file
cat > "$RESULTS_FOLDER/SUMMARY.txt" << 'EOF'
╔════════════════════════════════════════════════════════════════╗
║          COMPREHENSIVE MONGODB WORKLOAD A EVALUATION            ║
║                    5-Node Heterogeneous Cluster                ║
║            (2 Strong c16 nodes + 3 Weak c4 nodes)              ║
╚════════════════════════════════════════════════════════════════╝

EVALUATION SUMMARY
==================

EVAL 1: Independent vs Common Ratio
   - 8 test cases (workload compositions)
   - Tests synchronization overhead and parallelism
   - Results in: EVAL_1/

EVAL 2: Max Pipeline In-Flight
   - 15 test cases (pipeline depths)
   - Optimizes request batching and throughput
   - Results in: EVAL_2/

EVAL 3: Fault Tolerance
   - 5 test scenarios (failure modes)
   - Validates Byzantine fault tolerance
   - Results in: EVAL_3/

EVAL 4: Network Delay Impact
   - 7 test cases (latency values 0-200ms)
   - Measures impact of network conditions
   - Results in: EVAL_4/

FOLDER STRUCTURE
================
eval_results_YYYYMMDD_HHMMSS/
├── SUMMARY.txt                 (this file)
├── EVAL_1/
│   ├── METADATA.txt            # Test configuration (8 cases)
│   ├── eval1_logs.tar.gz       # Compressed logs
│   ├── CSV_STRUCTURE.txt       # CSV files summary
│   └── csv_data/               # Downloaded CSV files organized by role
│       ├── server0/*.csv
│       ├── server1/*.csv
│       ├── server2/*.csv
│       ├── server3/*.csv
│       ├── server4/*.csv
│       ├── client0/*.csv
│       └── client1/*.csv
├── EVAL_2/
│   ├── METADATA.txt
│   ├── eval2_logs.tar.gz
│   ├── CSV_STRUCTURE.txt
│   └── csv_data/
│       ├── server0/*.csv ... server4/*.csv
│       ├── client0/*.csv ... client1/*.csv
├── EVAL_3/
│   ├── METADATA.txt
│   ├── eval3_logs.tar.gz
│   ├── CSV_STRUCTURE.txt
│   └── csv_data/
│       ├── server0/*.csv ... server4/*.csv
│       ├── client0/*.csv ... client1/*.csv
├── EVAL_4/
│   ├── METADATA.txt
│   ├── eval4_logs.tar.gz
│   ├── CSV_STRUCTURE.txt
│   └── csv_data/
│       ├── server0/*.csv ... server4/*.csv
│       ├── client0/*.csv ... client1/*.csv
└── SUMMARY.txt (this file)

HOW TO RETRIEVE ADDITIONAL DATA
===============================

1. CSV results are already downloaded and organized by role/instance:
   EVAL_*/csv_data/server0/, server1/, ..., client0/, client1/

2. Extract logs:
   tar -xzf EVAL_1/eval1_logs.tar.gz
   tar -xzf EVAL_2/eval2_logs.tar.gz
   tar -xzf EVAL_3/eval3_logs.tar.gz
   tar -xzf EVAL_4/eval4_logs.tar.gz

3. Analyze results per role:
   ls EVAL_1/csv_data/server0/*.csv    # Server 0 results
   ls EVAL_1/csv_data/client0/*.csv    # Client 0 results

4. Merge all evaluation results:
   # Copy all CSV files to one location
   mkdir merged_results
   for eval in EVAL_*/; do
     cp $eval/csv_data/*/*.csv merged_results/
   done
   
   # Use merge_eval.py (if available)
   python3 merge_eval.py --output merged_results.csv

ANALYSIS RECOMMENDATIONS
========================

1. Plot throughput vs workload composition (EVAL_1)
   - Compare server0/ vs server4/ (strong vs weak nodes)
   
2. Find optimal pipeline depth (EVAL_2)
   - Analyze latency curves across pipeline values
   
3. Compare failure scenarios (EVAL_3)
   - Baseline vs node failures
   
4. Analyze latency degradation curve (EVAL_4)
   - Throughput/latency vs network delay
   
5. Strong vs Weak node performance comparison:
   - Compare server0/server1/ (c16) vs server2/3/4/ (c4)

For file listings, check EVAL_*/CSV_STRUCTURE.txt
EOF

echo ""
echo "✓ Collection and organization complete!"
echo ""
echo "Results saved to: $RESULTS_FOLDER"
echo ""
echo "Folder structure (with merged CSV data):"
tree "$RESULTS_FOLDER" 2>/dev/null || find "$RESULTS_FOLDER" -type f | sed 's|[^/]*/| |g'
echo ""
echo "Merged CSV Files (ready for analysis):"
for eval_num in 1 2 3 4; do
    eval_dir="$RESULTS_FOLDER/EVAL_${eval_num}"
    if [ -d "$eval_dir/csv_data" ]; then
        echo ""
        echo "EVAL_$eval_num merged files:"
        [ -f "$eval_dir/csv_data/merged_servers.csv" ] && ls -lh "$eval_dir/csv_data/merged_servers.csv" | awk '{print "  ✓ merged_servers.csv (" $5 ")"}'
        [ -f "$eval_dir/csv_data/merged_clients.csv" ] && ls -lh "$eval_dir/csv_data/merged_clients.csv" | awk '{print "  ✓ merged_clients.csv (" $5 ")"}'
    fi
done
echo ""
echo "Individual server/client results:"
for eval_num in 1 2 3 4; do
    eval_dir="$RESULTS_FOLDER/EVAL_${eval_num}"
    if [ -d "$eval_dir/csv_data" ]; then
        echo ""
        echo "EVAL_$eval_num:"
        find "$eval_dir/csv_data" -type f -name "*.csv" ! -name "merged_*" | wc -l | xargs echo "  Individual CSV files:"
        ls -d "$eval_dir/csv_data"/*/ 2>/dev/null | while read dir; do
            count=$(ls "$dir"/*.csv 2>/dev/null | wc -l)
            echo "  $(basename $dir): $count files"
        done
    fi
done
echo ""
echo "Next steps:"
echo "1. Review EVAL_*/METADATA.txt for test configuration"
echo "2. Check EVAL_*/CSV_STRUCTURE.txt for file listing"
echo "3. Use merged_servers.csv and merged_clients.csv for analysis"
echo "4. Extract logs: tar -xzf EVAL_*/eval*_logs.tar.gz"
