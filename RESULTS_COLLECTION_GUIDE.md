# Evaluation Results Guide

## Quick Answer

**YES** - The evaluation scripts now collect, label, and organize all results:

1. **Automated collection** - `collect_eval_results.sh` script organizes all 4 evals
2. **Labeled folders** - Each evaluation in its own `EVAL_1/`, `EVAL_2/`, `EVAL_3/`, `EVAL_4/` folder
3. **Metadata files** - Each folder has `METADATA.txt` with test configuration
4. **Auto-triggered** - `run_all_evals.sh` automatically runs collection at end

---

## Result Organization Structure

```
eval_results_20260512_143025/
├── SUMMARY.txt                # Overview of all evaluations
├── EVAL_1/                    # Independent vs Common Ratio
│   ├── METADATA.txt           # Test configuration (8 tests)
│   ├── eval1_logs.tar.gz      # Compressed logs
│   ├── CSV_STRUCTURE.txt      # Summary of downloaded CSV files
│   └── csv_data/              # ✓ CSV files downloaded & organized
│       ├── server0/*.csv
│       ├── server1/*.csv
│       ├── server2/*.csv
│       ├── server3/*.csv
│       ├── server4/*.csv
│       ├── client0/*.csv
│       └── client1/*.csv
├── EVAL_2/                    # Max Pipeline In-Flight
│   ├── METADATA.txt
│   ├── eval2_logs.tar.gz
│   ├── CSV_STRUCTURE.txt
│   └── csv_data/              # ✓ CSV files organized by role
│       ├── server0/ ... server4/
│       ├── client0/ ... client1/
├── EVAL_3/                    # Fault Tolerance
│   ├── METADATA.txt
│   ├── eval3_logs.tar.gz
│   ├── CSV_STRUCTURE.txt
│   └── csv_data/              # ✓ CSV files organized by role
│       ├── server0/ ... server4/
│       ├── client0/ ... client1/
└── EVAL_4/                    # Network Delay
    ├── METADATA.txt
    ├── eval4_logs.tar.gz
    ├── CSV_STRUCTURE.txt
    └── csv_data/              # ✓ CSV files organized by role
        ├── server0/ ... server4/
        ├── client0/ ... client1/
```

---

## Workflow

### Step 1: Run All Evaluations
```bash
bash run_all_evals.sh
# Takes ~6-8 hours
# Auto-runs collect_eval_results.sh at end
```

### Step 2: Results Automatically Organized
```
eval_results_YYYYMMDD_HHMMSS/
├── EVAL_1/  ← Each eval in labeled folder
├── EVAL_2/
├── EVAL_3/
├── EVAL_4/
└── SUMMARY.txt
```

### Step 3: Extract and Analyze

**CSV files are already downloaded and organized!**

```bash
# View downloaded CSV structure
cat eval_results_20260512_143025/EVAL_1/CSV_STRUCTURE.txt

# List results by server
ls -la eval_results_20260512_143025/EVAL_1/csv_data/server0/*.csv

# List results by client
ls -la eval_results_20260512_143025/EVAL_1/csv_data/client0/*.csv
```

**Extract logs from an evaluation:**
```bash
cd eval_results_20260512_143025/EVAL_1
tar -xzf eval1_logs.tar.gz
ls -lh server_*_indep_*.log  # View labeled logs
```

**Analyze results per role (strong vs weak nodes):**
```bash
# Compare server 0/1 (strong c16) vs server 4 (weak c4)
cat eval_results_20260512_143025/EVAL_1/csv_data/server0/*.csv | head -5
cat eval_results_20260512_143025/EVAL_1/csv_data/server4/*.csv | head -5
```

**Merge all evaluation CSVs:**
```bash
# Copy all CSV files to one folder
mkdir eval_results_20260512_143025/all_merged
cat eval_results_20260512_143025/EVAL_1/csv_data/*/*.csv > eval_results_20260512_143025/all_merged/eval1.csv
cat eval_results_20260512_143025/EVAL_2/csv_data/*/*.csv > eval_results_20260512_143025/all_merged/eval2.csv
cat eval_results_20260512_143025/EVAL_3/csv_data/*/*.csv > eval_results_20260512_143025/all_merged/eval3.csv
cat eval_results_20260512_143025/EVAL_4/csv_data/*/*.csv > eval_results_20260512_143025/all_merged/eval4.csv
```

---

## Manual Collection (if needed)

```bash
# Collect results manually
bash collect_eval_results.sh

# This creates:
# - eval_results_YYYYMMDD_HHMMSS/
# - Organized by EVAL_1, EVAL_2, EVAL_3, EVAL_4
# - Metadata files with test configs
# - Compressed logs from each evaluation
```

---

## Log Files Labeling

Logs are labeled with evaluation parameters:

| Log File | Test Config |
|----------|-------------|
| `server_0_indep_100.0_common_0.0.log` | EVAL 1: 100% independent |
| `server_0_inflight_10.log` | EVAL 2: Max inflight = 10 |
| `server_0_no_failure.log` | EVAL 3: No failures |
| `server_0_delay_50ms.log` | EVAL 4: 50ms network delay |

---

## Files Included

Each `EVAL_*/` folder contains:

1. **METADATA.txt**
   - Test case descriptions
   - Number of tests and runtime
   - Test configurations
   - Purpose and metrics measured

2. **csv_data/** (NEW! ✓ Auto-downloaded)
   - **server0/, server1/, server2/, server3/, server4/** - CSV results from each server
   - **client0/, client1/** - CSV results from each client
   - Ready for immediate analysis

3. **eval*_logs.tar.gz**
   - All server logs for that evaluation
   - All client logs for that evaluation
   - Organized with descriptive filenames

4. **CSV_STRUCTURE.txt** (NEW! ✓ Auto-generated)
   - Summary of downloaded CSV files
   - File counts by server/client
   - Quick reference for what was collected

---

## CSV Results Location

✓ **CSV files are automatically downloaded and organized!**

After evaluations complete:
```
eval_results_YYYYMMDD_HHMMSS/EVAL_1/csv_data/
├── server0/  # All CSV results from server 0
├── server1/  # All CSV results from server 1
├── server2/  # All CSV results from server 2
├── server3/  # All CSV results from server 3
├── server4/  # All CSV results from server 4
├── client0/  # All CSV results from client 0
└── client1/  # All CSV results from client 1
```

No manual download needed! Results are ready to analyze immediately.

---

## Analysis

### EVAL 1: Workload Composition
```bash
cd EVAL_1
tar -xzf eval1_logs.tar.gz
grep "throughput" server_0_indep_*.log
# Analyze: 100/0 vs 0/100 workload impact
```

### EVAL 2: Pipeline Optimization
```bash
cd EVAL_2
tar -xzf eval2_logs.tar.gz
# Compare latencies across pipeline depths
grep "P99" server_0_inflight_*.log
```

### EVAL 3: Fault Tolerance
```bash
cd EVAL_3
tar -xzf eval3_logs.tar.gz
# Compare baseline vs failure scenarios
diff <(grep "throughput" server_0_no_failure.log) \
     <(grep "throughput" server_0_node0_fails.log)
```

### EVAL 4: Network Impact
```bash
cd EVAL_4
tar -xzf eval4_logs.tar.gz
# Plot latency vs delay
for f in server_0_delay_*.log; do
  delay=$(echo $f | sed 's/.*delay_//;s/ms.*//')
  latency=$(grep "P99" $f | awk '{print $NF}')
  echo "$delay, $latency"
done
```

---

## Summary

✓ **Automated collection & organization** - Results labeled with test parameters and downloaded
✓ **Organized by role/instance** - Each eval in separate folder with server0-4/ and client0-1/ subdirs
✓ **CSV files included** - No manual download needed, all CSV data auto-organized
✓ **Metadata documented** - Test configuration saved in METADATA.txt
✓ **Auto-triggered** - `run_all_evals.sh` automatically runs collection at end
✓ **Ready to analyze** - CSV files organized like merge_eval.py structure
