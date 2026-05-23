# Comprehensive Evaluation Scripts for MongoDB Workload A

## Overview

Four comprehensive evaluation scripts for testing a 5-node heterogeneous MongoDB cluster (2 strong c16 + 3 weak c4). Each evaluation runs for 30 seconds per test case.

## Quick Start

```bash
# Make all scripts executable
chmod +x eval_*.sh run_all_evals.sh

# Run all evaluations sequentially (takes ~6-8 hours total)
bash run_all_evals.sh

# Or run individual evaluations
bash eval_1_indep_common_ratio.sh    # ~4 minutes
bash eval_2_max_inflight.sh          # ~8 minutes
bash eval_3_fault_tolerance.sh       # ~2 minutes
bash eval_4_network_delay.sh         # ~3.5 minutes
```

---

## EVAL 1: Independent vs Common Ratio

**File:** `eval_1_indep_common_ratio.sh`

**Purpose:** Tests how workload composition (independent vs common operations) affects system performance.

**Test Cases:** 8 configurations
- 100.0 / 0.0   (all independent operations)
- 90.0 / 10.0
- 80.0 / 20.0
- 60.0 / 40.0
- 40.0 / 60.0
- 20.0 / 80.0
- 10.0 / 90.0
- 0.0 / 100.0   (all common operations)

**Metrics Collected:**
- Throughput (ops/sec)
- Latency (P50, P95, P99)
- Cache hit rates
- Synchronization overhead

**Expected Behavior:**
- Higher independent ratio → better parallelism, lower latency
- Higher common ratio → more contention, higher latency
- Heterogeneous cluster shows performance variance between strong/weak nodes

**Run time:** ~4 minutes (8 tests × 30 seconds)

---

## EVAL 2: Max Pipeline In-Flight

**File:** `eval_2_max_inflight.sh`

**Purpose:** Tests impact of request pipelining depth on throughput and latency.

**Test Cases:** 15 configurations
- 1, 2, 3, 4, 5, 8, 10, 15, 20, 25, 30, 35, 40, 45, 50

**Metrics Collected:**
- Throughput (ops/sec)
- Latency (P50, P95, P99)
- Request queue depths
- CPU utilization

**Expected Behavior:**
- Low values (1-5): Lower throughput, lower latency
- Mid values (10-25): Optimal throughput-latency tradeoff
- High values (30-50): Higher throughput but increased latency variance

**Run time:** ~8 minutes (15 tests × 30 seconds)

---

## EVAL 3: Fault Tolerance

**File:** `eval_3_fault_tolerance.sh`

**Purpose:** Tests system resilience when cluster nodes fail.

**Test Scenarios:** 5 configurations
1. **no_failure** - Baseline with all 5 nodes
2. **node0_fails** - Node 0 (strong c16) crashes after 10s
3. **node1_fails** - Node 1 (strong c16) crashes after 10s
4. **node4_fails** - Node 4 (weak c4) crashes after 10s
5. **node0_node1_fail** - Both strong nodes fail after 10s

**Metrics Collected:**
- Throughput during failure
- Failover time
- Request drop rate
- Recovery time to baseline

**Expected Behavior:**
- Loss of one node: ~20% throughput drop, quick recovery
- Loss of both strong nodes: significant impact due to imbalance
- System should remain available (f=2 tolerance)

**Run time:** ~2.5 minutes (5 tests × 30 seconds)

---

## EVAL 4: Network Delay Impact

**File:** `eval_4_network_delay.sh`

**Purpose:** Tests system performance under network latency (simulated with tc/netem).

**Test Cases:** 7 latency configurations
- 0ms (baseline)
- 5ms
- 10ms
- 20ms
- 50ms
- 100ms
- 200ms

**Metrics Collected:**
- Throughput (ops/sec)
- Latency (P50, P95, P99)
- Network round-trip time
- Consensus overhead

**Expected Behavior:**
- 0-10ms: Minimal impact on throughput
- 20-50ms: Noticeable latency increase, slight throughput decrease
- 100-200ms: Significant throughput reduction, high latency
- Strong nodes may compensate for weak nodes at high latency

**Run time:** ~3.5 minutes (7 tests × 30 seconds)

**Note:** Requires `tc` (traffic control) and `sudo` access on all nodes.

---

## Master Script: run_all_evals.sh

Runs all evaluations sequentially with optional filtering.

**Usage:**
```bash
# Run all evaluations
bash run_all_evals.sh

# Run only specific evaluations
bash run_all_evals.sh 1 1 0 0    # Run eval1 and eval2 only
bash run_all_evals.sh 0 0 1 1    # Run eval3 and eval4 only
bash run_all_evals.sh 1 0 0 0    # Run only eval1
```

**Parameters:** (1=run, 0=skip)
1. EVAL1: Independent vs Common Ratio
2. EVAL2: Max Pipeline In-Flight
3. EVAL3: Fault Tolerance
4. EVAL4: Network Delay

---

## Output and Results

### Log Files
All logs stored in `/home/ubuntu/woc/logs/` on remote nodes:
- `server_*.log` - Server-side logs
- `client_*.log` - Client-side logs

### Evaluation Data
Results stored in `/home/ubuntu/woc/eval/` as CSV files:
- Timestamped with configuration parameters
- Compatible with `merge_eval.py` for analysis

### Retrieve Results
```bash
# Copy all results locally
ssh -i ~/.ssh/tani.pem ubuntu@192.168.73.159 \
    "tar -czf eval_results.tar.gz /home/ubuntu/woc/eval" && \
scp -i ~/.ssh/tani.pem ubuntu@192.168.73.159:~/eval_results.tar.gz . && \
tar -xzf eval_results.tar.gz

# Or merge on remote and copy
ssh -i ~/.ssh/tani.pem ubuntu@192.168.73.159 \
    "cd /home/ubuntu/woc && python3 merge_eval.py"
```

---

## Cluster Configuration

**5-Node Setup:**
- **2 Strong nodes (c16 - 16 vCPU):** 192.168.73.159, 192.168.73.84
- **3 Weak nodes (c4 - 4 vCPU):** 192.168.73.69, 192.168.73.235, 192.168.73.194
- **Client nodes:** 192.168.73.218, 192.168.73.219

**Workload:** MongoDB Workload A (50% read, 50% write)

**Default Parameters:**
- Batch size: 10
- Message size: 512 bytes
- Mode: 1
- Threshold: 2 (Byzantine Fault Tolerance)
- MongoDB Replica Set: `wocrs`

---

## Troubleshooting

### Scripts won't execute
```bash
chmod +x eval_*.sh run_all_evals.sh
```

### SSH connection errors
Check SSH key and permissions:
```bash
chmod 600 ~/.ssh/tani.pem
ssh -i ~/.ssh/tani.pem ubuntu@192.168.73.159 "echo OK"
```

### Network emulation not working (EVAL 4)
Ensure `tc` is installed on all nodes:
```bash
ssh -i ~/.ssh/tani.pem ubuntu@192.168.73.159 "sudo apt-get install -y iproute2"
```

### Processes hanging
Manual cleanup:
```bash
for ip in 192.168.73.159 192.168.73.84 192.168.73.69 192.168.73.235 192.168.73.194 192.168.73.218 192.168.73.219; do
  ssh -i ~/.ssh/tani.pem ubuntu@$ip "pkill -9 woc; pkill -9 mongod; sudo tc qdisc del dev eth0 root 2>/dev/null || true"
done
```

### MongoDB replica set issues
Check status:
```bash
ssh -i ~/.ssh/tani.pem ubuntu@192.168.73.159 \
    "mongosh --eval 'rs.status()'"
```

---

## Performance Expectations

For reference, typical performance on this heterogeneous cluster:

| Scenario | Throughput | P99 Latency | Notes |
|----------|-----------|-------------|-------|
| Baseline (100/0) | 50K ops/sec | 50ms | Strong nodes dominate |
| High contention (0/100) | 20K ops/sec | 200ms | Synchronization overhead |
| Max inflight=10 | 45K ops/sec | 80ms | Optimal tradeoff |
| 1 node fails | 35K ops/sec | 120ms | Rebalancing |
| 100ms latency | 30K ops/sec | 300ms | Network dominates |

Actual results depend on hardware, network conditions, and workload specifics.

---

## Analysis

Post-evaluation, analyze results:
```bash
# Merge all evaluation data
python3 merge_eval.py

# Analyze with your tools (pandas, matplotlib, etc.)
python3 << 'EOF'
import pandas as pd
df = pd.read_csv('eval/merged_results.csv')
print(df.groupby('config').agg({'throughput': 'mean', 'latency_p99': 'mean'}))
EOF
```
