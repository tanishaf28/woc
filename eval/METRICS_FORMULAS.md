# Performance Metrics Formulas

## Per-Batch Metrics (Each Row in CSV)

### 1. Latency (ms) per Batch
```
latency = TimeElapsed (in milliseconds)
```
- **Example**: 50 ms
- **Meaning**: Time to complete one batch of operations

### 2. Throughput (Tx/sec) per Batch
```
throughput = (batchSize / latency_ms) × 1000
```
- **Example**: `(100 ops / 50 ms) × 1000 = 2,000 Tx/sec`
- **Meaning**: Operations per second for this specific batch
- **Why × 1000**: Converts from "ops per millisecond" to "ops per second"

---

## Summary Metrics (At End of CSV)

### 1. AVERAGE - Average Latency
```
avgLatency = sum(all latencies) / number_of_batches
```
- **Example**: `(50 + 45 + 55 + 48) / 4 = 49.5 ms`
- **Meaning**: Mean latency across all batches

### 2. AVERAGE - Average Batch Throughput
```
avgBatchThroughput = sum(all batch throughputs) / number_of_batches
```
- **Example**: `(2000 + 2222 + 1818 + 2083) / 4 = 2,030.75 Tx/sec`
- **Meaning**: Average of individual batch throughputs
- **Use Case**: Shows typical per-batch performance

### 3. OVERALL_THROUGHPUT - Overall System Throughput
```
overallThroughput = (batchSize × number_of_batches) / total_elapsed_time
```
- **Calculation**:
  - `total_ops = 100 × 100 = 10,000 operations`
  - `total_time = last_batch_end - first_batch_start = 5.2 seconds`
  - `throughput = 10,000 / 5.2 = 1,923 Tx/sec`
- **Meaning**: System-wide throughput including gaps between batches
- **Use Case**: Real-world sustained throughput

### 4. P50_LATENCY (Median)
```
p50 = latencies[50th percentile]
```
- **Example**: Sort all latencies, pick the middle value
- **Meaning**: 50% of batches completed faster than this

### 5. P95_LATENCY
```
p95 = latencies[95th percentile]
```
- **Meaning**: 95% of batches completed faster than this
- **Use Case**: Captures most tail latencies

### 6. P99_LATENCY
```
p99 = latencies[99th percentile]
```
- **Meaning**: 99% of batches completed faster than this
- **Use Case**: Captures extreme tail latencies

### 7. AVG_LATENCY_PER_TX
```
avgLatencyPerTx = avgLatency / batchSize
```
- **Example**: `50 ms / 100 ops = 0.5 ms/Tx`
- **Meaning**: Average time per individual transaction

### 8. AVG_FAST_PATH_LATENCY
```
avgFastPathLatency = sum(fast path batch latencies) / count(fast path batches)
```
- **Example**: Only batches where FastPathCount > SlowPathCount
- **Meaning**: Average latency for fast path batches

### 9. AVG_SLOW_PATH_LATENCY
```
avgSlowPathLatency = sum(slow path batch latencies) / count(slow path batches)
```
- **Example**: Only batches where SlowPathCount > 0
- **Meaning**: Average latency for slow path batches

### 10. GLOBAL_TOTALS
```
FastCommits   = total number of fast path OPERATIONS across all batches
SlowCommits   = total number of slow path OPERATIONS across all batches
ConflictCommits = total number of conflict OPERATIONS across all batches
```
- **Validation**: FastCommits + SlowCommits + ConflictCommits should equal total operations
- **Example**: 76 batches × 10,000 ops = 760,000 total ops
- **Not**: Number of batches (that would be ~76)

---

## Formula Verification

### Per-Batch Throughput ✅
```go
tpt := (float64(m.batchSize) / float64(lat)) * 1000
```
- **Input**: `batchSize = 100 ops`, `lat = 50 ms`
- **Output**: `(100 / 50) × 1000 = 2,000 Tx/sec` ✅ CORRECT

### Average Batch Throughput ✅
```go
avgBatchThroughput := tptSum / float64(counter)
```
- **Input**: Sum of all individual batch throughputs
- **Output**: Mean throughput per batch ✅ CORRECT

### Overall Throughput ✅
```go
totalElapsedSeconds := lastEndTime.Sub(m.meters[keys[0]].StartTime).Seconds()
overallThroughput := float64(m.batchSize*counter) / totalElapsedSeconds
```
- **Input**: 
  - Total ops = `100 ops/batch × 100 batches = 10,000 ops`
  - Total time = `last batch end time - first batch start time`
- **Output**: `10,000 ops / total_seconds` ✅ CORRECT
- **Accounts for**: Gaps between batches, queuing delays, network latency

---

## Difference: Average Batch Throughput vs Overall Throughput

### Average Batch Throughput
- Measures **instantaneous throughput** of each batch
- Does NOT account for gaps between batches
- **Higher value** (optimistic)
- **Use**: Understanding batch processing efficiency

### Overall Throughput
- Measures **sustained throughput** over entire run
- DOES account for gaps, queuing, network delays
- **Lower value** (realistic)
- **Use**: Understanding real-world system performance

### Example:
```
Batch 1: 50ms latency → 2,000 Tx/sec
Batch 2: 45ms latency → 2,222 Tx/sec
Batch 3: 55ms latency → 1,818 Tx/sec

Average Batch Throughput = (2000 + 2222 + 1818) / 3 = 2,013 Tx/sec

But if there were 10ms gaps between batches:
Total time = 50 + 10 + 45 + 10 + 55 = 170ms
Overall Throughput = (300 ops / 170ms) × 1000 = 1,765 Tx/sec
```

---

## CSV Output Example

```csv
pclock,latency (ms) per batch,throughput (Tx/sec),fast path ops,slow path ops,conflict ops
1,50,2000.000,80,20,0
2,45,2222.222,85,15,0
3,55,1818.182,75,25,0
AVERAGE,50.000 ms,2013.468 Tx/sec,80.000,20.000,0.000
OVERALL_THROUGHPUT,,1765.432 Tx/sec,,,,
P50_LATENCY,50 ms,,,,,
P95_LATENCY,55 ms,,,,,
P99_LATENCY,55 ms,,,,,
AVG_LATENCY_PER_TX,0.500 ms/Tx,,,,,
AVG_FAST_PATH_LATENCY,48.000 ms,,80 batches,,,
AVG_SLOW_PATH_LATENCY,52.000 ms,,,20 batches,,
GLOBAL_TOTALS,,,240 ops,60 ops,0 ops
```

**Note**: Before this fix, GLOBAL_TOTALS incorrectly counted batches instead of operations.
After fix: Global totals now correctly sum all operations across all batches.

All formulas are **CORRECT** ✅
