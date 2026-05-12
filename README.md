# CORA : Object-Weighted Dual-Path Consensus

> **C**onsensus with **O**bject-level **R**outing **A**daptation  
> A hybrid consensus protocol for mixed-workload cloud systems.

```
Fast path  →  leaderless, one RTT, per-object weighted quorum
Slow path  →  leader-coordinated, priority-weighted, FIFO ordered
```

---

## What is CORA?

Most real-world cloud workloads are **multi-tenant**: the vast majority of operations touch private, per-user data that no other client ever writes. Yet classical consensus protocols  Paxos, Raft, serialize *every* operation through a single leader, wasting throughput proportional to how independent the workload actually is.
CORA takes a different approach. Instead of detecting conflicts at runtime (EPaxos) or serializing everything (Raft/Cabinet), CORA routes operations based on a **declared access pattern** made at object initialization:

| Object type | Who writes it | Path taken |
|---|---|---|
| `Independent` | Single writer only | Fast path: leaderless, 1 RTT |
| `Dependent` | Multiple writers | Slow path:  leader-coordinated |
| `HotObject(Only for testing)` | High-contention shared | Slow path directly (no fast-path attempt) |

The routing decision is a single hash lookup. Zero runtime conflict detection.

---

## Architecture

```
Client Request
      │
      ▼
 Object Registry
 (type lookup)
      │
  ┌───┴────────────────────┐
  │                        │
  ▼                        ▼
FAST PATH              SLOW PATH
(Independent)          (Dependent / Hot)
  │                        │
  │  Any replica becomes   │  Forwarded to leader
  │  coordinator           │  → FIFO queue
  │                        │  → Priority-weighted
  │  Per-object weighted   │    voting
  │  quorum (≥ T_O)        │
  │                        │
  └────────────┬───────────┘
               │
          State Machine
           (all replicas)
```

**Fast path** runs in parallel across all independent objects simultaneously. Multiple coordinators, no leader involvement, one network round-trip.

**Slow path** handles only the operations that genuinely need ordering. The leader's queue establishes total order; a priority-weighted reputation mechanism concentrates decisions in the fastest-responding nodes.

---

## Key Properties

- **Per-object linearizability** — all operations on any single object appear atomic in real-time order
- **No cross-path conflict** — Independent and Dependent objects are disjoint; paths never interfere
- **Graceful degradation** — at 100% Dependent workload, CORA converges with Cabinet (no overhead added)
- **Leaderless fast path** — leader failure does not affect Independent object availability
- **Liveness under partial synchrony** — timeout-bounded fallback from fast path to slow path

---

## Performance

Evaluated on a 5-node cluster (Compute Canada Cloud, 4 vCPU / 8 GB / 10 Gbps), 95% independent / 5% dependent workload:

| Scenario | CORA | Cabinet | Speedup |
|---|---|---|---|
| Baseline (batch=1, pipe=1) | ~8 kTx/s | ~1.5 kTx/s | **5–6×** |
| Pipelined (MAX\_INFLIGHT=50, n=3) | 24.7 kTx/s | 1.38 kTx/s | **18×** |
| 100% Dependent | ~1.0 kTx/s | ~1.4 kTx/s | converges |
| Large batch (2000 ops/RPC) | 248 kTx/s | 149 kTx/s | **1.67×** |

Median latency at 95% independent: **< 1 ms**.

---

## Repository Structure

```
woc/
├── main.go                   # Entry point, flag parsing, pre-warms all objects
├── parameters.go             # Global config: IndependentObject, CommonObject, HotObject constants
├── client.go                 # Client driver — pipelined/sequential modes, limiter variants
├── consensus.go              # Core protocol — fast path, slow path, weight vectors, timeouts
├── service.go                # RPC handlers — ConsensusService, RequestVote, Ping
├── conns.go                  # Connection management, per-connection FIFO ordering
├── utils.go                  # Metrics, logging helpers
│
├── config/                   # Cluster config parsing (IPs, ports, node weights)
├── smr/                      # State machine layer
├── mongodb/                  # MongoDB integration (YCSB workload support)
│   ├── mgdb_main.go          # MongoDB workload entry point
│   ├── mgdb_leader.go        # YCSB query parser (INSERT/READ/UPDATE/SCAN)
│   ├── mgdb_follower.go      # MongoDB client pool, query execution
│   └── mgdb_dbClient.go      # Raw MongoDB CRUD operations
│
├── eval/                     # Evaluation output (per-client CSV files)
├── ycsb/                     # YCSB workload data files
│   └── scripts/              # Workload generation scripts
│
├── start_cluster.sh          # Start all server nodes
├── start_cluster_homo.sh     # Homogeneous cluster variant
├── start_cluster_hetero.sh   # Heterogeneous cluster (varying node weights)
├── stop_cluster.sh           # Graceful cluster shutdown
├── run_woc.sh                # Run client benchmark
├── run_mongodb_workload_hetero.sh
├── start_mongodb_hetero.sh
├── stop_mongodb_hetero.sh
├── distribute_tani_key.sh    # SSH key distribution to cluster nodes
├── delete_woc_homo.sh        # Cleanup helper
└── merge_eval.py             # Merge per-client eval CSVs into unified results
```

---

## Getting Started

### Prerequisites

- Go 1.21+
- MongoDB 6.0+ (for MongoDB workload mode)
- A cluster of machines with SSH access (or localhost for single-node testing)

### Build

```bash
git clone https://github.com/tanishaf28/woc.git
cd woc
go build -o woc .
```

### Single-node quick test

```bash
# Terminal 1: Start server (node 0, 5-node config, localhost)
./woc -server -id 0 -n 5 -config config/local.json

# Terminal 2: Run client (95% independent, sequential mode)
./woc -client -id 0 -n 5 -config config/local.json \
      -indep 95 -common 5 -ops 10000
```

### Cluster deployment

```bash
# 1. Distribute SSH keys
./distribute_tani_key.sh

# 2. Start all 5 nodes (homogeneous weights)
./start_cluster_homo.sh

# 3. Run benchmark
./run_woc.sh

# 4. Stop cluster
./stop_cluster.sh

# 5. Merge evaluation results
python3 merge_eval.py
```

---

## Configuration

Key flags (see `parameters.go` and `main.go`):

| Flag | Default | Description |
|---|---|---|
| `-n` | 5 | Number of server nodes |
| `-indep` | 95 | % Independent (fast path) operations |
| `-common` | 5 | % Common/Dependent (slow path) operations |
| `-conflict` | 0 | % HotObject (high-contention) operations |
| `-batch` | 1 | Operations per RPC batch |
| `-ops` | 0 (∞) | Total operations (0 = run until SIGINT) |
| `-msgsize` | 512 | Payload size in bytes |

Environment variables:

| Variable | Values | Effect |
|---|---|---|
| `PIPELINE_MODE` | `true` | Enable open-loop pipelined client |
| `MAX_INFLIGHT` | integer | Max concurrent in-flight batches |
| `USE_ADAPTIVE_LIMITER` | `true` | AdaptiveLimiter (adjusts based on path feedback) |
| `USE_SIMPLE_LIMITER` | `true` | SimpleLimiter (static semaphore) |
| `NO_LIMITER` | `true` | NoOpLimiter (localhost testing, zero overhead) |
| `LATENCY_DEBUG` | `true` | Detailed per-RPC latency breakdowns in logs |
| `MONGODB_URI` | URI string | MongoDB connection string (default: `localhost:27017`) |

---

## MongoDB / YCSB Workload

CORA includes a MongoDB execution layer that runs YCSB-format workloads through the consensus protocol.

### Generate YCSB data files

```bash
# Using the YCSB benchmark tool (https://github.com/brianfrankcooper/YCSB)

# Load phase
./bin/ycsb load mongodb -s -P workloads/workloada \
  -p recordcount=100000 \
  > ./ycsb/workData/load_workloada.dat

# Run phase  
./bin/ycsb run mongodb -s -P workloads/workloada \
  -p recordcount=100000 -p operationcount=100000 \
  > ./ycsb/workData/run_workloada.dat
```

Supported workloads: A (50/50 read-write, Zipfian), B (95/5), D (latest distribution), F (read-modify-write).

### Run MongoDB workload

```bash
# Heterogeneous cluster with MongoDB backend
./start_mongodb_hetero.sh
./run_mongodb_workload_hetero.sh
./stop_mongodb_hetero.sh
```

### Key mapping to CORA object types

| YCSB key | CORA classification |
|---|---|
| Key in this client's keyspace slice | `IndependentObject` → fast path |
| Key in another client's slice | `CommonObject` → slow path |
| Zipfian hot keys (top of distribution) | `HotObject` → slow path directly |

---

## Evaluation Sweeps
Results land in `./eval/client{N}/` as CSV files. Merge across clients with:

```bash
python3 merge_eval.py
```

### Reproduce paper experiments

```bash
# Contention sweep (Section 8.2): vary --indep from 100 to 0
for indep in 100 90 80 60 40 20 0; do
  PIPELINE_MODE=true MAX_INFLIGHT=10 ./run_woc.sh --indep $indep --common $((100-indep))
done

# Pipelining sweep (Section 8.4): vary MAX_INFLIGHT
for inflight in 1 5 10 20 30 40 50 55; do
  PIPELINE_MODE=true MAX_INFLIGHT=$inflight ./run_woc.sh
done

# Batch size sweep (Section 8.5): vary -batch
for batch in 1 10 50 100 500 1000 2000; do
  ./run_woc.sh -batch $batch
done
```

---

## Paper

**CORA: Adaptive Object Weighted Consensus Made Efficient**   
arXiv: [2512.20485](https://arxiv.org/abs/2512.20485)

---

## License
MIT

---
