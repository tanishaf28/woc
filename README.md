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

The routing decision is a single hash lookup. Zero runtime conflict detection.
A fixed pool of objects (`-numobjects`, default 1000) is split into
independent/dependent by `-indep`. The global leader builds a single
consistent-hash ring over all objects (`objectmap.go`) and disseminates the
resulting ownership mapping to every replica at startup
(`WocService.GetObjectOwnership`) — only the owner coordinates that
object's fast path; other replicas forward to it. Ring updates on replica
failure are not yet implemented (the ring is fixed once at startup).

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
(Independent)          (Dependent)
  │                        │
  │  Owning replica        │  Forwarded to leader
  │  (hash ring) becomes   │  → FIFO queue
  │  coordinator           │  → Priority-weighted
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

## Reads

Every committed object can be read in two modes (`-readratio`, `-readmode`):

- **Fast read** — whichever replica receives the request answers from its
  own local state immediately. Zero coordination, lowest latency,
  speculative (no freshness guarantee).
- **Safe read** — gathers Value from a quorum of replicas (the same
  per-object weight threshold independent-object writes use, or the global
  threshold for dependent objects, routed through the leader like dependent
  writes) and returns the value from whichever responding replica carries
  the highest weight. Under Cabinet's weight reassignment the highest-weight
  responder is the most recent fast responder — the last proposer by
  definition — so its value is the freshest available without a per-object
  version vector.

Reads only return meaningful values because every replica that votes in a
fast/slow-path round now actually applies the command locally (not just the
coordinator/leader) — see `Execute` in `service.go`/`consensus.go`.

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
├── main.go                   # Entry point, flag parsing, pre-warms the 1000-object pool
├── objectmap.go              # Hash ring + object registry: object mapping (paper §4.2)
├── parameters.go             # Global config: IndependentObject, DependentObject constants
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
│   └── mgdb_dbClient.go      # Raw MongoDB CRUD operations + bulkWrite batching
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
      -indep 95 -ops 10000
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
| `-indep` | 95 | % Independent (fast path) objects; remainder are Dependent |
| `-numobjects` | 1000 | Size of the fixed, hash-ring-mapped object pool |
| `-batch` | 1 | Operations per RPC batch |
| `-ops` | 0 (∞) | Total operations (0 = run until SIGINT) |
| `-msgsize` | 512 | Payload size in bytes |
| `-readratio` | 0 | % of operations that are reads (0 = all writes) |
| `-readmode` | `fast` | `fast` (any replica, zero coordination, speculative) or `safe` (weighted-quorum confirmation before returning a value) |

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

CORA includes a MongoDB execution layer that runs YCSB-format workloads through
the consensus protocol: each replica applies committed operations to its own
**independent, standalone** MongoDB instance (no MongoDB-level replica set —
CORA does the replication, not MongoDB). The pipeline is three steps: generate
YCSB data locally, distribute it to the cluster, then start/stop the
MongoDB-backed cluster.

### 0. Prerequisites

- The remote scripts under `scripts/` target a specific hardcoded topology: 5
  server IPs (`SERVER_IPS`) + 2 client IPs (`CLIENT_HOST_IPS`), SSH user
  `ubuntu`, remote path `/home/ubuntu/woc`, key `~/.ssh/tani.pem`. Edit those
  arrays at the top of each script (`start_mongodb_hetero.sh`,
  `stop_mongodb_hetero.sh`, `scripts/distribute_ycsb.sh`, and the
  `run_mongodb_*_sweep_5n.sh` scripts) to match your own cluster's IPs before
  running anything.
- Push the SSH key to every node first (reads the IPs out of
  `config/cluster_hetero.conf` / `config/cluster_homo.conf`):
  ```bash
  ./scripts/distribute_tani_key.sh
  ```
- MongoDB 6.0+ must be installed (as the `mongod` binary) on every server
  node; `mongosh` is used locally by the readiness checks.

### 1. Generate YCSB data files

Data generation lives under `ycsb/scripts/`, not the repo root, and drives
the real `ycsb` benchmark tool (auto-cloned/built on first run — needs a JDK
and Maven, both auto-installed locally if missing):

```bash
cd ycsb/scripts
./genData.sh
```

This clones/builds YCSB into `./YCSB` if not already present, then for each
workload letter `a`-`f` generates:
- `ycsb/workData/workload.dat` — the load phase (INSERTs only), generated
  once from `ycsb/config/workloada`. Every replica loads this same file at
  startup (`initMongoDB()` in `conns.go`) to seed its local database.
- `ycsb/workData/run_workload{a..f}.dat` — the run phase for each workload,
  generated from `ycsb/config/workload{a..f}`. Clients read the one matching
  `-mload`/`WORKLOAD` directly at runtime.

**Recordcount/operationcount default to 10/10** in the checked-in
`ycsb/config/workload*` files (a tiny smoke-test size) — that's *not* what
generated the `.dat` files already committed in this repo (those are sized
for `recordcount=100000`). To regenerate at that scale, either edit
`recordcount`/`operationcount` in `ycsb/config/workload{a..f}` before running
`genData.sh`, or generate one workload at a time with explicit sizes:

```bash
./genSingleData.sh -f a -r 100000 -o 100000   # workload a, 100k records/ops
```

### 2. Distribute YCSB data to the cluster

`start_mongodb_hetero.sh` only auto-syncs `workload.dat` to the *server*
nodes as part of its own setup. Client nodes need the `run_workload*.dat`
files too (they read them directly), so run this once after generating data
and before starting the cluster:

```bash
./scripts/distribute_ycsb.sh
```

Copies every `ycsb/workData/*.dat` file to all server *and* client IPs over
scp, then verifies the file count landed on each node.

### 3. Run the MongoDB-backed cluster

```bash
./scripts/start_mongodb_hetero.sh a      # workload letter (a-f), default a
```

This one script does the whole cluster lifecycle: builds the binary locally,
distributes it (+ config + `workload.dat`) to every server/client, starts a
fresh standalone `mongod` per server (drops any previous data), waits for
each to report ready, starts the WOC servers (each independently reloads
`workload.dat` into its own local database), waits for their RPC listeners,
then starts the WOC clients pinned round-robin across servers. Runs
open-ended (`-ops=0`) until stopped.

Tune it via environment variables (defaults shown):

| Variable | Default | Meaning |
|---|---|---|
| `INDEP_RATIO` | `100.0` | % independent (fast-path) objects |
| `BATCHSIZE` | `10` | ops per RPC batch |
| `NUM_OBJECTS` | `1000` | size of the fixed object pool |
| `READ_RATIO` | `0.0` | % of ops that are reads |
| `THRESHOLD` | `1` | fault-tolerance threshold (quorum = t+1) |
| `NUM_CLIENTS` | `2` | MongoDB client connections per server |
| `MAX_INFLIGHT` | `5` | pipelined client concurrency |
| `LOG_LEVEL` | `info` | `info` or `debug` |

```bash
# Example: 50/50 independent-dependent split, batch of 25, workload a
INDEP_RATIO=50 BATCHSIZE=25 ./scripts/start_mongodb_hetero.sh a
```

Let it run for as long as you want the benchmark to collect data, then stop
and collect results:

```bash
./scripts/stop_mongodb_hetero.sh
```

Gracefully kills clients (waits up to 45s, then SIGKILLs), then servers
(waits up to 60s, then SIGKILLs), copies every node's `eval/` directory back
to `scripts/eval/`, and merges the per-client and per-server CSVs via
`merge_eval.py` into `scripts/eval/merged/`.

### 4. Automated sweeps (optional)

For a full parameter sweep instead of one manual start/stop, use:

```bash
RUNTIME_SECONDS=30 WORKLOAD=a ./scripts/run_mongodb_ratio_sweep_5n.sh      # sweeps INDEP_RATIO: 100,90,80,60,40,20,10,0
RUNTIME_SECONDS=30 WORKLOAD=a ./scripts/run_mongodb_batchsize_sweep_5n.sh  # sweeps BATCHSIZE
```

Each internally calls `start_mongodb_hetero.sh` / `stop_mongodb_hetero.sh`
once per test case, archives each case's merged CSVs under
`scripts/results/mongodb_*_sweep_5n/<timestamp>/`, and finishes by running
`extract_metrics.py` for a summary CSV.

Two older, self-contained variants of the same idea exist as
`scripts/eval_1_indep_ratio.sh` (indep-ratio sweep) and
`scripts/eval_2_batching.sh` (batch-size sweep) — they embed their own
mongod start/stop instead of delegating to `start_mongodb_hetero.sh`, but
follow the same standalone-per-node MongoDB setup.

### Key mapping to CORA object types

| YCSB key | CORA classification |
|---|---|
| Key hashes independent under `-indep` (`classifyRealKey` in `objectmap.go`) | `IndependentObject` → fast path |
| Key hashes dependent | `DependentObject` → slow path |

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
  PIPELINE_MODE=true MAX_INFLIGHT=10 ./run_woc.sh --indep $indep
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
