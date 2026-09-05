# CORA: Adaptive Object-Weighted Consensus Made Efficient

> A dual-path consensus protocol that jointly exploits **weighted quorums**
> and **object-level concurrency** through adaptive request routing.
>
> **Correctness report:** full safety/liveness proofs are in
> [`CORA_extended_correctness_report.pdf`](CORA_extended_correctness_report.pdf)
> at the repo root. This README covers the implementation only.

```
Fast path  →  leaderless, one RTT, per-object weighted quorum   (independent objects)
Slow path  →  leader-coordinated, node-weighted, FIFO ordered   (dependent objects)
```

---

## How It Works

Classical consensus (Paxos, Raft) and weighted-quorum protocols (Cabinet)
serialize *every* operation through one global order, even though most
workloads are dominated by operations on data no other client ever touches.
CORA routes each transaction by a **declared, per-object access pattern**:

| Object type | Writer cardinality | Path | Coordinator |
|---|---|---|---|
| `Independent` | single writer | **Fast path**: leaderless, 1 RTT, per-object weighted quorum | the object's owner (assigned via a hash ring) |
| `Dependent` | multiple writers | **Slow path**: leader-coordinated, node-weighted, FIFO | the cluster leader |

Routing costs one hash lookup, no runtime conflict detection, no dependency
graph. A transaction touching **more than one object is always dependent**,
even if every object is individually independent, since atomically
committing across objects needs the ordering only the slow path provides
(`client.go`'s `multi_obj` composition, `consensus.go`'s `MultiObject` flag).

```
Client Request
      │
      ▼
 Object Registry (objectmap.go)
      │
  ┌───┴────────────────────┐
  │                        │
  ▼                        ▼
FAST PATH              SLOW PATH
(Independent)          (Dependent)
  │                        │
  │ Owning replica (hash   │ Forwarded to leader
  │ ring) becomes          │  → single FIFO queue
  │ coordinator; per-object│  → node-weighted
  │ weighted quorum        │    quorum vote
  └────────────┬───────────┘
               │
     State Machine (applied by every
      voting replica, not just the
         coordinator/leader)
```

Both paths share one pattern: a coordinator seeds an accumulator with its
own weight, broadcasts a proposal, and commits once collected replies cross
half the domain's total weight object weights on the fast path, the
global replica-weight vector on the slow path.

**Key properties:**

- **Per-object linearizability** : a per-object write lock plus a monotonic
  sequence guard (`ObjectState.ApplyFastProvisional`) keep at most one
  fast-path round outstanding per object.
- **Path disjointness** : an object's type is fixed at creation; independent
  and dependent objects share no coordination state.
- **Leaderless fast path** : leader failure doesn't affect independent
  object availability; fast-path coordinators are assigned purely by
  consistent hashing.
- **Dynamic fault tolerance** : the leader's health monitor detects failures
  and reassigns the ownership ring through the same slow-path consensus
  round as any dependent write, so every replica adopts it at the same
  global order position (`startReplicaHealthMonitor`, `RingUpdate`).
- **Per-object fault-tolerance tuning** : `-hotobjthreshold`/`-hotobjid` let
  one object run a different failure threshold `t` than the rest of the run.
- **Adaptive liveness** : an RFC 6298-style (SRTT/RTTVAR) timeout bounds the
  fast-path quorum wait before falling back to the slow path.

**Reads** (`-readratio`, `-readmode`): **fast** answers from whichever
replica got the request, no coordination, independent objects only. **safe**
gathers a weighted quorum and returns the value held by the
highest-weight responder under the weight-reassignment rule, that's
always the most recent proposer, so it's the freshest value available
without a per-object version vector (`quorumRead`, `consensus.go`).

---

## Implementation Map

| Paper concept | Implementation |
|---|---|
| Independent/dependent classification, static type / dynamic per-tx routing | `objectmap.go`: `InitObjectRegistry`, `classifyRealKey` |
| A transaction spanning >1 object is dependent even if every object is independent | `client.go`'s `multi_obj` composition, `consensus.go`'s `MultiObject` flag |
| Consistent-hash ring ownership, computed by the leader and disseminated | `objectmap.go`: `HashRing`, `BuildOwnershipRing`, `AssignOwnership`; `main.go`: `fetchOwnershipFromLeader` |
| Dynamic ring reconfiguration on replica failure, via slow-path consensus | `consensus.go`: `startReplicaHealthMonitor`; `objectmap.go`: `RingUpdate`/`applyRingUpdate` |
| Per-object weight matrix, geometric weight vector, configurable per-object `t` | `smr/state.go`: `GenerateWeights`; `smr/pmgr.go`: `calcInitPrioRatio` |
| Fast-path quorum threshold = half the object's total weight | `smr/state.go`: `ComputeFastThreshold` / `ComputeFastThresholdExcluding` |
| Fast-path pattern: seed, broadcast, accumulate, commit, reassign, fallback | `consensus.go`: `handleFastPath` |
| "The vote is the apply"  a follower applies on accept, revertible on fallback | `smr/state.go`: `ApplyFastProvisional` / `RevertIfSeqMatches`; `consensus.go`: `broadcastFallBack` |
| Weight reassignment: coordinator keeps top weight, responders next by arrival order | `smr/state.go`: `ObjectState.ReassignWeights` |
| Slow path: single serialized leader queue, global weight vector | `consensus.go`: `startSlowPathProcessor`; `smr/pmgr.go`: `PriorityManager` |
| Auto load balancing from independent per-object weight rankings | falls out of each object's independently generated weight vector |
| Fast read (local) vs. safe read (weighted quorum, freshest-by-weight) | `consensus.go`: `handleRead` / `quorumRead` |

See the correctness report for the full safety/liveness proof and where the
implementation's guarantees are established.

---

## Repository Structure

```
woc/
├── main.go              # Entry point, flag parsing, object-pool pre-warm, ownership bootstrap
├── objectmap.go          # Hash ring + object registry: object mapping
├── parameters.go          # All CLI flags and global run parameters
├── client.go              # Client driver: sequential/pipelined dispatch, limiters, batch composition
├── consensus.go            # Core protocol: fast path, slow path, weight/priority orchestration,
│                            #   timeouts, failure detection, leader election
├── service.go              # net/rpc surface: ConsensusService, RequestVote, Ping, ownership RPCs
├── conns.go                # Connection management, per-connection FIFO ordering, gob registration
├── metrics_server.go        # Runtime metrics endpoint
├── utils.go                 # Logging setup
│
├── smr/                      # State machine layer
│   ├── state.go               #   ObjectState/ServerState: per-object weights, thresholds, provisional apply
│   ├── pmgr.go                 #   PriorityManager: global weight vector, geometric-ratio search, reassignment
│   └── priority.go              #   PriorityState: this replica's own current priority clock/value
│
├── config/                        # Cluster config parsing + one .conf file per topology
├── mongodb/                        # MongoDB integration (YCSB workload execution layer)
├── eval/                            # Evaluation output (per-client/per-server CSVs) + PerfMeter
├── ycsb/                             # YCSB workload config + generated data files
│
├── run_woc.sh                        # Build + run a full localhost cluster in one command
├── merge_eval.py                      # Merge per-client/per-server eval CSVs
├── extract_metrics.py                  # Summarize a sweep directory into one CSV
├── plot_timeseries.py                   # Render time-series plots from eval CSVs
│
└── scripts/                              # Distributed deployment + evaluation sweeps (below)
```

> **All scripts except `run_woc.sh` live under `scripts/`** - distributed
> cluster deployment, MongoDB/YCSB helpers, and every evaluation sweep. Those
> scripts target the authors' own private test cluster (hardcoded IPs, SSH
> user `ubuntu`, key `~/.ssh/tani.pem`, remote path `/home/ubuntu/woc`) 
> edit the IP arrays at the top of each before pointing them elsewhere.

---

## Getting Started

**Prerequisites:** Go 1.21+ (tested with 1.22); MongoDB 6.0+ only for the
MongoDB/YCSB workload mode.

```bash
git clone https://github.com/tanishaf28/woc.git
cd woc
go build -o woc .
```

**One-command local cluster:**

```bash
./run_woc.sh
```

Builds, starts a 5-node localhost cluster (`config/cluster_localhost.conf`)
plus 2 clients, and runs until `Ctrl+C` (graceful shutdown, saves metrics to
`eval/`). Every variable at the top of the script is env-overridable:

```bash
INDEP_RATIO=50 BATCHSIZE=100 NUM_SERVERS=7 ./run_woc.sh
```

**Manual quick test** (real flag names - `-role` not `-server`/`-client`,
`-path` not `-config`, `-b` not `-batch`, `-ms` not `-msgsize`; `-pinserver`
is required for clients, no round-robin default):

```bash
./woc -role=0 -id=0 -n=5 -t=1 -path=config/cluster_localhost.conf -indep=90
# ...repeat -id=1..4 for the rest of the cluster, then:
./woc -role=1 -id=5 -n=5 -t=1 -path=config/cluster_localhost.conf \
      -indep=95 -pinserver=0 -ops=10000
```

---

## Configuration

| Flag | Default | Description |
|---|---|---|
| `-role` | 0 | `0` = server, `1` = client |
| `-n` | 10 | Number of server nodes |
| `-t` | 1 | Fault-tolerance threshold; quorum size is `t+1` |
| `-path` | `./config/cluster_localhost.conf` | Cluster config file path |
| `-mode` | 0 | `0` = localhost, `1` = distributed |
| `-et` | 0 | Eval type: `0` = plain message, `1` = MongoDB |
| `-indep` | 90 | % Independent (fast-path) objects |
| `-numobjects` | 1000 | Size of the fixed, hash-ring-mapped object pool |
| `-b` | 1 | Operations per RPC batch |
| `-bcomp` | `object-specific` | `mixed`, `object-specific`, `single_obj`, or `multi_obj` |
| `-ops` | 1000 | Total ops (client); `0` = run until SIGINT |
| `-readratio` / `-readmode` | 0 / `fast` | % reads; `fast` (no quorum) or `safe` (weighted quorum) |
| `-pinserver` | required | Client's initial contact server (no round-robin default) |
| `-ep` | true | `true` = weighted (Cabinet-style), `false` = plain Raft-style |
| `-cm` / `-ct` / `-crashtarget` | 0 / 20 / -1 | Crash-simulation mode / round count / target replica |
| `-hotobjthreshold` / `-hotobjid` | -1 / `obj-0` | Give one object a different `-t` than the rest |
| `-pd` / `-log` | false / debug | Production mode (logs to `./logs/`) / log level |

**Environment variables:** `PIPELINE_MODE`, `MAX_INFLIGHT`,
`USE_ADAPTIVE_LIMITER` / `USE_SIMPLE_LIMITER`, `LATENCY_DEBUG`,
`SERVER_BATCHING`, `MONGODB_URI`.

---

## Distributed Cluster Deployment

Scripts under `scripts/` build the binary locally, distribute it plus
config, and launch/stop the cluster over SSH - one start/stop pair per
topology:

| Topology | Start / Stop | Node config |
|---|---|---|
| Heterogeneous (paper's main setup) | `start_cluster_hetero.sh` / `stop_cluster_hetero.sh` | `config/cluster_hetero_*n_*.conf` (n ∈ {3,5,7,11}) |
| Homogeneous | `start_cluster_homo.sh` / `stop_cluster_homo.sh` | `config/cluster_homo.conf` (flat pool, any size) |

```bash
./scripts/distribute_tani_key.sh                                     # 1. SSH keys
NUM_SERVERS=5 INDEP_RATIO=90 BATCHSIZE=10 ./scripts/start_cluster_hetero.sh  # 2. start
./scripts/stop_cluster_hetero.sh                                     # 3. stop + collect + merge
```

`scripts/start_cluster.sh`/`stop_cluster.sh` (no suffix) are an older,
pre-split variant kept for reference prefer the pair above.

> **Gotcha:** start scripts read `NUM_SERVERS`/`NUM_CLIENTS`; the matching
> stop scripts read `SERVER_COUNT`/`CLIENT_COUNT` instead set both when
> overriding cluster size, or the stop side silently reverts to its default.

---

## MongoDB / YCSB Workload

Each replica applies committed ops to its own **standalone** MongoDB
instance no Mongo replica set; CORA does the replication. Paper's §6.2
results use YCSB Workload A (50% read / 50% update).

```bash
# 0. Edit IP arrays in scripts/start_mongodb_hetero.sh, stop_mongodb_hetero.sh,
#    distribute_ycsb.sh, and run_mongodb_*_sweep_5n.sh for your cluster, then:
./scripts/distribute_tani_key.sh

# 1. Generate YCSB data (checked-in workload*.dat is sized recordcount=100000;
#    the checked-in ycsb/config/workload* defaults to a 10/10 smoke test)
cd ycsb/scripts && ./genData.sh
# or one workload at a time: ./genSingleData.sh -f a -r 100000 -o 100000

# 2. Distribute data (servers auto-sync workload.dat; clients need run_workload*.dat too)
./scripts/distribute_ycsb.sh

# 3. Run (builds, deploys, starts mongod + WOC servers/clients; open-ended)
INDEP_RATIO=90 BATCHSIZE=25 ./scripts/start_mongodb_hetero.sh a   # workload letter a-f

# 4. Stop + collect (merges eval/ from every node into scripts/eval/merged/)
./scripts/stop_mongodb_hetero.sh
```

Tunable env vars (defaults): `INDEP_RATIO=100.0`, `BATCHSIZE=10`,
`NUM_OBJECTS=1000`, `READ_RATIO=0.0`, `THRESHOLD=1`, `NUM_CLIENTS=2`,
`MAX_INFLIGHT=5`, `LOG_LEVEL=info`.

Automated sweeps: `RUNTIME_SECONDS=30 WORKLOAD=a ./scripts/run_mongodb_ratio_sweep_5n.sh`
and `..._batchsize_sweep_5n.sh` - each archives results under
`scripts/results/mongodb_*_sweep_5n/<timestamp>/` and runs `extract_metrics.py`.

Key classification: a key hashes independent under `-indep`
(`classifyRealKey` in `objectmap.go`) → fast path; else → slow path.

---

## Evaluation Sweeps

Results land in `./eval/client{N}/` and `./eval/server{N}/`; merge with
`python3 merge_eval.py`. `run_woc.sh` takes every parameter as an
environment-variable override, so the paper's single-machine sweeps are
direct loops over it:

```bash
# I2D ratio (§6.1.1/Fig. 3)
for indep in 100 90 80 60 40 20 10 0; do INDEP_RATIO=$indep NUM_SERVERS=5 ./run_woc.sh; done

# Client scalability (§6.1.2/Fig. 4)
for nc in 2 5 10 20 30 40 50; do NUM_CLIENTS=$nc INDEP_RATIO=90 ./run_woc.sh; done

# Batch size (§6.1.3/Fig. 5)
for batch in 1 10 100 500 1000 2000; do BATCHSIZE=$batch INDEP_RATIO=90 ./run_woc.sh; done

# Failure-threshold sensitivity (§6.1.4/Fig. 6)
for t in 1 2 3 4 5; do THRESHOLD=$t INDEP_RATIO=100 NUM_SERVERS=11 ./run_woc.sh; done

# Read-ratio sensitivity (§6.1.5/Fig. 7)
for r in 0 25 50 75 100; do READ_RATIO=$r READ_MODE=safe ./run_woc.sh; done
```

For the heterogeneous-cluster and MongoDB/YCSB figures, use the matching
`scripts/*_sweep*.sh` drivers on a real multi-machine deployment.

---

## Development

```bash
go build ./...   # build everything, including smr/, config/, mongodb/
go vet ./...      # static checks
go test ./...      # unit tests
gofmt -l .          # should print nothing
```

---

## Performance

Headline numbers from the paper (§6), heterogeneous `n = 5` unless noted:

| Scenario | CORA | Best baseline | Speedup |
|---|---|---|---|
| I2D=100/0 (all independent, no batching) | 11.3 KTPS @ 0.86 ms | EPaxos: 6.1 KTPS @ 1.24 ms | 1.86× |
| I2D=100/0 vs. Cabinet / Raft | 11.3 KTPS | Cabinet: 774 TPS · Raft: 483 TPS | 16× / 22.6× |
| Client scaling (I2D=90/10, peak) | 19 KTPS @ 13 ms (35 clients) | EPaxos: 13 KTPS @ 21 ms | 1.46× |
| Batching (I2D=90/10, b=2000) | 570K TPS @ 20 ms | EPaxos: 1.5× lower throughput, 2× higher latency | 1.5–6.3× vs. all |
| YCSB Workload A over MongoDB (I2D=100/0) | 8,932 TPS @ 0.93 ms | EPaxos: 5,251 TPS @ 1.87 ms | 1.7×, ~half latency |

Across the full evaluation, CORA achieves **1.86–22.6×** higher throughput
than EPaxos/Cabinet/Raft, with the largest margins on heterogeneous clusters
and independent-heavy workloads.

---
Artifact repository: <https://github.com/tanishaf28/woc>

## License

MIT
