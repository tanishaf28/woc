# CORA : Object-Weighted Dual-Path Consensus

> **Extended Correctness Report**: full safety/liveness proofs are in
> `CORA_extended_correctness_report.pdf` at the repo root.

Go implementation of CORA , plus the evaluation harness used for the paper's experiments.

```
Fast path  →  leaderless, one RTT, per-object weighted quorum   (Independent objects)
Slow path  →  leader-coordinated, priority-weighted, FIFO       (Dependent objects)
```

## What is CORA?

Classical consensus (Paxos, Raft) serializes every operation through one
leader, even when most operations touch independent, single-writer objects.
CORA classifies each object as `Independent` or `Dependent` at init and
routes accordingly, via a single hash-ring lookup with zero runtime conflict
detection:

| Object type | Path | Mechanism |
|---|---|---|
| `Independent` | Fast | Leaderless, 1 RTT, per-object weighted quorum |
| `Dependent` | Slow | Leader-coordinated, globally ordered |

`-indep` splits the fixed `-numobjects` pool between the two. The leader
builds the ring once at startup (`objectmap.go`) and disseminates ownership;
only the owning replica coordinates that object's fast path.

## Build

```bash
git clone https://github.com/tanishaf28/woc.git && cd woc
go build -o woc .
```

## Quick start

```bash
# server
./woc -server -id 0 -n 5 -config config/local.json
# client (95% independent)
./woc -client -id 0 -n 5 -config config/local.json -indep 95 -ops 10000
```

## Cluster deployment

```bash
./distribute_tani_key.sh          # 1. SSH keys
./start_cluster_homo.sh           # 2. start (or start_cluster_hetero.sh)
./run_woc.sh                      # 3. benchmark
./stop_cluster.sh                 # 4. stop
python3 merge_eval.py             # 5. merge eval/client{N}/ CSVs
```

## Configuration reference

| Flag | Default | Meaning |
|---|---|---|
| `-n` | 5 | server count |
| `-indep` | 95 | % independent objects |
| `-numobjects` | 1000 | object pool size |
| `-batch` | 1 | ops per RPC |
| `-ops` | 0 (∞) | total ops |
| `-readratio` | 0 | % reads |
| `-readmode` | `safe` | `safe` (quorum-confirmed) or `fast` (uncoordinated, not proof-covered) |

Env vars: `PIPELINE_MODE=true`, `MAX_INFLIGHT=<n>`, `USE_ADAPTIVE_LIMITER` /
`USE_SIMPLE_LIMITER` / `NO_LIMITER=true`, `LATENCY_DEBUG=true`,
`MONGODB_URI=<uri>`.

## Evaluation sweeps

Assumes a running cluster. Merge with `merge_eval.py` after each sweep.

```bash
# I2D ratio sweep
for i in 100 90 80 60 40 20 10 0; do PIPELINE_MODE=true MAX_INFLIGHT=10 ./run_woc.sh -indep $i; done

# Client scalability
for c in 2 5 10 20 30 35 40 50; do ./run_woc.sh -indep 90 -clients $c; done

# Batch size sweep
for b in 1 10 50 100 500 1000 2000; do ./run_woc.sh -indep 90 -batch $b; done

# Failure threshold sweep
for t in 1 2 3 4 5; do ./run_woc.sh -indep 100 -t $t; done

# Read ratio sweep
for r in 0 25 50 75 100; do ./run_woc.sh -readratio $r -readmode safe; done
```

## MongoDB / YCSB workload

Each replica applies committed ops to its own **standalone** MongoDB (no Mongo
replica set CORA does the replication).

**Setup**: edit the IP arrays (`SERVER_IPS`, `CLIENT_HOST_IPS`) at the top of
`scripts/start_mongodb_hetero.sh`, `stop_mongodb_hetero.sh`,
`scripts/distribute_ycsb.sh`, and the sweep scripts; run
`./scripts/distribute_tani_key.sh`; install MongoDB 6.0+ on every server.

```bash
# 1. Generate data (checked-in workload*.dat files are sized recordcount=100000;
#    ycsb/config/workload* defaults to 50/50 edit before regenerating, or:
cd ycsb/scripts && ./genSingleData.sh -f a -r 100000 -o 100000

# 2. Distribute to cluster (servers only auto-sync workload.dat; clients need run_workload*.dat)
./scripts/distribute_ycsb.sh

# 3. Run (builds, deploys, starts mongod+WOC servers/clients; open-ended)
INDEP_RATIO=90 ./scripts/start_mongodb_hetero.sh a      # sweep INDEP_RATIO for I2D
THRESHOLD=3 ./scripts/start_mongodb_hetero.sh a         # sweep THRESHOLD 1-5

# 4. Stop + collect (merges eval/ from every node into scripts/eval/merged/)
./scripts/stop_mongodb_hetero.sh
```

Key env vars: `INDEP_RATIO` (100), `BATCHSIZE` (10), `READ_RATIO` (0),
`THRESHOLD` (1), `NUM_CLIENTS` (2), `MAX_INFLIGHT` (5).

Automated sweeps: `RUNTIME_SECONDS=30 WORKLOAD=a ./scripts/run_mongodb_ratio_sweep_5n.sh`
and `..._batchsize_sweep_5n.sh` - each archives results under
`scripts/results/mongodb_*_sweep_5n/<timestamp>/` and runs `extract_metrics.py`.

Key classification: keys hashing independent under `-indep`
(`classifyRealKey` in `objectmap.go`) → fast path; else → slow path.

## License

MIT
