
##  Dual-Path Consensus Overview
This system implements a dual-path consensus protocol combining **object-weighted fast quorums** and **node-priority slow quorums** (Cabinet-style).
The goal is to maximize concurrency for independent objects while maintaining global safety and liveness under conflicts.

---

### Fast Path: Object-Weighted, Leaderless
**Nature:** One-round-trip consensus (no global leader).
**Coordinator:** The first replica contacted by the client.
**Goal:** Maximize concurrency for independent objects.

**Flow**
1. Client → Replica: sends command `(ObjID, Value, OpID)`.
2. Coordinator → All Replicas: forwards proposal.
3. Replicas validate for conflicts and return their object weights.
4. Coordinator aggregates replies:

   *  Commit if total weight ≥ object threshold.
   *  Otherwise, fall back to slow path.

**Conflict Detection:**
If multiple writes target the same object, fast path aborts → switch to slow path.
Independent objects proceed concurrently.

---

###  Slow Path: Node-Priority, Cabinet-Style

**Leader:** Stable, globally elected (Cabinet/Raft-style).
**Threshold:** Majority of node-weight sum.

**Triggered When**
* Object is shared or frequently updated (hot).
* Fast path quorum not met or conflict detected.

**Flow**
1. Coordinator forwards request to the global leader.
2. Leader broadcasts proposal to all replicas.
3. Replicas reply with node weights.
4. Leader commits once weighted quorum is reached and notifies client.

---

###  Key Properties

| Property      | Fast Path                 | Slow Path                          |
| ------------- | ------------------------- | ---------------------------------- |
| Leadership    | Leaderless                | Stable leader                      |
| Weight Scheme | Object-based              | Node-based                         |
| Concurrency   | High (parallel)           | Serialized                         |
| Use Case      | Independent objects       | Shared/conflicting objects         |
| Guarantees    | Safety via object weights | Safety + liveness (Cabinet quorum) |

---

###  Example

**Cluster:** 7 replicas
**Object A Weights:** R1=0.3, R2=0.3, R3=0.2, R4=0.1 → `Threshold(A)=0.5`

*  **Fast Success:** R1 (0.3) + R2 (0.3) = 0.6 ≥ 0.5 → Commit via fast path.
*  **Conflict:** Two clients update `A` concurrently → Slow path handles serialization.
*  **Independent Object:** `B` commits via fast path in parallel, unaffected by A.

---

###  Module Summary
**Root Files**
* `main.go` – Bootstrap: initializes configs, managers, and launches client/server roles.
* `client.go` – Client workload generator; records metrics for FAST/SLOW paths.
* `service.go` – RPC entrypoint; maps requests to consensus logic and handles leader re-election.
* `consensus.go` – Core logic: routes commands, handles quorum collection, fallback, and leader coordination.
* `conns.go` – Manages peer connections, server docks, and MongoDB init/cleanup.
* `parameters.go` – CLI flag parsing and experiment parameters.
* `utils.go` – Logger setup and random payload generation.
* `run_woc.sh` – Script to start servers and clients.

**SMR Folder**
* `state.go` – Object and server state, quorum thresholds, and commit helpers.
* `pmgr.go` – PriorityManager (Cabinet-style quorum logic and rotation).
* `priority.go` – Priority computation utilities.
* `object_test.go`, `pmgr_test.go` – Unit tests for SMR components.

**Other**
* `eval/` – Experimental metrics and evaluation scripts.
* `config/` – Cluster configuration and test configs.
* `logs/`, `scripts/`, `mongodb/` – Supporting runtime data.

