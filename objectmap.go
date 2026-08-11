package main

import (
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"sync"
)

// ObjectMeta describes one entry in CORA's fixed object pool: its type
// (independent/dependent, per the paper's §3.1 classification) and the
// replica that owns its fast-path coordination, per the hash-ring object
// mapping described in §4.2.
type ObjectMeta struct {
	Index int
	ID    string
	Type  int
	Owner int
}

const virtualNodesPerReplica = 100

// HashRing is a standard consistent-hash ring: replicas are hashed onto
// the ring at several virtual-node positions each, and an object's owner
// is the first replica found walking clockwise from the object's hash.
// This gives each replica a contiguous arc of the ring for free, matching
// "each replica handles a range of objects" (paper §4.2).
type HashRing struct {
	positions []uint32
	owners    map[uint32]int
}

func NewHashRing(numReplicas int) *HashRing {
	r := &HashRing{owners: make(map[uint32]int, numReplicas*virtualNodesPerReplica)}
	for replicaID := 0; replicaID < numReplicas; replicaID++ {
		for v := 0; v < virtualNodesPerReplica; v++ {
			key := fmt.Sprintf("replica-%d-vnode-%d", replicaID, v)
			pos := crc32.ChecksumIEEE([]byte(key))
			r.positions = append(r.positions, pos)
			r.owners[pos] = replicaID
		}
	}
	sort.Slice(r.positions, func(i, j int) bool { return r.positions[i] < r.positions[j] })
	return r
}

// Owner returns the replica responsible for coordinating objID's fast path.
func (r *HashRing) Owner(objID string) int {
	pos := crc32.ChecksumIEEE([]byte(objID))
	idx := sort.Search(len(r.positions), func(i int) bool { return r.positions[i] >= pos })
	if idx == len(r.positions) {
		idx = 0
	}
	return r.owners[r.positions[idx]]
}

// OwnerExcluding is Owner, but skips virtual nodes belonging to replicas in
// dead. This is paper §4.2's failure-handling mechanism — "the leader
// detects the failure and removes r from the ring, reassigning its arc to
// the adjacent replica" — implemented as a ring walk that skips dead
// replicas, rather than rebuilding the ring without them (cheaper, and
// gives the same "adjacent replica inherits the arc" result). Returns -1
// only if every replica on the ring is dead.
func (r *HashRing) OwnerExcluding(objID string, dead map[int]bool) int {
	if len(dead) == 0 {
		return r.Owner(objID)
	}
	pos := crc32.ChecksumIEEE([]byte(objID))
	idx := sort.Search(len(r.positions), func(i int) bool { return r.positions[i] >= pos })
	n := len(r.positions)
	for i := 0; i < n; i++ {
		owner := r.owners[r.positions[(idx+i)%n]]
		if !dead[owner] {
			return owner
		}
	}
	return -1
}

var (
	objectRegistryOnce sync.Once
	objectByIndex      map[int]*ObjectMeta // the fixed 1000-object pool
	objectByID         map[string]*ObjectMeta
	independentIdx     []int
	dependentIdx       []int
)

// InitObjectRegistry builds the cluster-wide object pool's ID/type
// assignment: numObjects objects, split into independent/dependent by
// indepRatio. This part is deployment-time-static and deterministic, so
// every server and client computes it independently — it is NOT the
// hash ring. Ownership (which replica coordinates each object's fast
// path) is assigned separately by AssignOwnership, since per §4.2 the
// ring is computed once by the global leader and disseminated, not
// independently derived by every node (see BuildOwnershipRing).
func InitObjectRegistry() {
	objectRegistryOnce.Do(func() {
		indepCount := int(float64(numObjects) * indepRatio / 100.0)

		objectByIndex = make(map[int]*ObjectMeta, numObjects)
		objectByID = make(map[string]*ObjectMeta, numObjects)

		for i := 0; i < numObjects; i++ {
			id := fmt.Sprintf("obj-%d", i)
			objType := DependentObject
			if i < indepCount {
				objType = IndependentObject
			}
			meta := &ObjectMeta{Index: i, ID: id, Type: objType, Owner: -1}
			objectByIndex[i] = meta
			objectByID[id] = meta

			if objType == IndependentObject {
				independentIdx = append(independentIdx, i)
			} else {
				dependentIdx = append(dependentIdx, i)
			}
		}
	})
}

// BuildOwnershipRing computes the owning replica for every object via a
// consistent-hash ring over numReplicas replicas. Only the global leader
// calls this (paper §4.2: "the global leader establishes a hash ring ...
// the resulting mapping metadata is sent to all replicas"); followers
// receive the result through AssignOwnership instead of computing their
// own ring. Index i of the result is object i's owning replica. dead
// excludes failed replicas from consideration (see SetDeadReplicas) — pass
// an empty/nil map at initial startup when nothing has failed yet.
func BuildOwnershipRing(numReplicas int, dead map[int]bool) []int {
	ring := NewHashRing(numReplicas)
	owners := make([]int, numObjects)
	for i := 0; i < numObjects; i++ {
		owners[i] = ring.OwnerExcluding(objectByIndex[i].ID, dead)
	}
	return owners
}

// AssignOwnership applies an ownership mapping to the registry — computed
// locally via BuildOwnershipRing if this replica is the leader, or fetched
// from the leader's WocService.GetObjectOwnership RPC otherwise.
func AssignOwnership(owners []int) {
	for i, owner := range owners {
		if meta, ok := objectByIndex[i]; ok {
			meta.Owner = owner
		}
	}
}

// OwnershipSnapshot returns the current owner of every object, in index
// order, for the leader to serve over WocService.GetObjectOwnership.
func OwnershipSnapshot() []int {
	owners := make([]int, numObjects)
	for i := 0; i < numObjects; i++ {
		owners[i] = objectByIndex[i].Owner
	}
	return owners
}

// ObjectByIndex returns the i-th object's metadata (nil if out of range).
func ObjectByIndex(i int) *ObjectMeta {
	return objectByIndex[i]
}

var (
	fallbackRingOnce sync.Once
	fallbackRing     *HashRing

	deadReplicasMu sync.RWMutex
	deadReplicas   = map[int]bool{}
)

// ringObjectID is the sentinel ObjID used to route a ring-reconfiguration
// command (see RingUpdate) through the normal DependentObject slow-path
// machinery (handleSlowPath / startSlowPathProcessor), which requires every
// command to have an ObjID. It never collides with a real object ID: the
// synthetic pool uses "obj-N" (see InitObjectRegistry) and real keys are
// caller-supplied application data.
const ringObjectID = "__ring__"

// RingUpdate is the payload of a ring-reconfiguration command: a new
// object-ownership assignment (OwnerByIndex, one entry per object in the
// fixed pool - see BuildOwnershipRing) together with the dead-replica set
// it was computed against. Committing this through slow-path consensus
// (rather than applying it locally and best-effort-pushing it to
// followers) guarantees every replica - including the leader that
// proposed it - adopts the new ring at the same global order position;
// see conJobRingReconfig in service.go and applyRingUpdate below.
type RingUpdate struct {
	OwnerByIndex []int
	DeadReplicas []int
}

// applyRingUpdate installs a committed RingUpdate: it's called both by the
// leader (after its own slow-path quorum check passes) and by every
// follower (as part of voting on the broadcast, via applyExecute) so all
// replicas apply the same update at the same point in the consensus order.
func applyRingUpdate(u RingUpdate) {
	AssignOwnership(u.OwnerByIndex)
	SetDeadReplicas(u.DeadReplicas)
	cm.mystate.RecomputeFastThresholds(quorum, toDeadSet(u.DeadReplicas))
}

// SetDeadReplicas updates the set of replicas currently excluded from
// object-ownership consideration (paper §4.2 failure handling). The leader
// calls this when its replica health monitor detects or clears a failure;
// followers call it when applying the leader's disseminated dead-replica
// set (see WocService.GetObjectOwnership). Always replaces the map wholesale
// rather than mutating in place, so a reference grabbed under RLock stays a
// consistent snapshot after the lock is released.
func SetDeadReplicas(dead []int) {
	next := make(map[int]bool, len(dead))
	for _, id := range dead {
		next[id] = true
	}
	deadReplicasMu.Lock()
	deadReplicas = next
	deadReplicasMu.Unlock()
}

// DeadReplicasSnapshot returns the current dead-replica set as a slice, for
// the leader to serve over WocService.GetObjectOwnership.
func DeadReplicasSnapshot() []int {
	deadReplicasMu.RLock()
	defer deadReplicasMu.RUnlock()
	out := make([]int, 0, len(deadReplicas))
	for id := range deadReplicas {
		out = append(out, id)
	}
	return out
}

func isDeadReplica(id int) bool {
	deadReplicasMu.RLock()
	defer deadReplicasMu.RUnlock()
	return deadReplicas[id]
}

// ObjectOwner returns the replica owning objID's fast-path coordination.
// For objects in the synthetic pool (see InitObjectRegistry), this is the
// leader-computed-and-disseminated ring (BuildOwnershipRing/AssignOwnership).
// For real keys outside that pool (e.g. MongoDB/YCSB record IDs), no
// ownership entry exists in objectByID, so every replica falls back to
// computing the same answer locally: NewHashRing(numReplicas) is fully
// deterministic given numReplicas, which every replica already agrees on,
// so no leader round-trip is needed for this case. Without this fallback,
// every replica would default to "I'm the owner" for any real key, and
// routeFastPath would never forward — breaking the single-owner-per-object
// invariant fast-path safety (paper §5, Lemma 2) relies on.
//
// Both paths consult the dead-replica set so a failed owner is never
// handed back, even if the synthetic-pool snapshot hasn't been refreshed
// yet — see SetDeadReplicas.
func ObjectOwner(id string) int {
	deadReplicasMu.RLock()
	dead := deadReplicas
	deadReplicasMu.RUnlock()

	if meta, ok := objectByID[id]; ok && meta.Owner >= 0 && !dead[meta.Owner] {
		return meta.Owner
	}
	fallbackRingOnce.Do(func() {
		fallbackRing = NewHashRing(numOfServers)
	})
	if fallbackRing != nil {
		if owner := fallbackRing.OwnerExcluding(id, dead); owner >= 0 {
			return owner
		}
	}
	return myServerID
}

// ObjectOwnerExcluding is ObjectOwner's routing decision, but additionally
// excludes extraDead — a replica this specific caller just observed as
// unreachable via one failed RPC (see forwardToObjectOwner), on top of
// whatever the shared, health-monitor-disseminated dead-replica set already
// knows about. The health monitor takes ~9-12s to confirm a failure (3
// missed pings at a 3s interval) and then disseminates the new ring through
// a consensus round, so a replica that discovers a dead owner on its own
// would otherwise keep re-forwarding to it (or falling all the way back to
// the slow path) for that whole window. This lets it reroute immediately,
// using the same deterministic hash-ring computation ObjectOwner already
// falls back to for real keys - no leader round-trip or ring update needed,
// since every replica computes the same answer given the same dead set.
func ObjectOwnerExcluding(id string, extraDead map[int]bool) int {
	deadReplicasMu.RLock()
	dead := deadReplicas
	deadReplicasMu.RUnlock()

	combined := make(map[int]bool, len(dead)+len(extraDead))
	for sid := range dead {
		combined[sid] = true
	}
	for sid := range extraDead {
		combined[sid] = true
	}

	fallbackRingOnce.Do(func() {
		fallbackRing = NewHashRing(numOfServers)
	})
	if fallbackRing != nil {
		if owner := fallbackRing.OwnerExcluding(id, combined); owner >= 0 {
			return owner
		}
	}
	return myServerID
}

// classifyRealKey deterministically types a real (non-synthetic) key, e.g.
// a MongoDB/YCSB record ID, as independent or dependent, driven by the same
// -indep ratio that controls the synthetic pool's split (see
// InitObjectRegistry). Hashing the key (rather than rolling a fresh random
// number per access, like PickObjectType) keeps the type static per key, as
// the paper requires: every client and replica computes the same type for
// the same key without coordination.
func classifyRealKey(key string) int {
	if key == "" {
		return DependentObject
	}
	h := crc32.ChecksumIEEE([]byte(key))
	if float64(h%10000)/100.0 < indepRatio {
		return IndependentObject
	}
	return DependentObject
}

// keyOwnerClientIdx deterministically assigns a real key to one of
// numClients client indices, independent of the key's classifyRealKey type.
// It's used only to decide which single client's workload generator treats
// an independent-typed key as "mine" (see client.go), approximating the
// single-writer pattern independent objects are meant to have in practice
// (a user's own cart, a single bank account) without actually depending on
// -indep for the assignment — a different concern from the key's type, kept
// on a separate hash so the two don't correlate.
func keyOwnerClientIdx(key string, numClients int) int {
	if numClients <= 0 {
		return 0
	}
	h := crc32.ChecksumIEEE([]byte("owner-" + key))
	return int(h % uint32(numClients))
}

// PickObjectType returns IndependentObject with probability indepRatio%,
// DependentObject otherwise.
func PickObjectType() int {
	if rand.Float64()*100 < indepRatio {
		return IndependentObject
	}
	return DependentObject
}

// PickObjectOfType returns a uniformly random object of the given type.
func PickObjectOfType(objType int) *ObjectMeta {
	indices := dependentIdx
	if objType == IndependentObject {
		indices = independentIdx
	}
	if len(indices) == 0 {
		return nil
	}
	return objectByIndex[indices[rand.Intn(len(indices))]]
}
