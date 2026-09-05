package main

import (
	"encoding/binary"
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
	var buf [8]byte
	for replicaID := 0; replicaID < numReplicas; replicaID++ {
		binary.BigEndian.PutUint32(buf[0:4], uint32(replicaID))
		for v := 0; v < virtualNodesPerReplica; v++ {
			binary.BigEndian.PutUint32(buf[4:8], uint32(v))
			pos := crc32.ChecksumIEEE(buf[:])
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

func BuildOwnershipRing(numReplicas int, dead map[int]bool) []int {
	ring := NewHashRing(numReplicas)
	owners := make([]int, numObjects)
	for i := 0; i < numObjects; i++ {
		owners[i] = ring.OwnerExcluding(objectByIndex[i].ID, dead)
	}
	return owners
}


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
	deadReplicasSlice = []int{}
)

const ringObjectID = "__ring__"


type RingUpdate struct {
	OwnerByIndex []int
	DeadReplicas []int
}

func applyRingUpdate(u RingUpdate) {
	AssignOwnership(u.OwnerByIndex)
	SetDeadReplicas(u.DeadReplicas)
	cm.mystate.RecomputeFastThresholds(quorum, toDeadSet(u.DeadReplicas))
}

func SetDeadReplicas(dead []int) {
	next := make(map[int]bool, len(dead))
	nextSlice := make([]int, 0, len(dead))
	for _, id := range dead {
		if !next[id] {
			next[id] = true
			nextSlice = append(nextSlice, id)
		}
	}
	deadReplicasMu.Lock()
	deadReplicas = next
	deadReplicasSlice = nextSlice
	deadReplicasMu.Unlock()
}


func DeadReplicasSnapshot() []int {
	deadReplicasMu.RLock()
	defer deadReplicasMu.RUnlock()
	return deadReplicasSlice
}

func isDeadReplica(id int) bool {
	deadReplicasMu.RLock()
	defer deadReplicasMu.RUnlock()
	return deadReplicas[id]
}


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
