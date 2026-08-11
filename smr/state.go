package smr

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------- ObjectState ----------------
// Tracks per-object metadata for WOC
type ObjectState struct {
	sync.RWMutex
	ID          string
	ObjType     int             // IndependentObject or DependentObject
	RValue      float64         // Decay factor (1.0 < R < 2.0)
	Weights     map[int]float64 // ReplicaID → weight
	// weightCache is published via atomic.Pointer rather than protected by
	// the embedded RWMutex: the fast-path broadcast loop reads it once per
	// round, per connection, without taking a lock (see consensus.go), and
	// ReassignWeights now rewrites it live during normal operation (it used
	// to be write-once at AddObject time, when a plain slice + "read it
	// unlocked, nothing ever mutates it again" was safe). atomic.Pointer
	// keeps that lock-free read path correct now that it can change under
	// a live reader: publishing a new slice is a single atomic pointer
	// swap, so readers always see either the old or the new slice in full,
	// never a torn one. Read it via GetWeightCache(); written via Store()
	// inside GenerateWeights/ReassignWeights, both already under o.Lock().
	weightCache atomic.Pointer[[]float64]
	TotalWeight float64 // Sum of all replica weights
	MaxWeight         float64         // Highest replica weight
	ThresholdFast     float64         // Weighted quorum threshold (fast path)
	LastCommittedOpID string
	LastProposer      int
	Value             interface{}
	LastCommitType    string // "FAST" or "SLOW"
	LastCommitTime    time.Time

	// Seq is the sequence number of the value currently held by this object
	// (committed or, for a fast-path provisional apply, not-yet-confirmed).
	// ApplyFastProvisional rejects any seq <= Seq as stale/regressive;
	// RevertIfSeqMatches only undoes a provisional apply if Seq still equals
	// the seq it's reverting (i.e. nothing newer has superseded it).
	Seq int64
	// seqCounter mints new seq numbers via NextSeq. Kept separate from Seq
	// (rather than reusing it) because NextSeq must hand the coordinator a
	// value that will still look newer than Seq once ApplyFastProvisional
	// checks it - if NextSeq advanced Seq directly, ApplyFastProvisional's
	// "seq <= Seq" guard would reject the very seq NextSeq just minted.
	seqCounter int64
	// Prev* hold the value this object held immediately before the current
	// Seq's fast-path provisional apply, so RevertIfSeqMatches can undo it if
	// the coordinator never reaches quorum. Only one level of undo is needed:
	// single-owner-per-object routing plus the coordinator's inFlight map
	// guarantee at most one fast-path round is ever outstanding per object.
	PrevValue      interface{}
	PrevSeq        int64
	PrevCommitType string
}

type ServerState struct {
	sync.RWMutex
	myID      int
	leaderID  int
	term      int
	votedFor  bool
	logIndex  int
	cmtIndex  int
	prioClock int
	Objects   map[string]*ObjectState // ObjectID → ObjectState
}

func NewServerState() *ServerState {
	return &ServerState{
		myID:      0,
		term:      0,
		votedFor:  false,
		logIndex:  0,
		cmtIndex:  0,
		prioClock: 0,
		Objects:   make(map[string]*ObjectState),
	}
}

// ---------------- Server Metadata ----------------
func (s *ServerState) SetMyServerID(id int) {
	s.Lock()
	defer s.Unlock()
	s.myID = id
}
func (s *ServerState) GetMyServerID() int {
	s.RLock()
	defer s.RUnlock()
	return s.myID
}

func (s *ServerState) SetLeaderID(id int) {
	s.Lock()
	defer s.Unlock()
	s.leaderID = id
}
func (s *ServerState) GetLeaderID() int {
	s.RLock()
	defer s.RUnlock()
	return s.leaderID
}

func (s *ServerState) SetTerm(term int) {
	s.Lock()
	defer s.Unlock()
	s.term = term
}
func (s *ServerState) GetTerm() int {
	s.RLock()
	defer s.RUnlock()
	return s.term
}

func (s *ServerState) ResetVotedFor() bool {
	s.Lock()
	defer s.Unlock()
	s.votedFor = false
	return s.votedFor
}
func (s *ServerState) CheckVotedFor() bool {
	s.RLock()
	defer s.RUnlock()
	return s.votedFor
}
func (s *ServerState) SetVotedFor(voted bool) {
	s.Lock()
	defer s.Unlock()
	s.votedFor = voted
}
func (s *ServerState) IsLeader() bool {
	s.RLock()
	defer s.RUnlock()
	return s.myID == s.leaderID
}

func (s *ServerState) SyncLogIndex(logIndex int) {
	s.Lock()
	defer s.Unlock()
	s.logIndex = logIndex
}
func (s *ServerState) GetLogIndex() int {
	s.RLock()
	defer s.RUnlock()
	return s.logIndex
}
func (s *ServerState) AddLogIndex(n int) {
	s.Lock()
	defer s.Unlock()
	s.logIndex += n
}

func (s *ServerState) SyncCommitIndex(commitIndex int) {
	s.Lock()
	defer s.Unlock()
	s.cmtIndex = commitIndex
}
func (s *ServerState) GetCommitIndex() int {
	s.RLock()
	defer s.RUnlock()
	return s.cmtIndex
}
func (s *ServerState) AddCommitIndex(n int) {
	s.Lock()
	defer s.Unlock()
	s.cmtIndex += n
}

func (s *ServerState) GetPrioClock() int {
	s.RLock()
	defer s.RUnlock()
	return s.prioClock
}

// ---------------- Object Management ----------------
// GenerateRValue returns this object's geometric weight ratio, using the
// same search Cabinet's global weights use (calcInitPrioRatio, starting from
// r=2.0 and only descending if 2.0 itself doesn't satisfy the invariants for
// this object's specific quorumSize) rather than picking from a fixed,
// unvalidated set of candidates. The previous per-object-varied picker could
// return an r that never actually satisfied Cabinet's two invariants for
// larger quorumSize/t (e.g. r=1.3 at n=11, t=5 fails the "top t weights stay
// below half" safety check) - silently breaking quorum intersection (paper
// Lemma 1) for that object. Every object with the same (numReplicas,
// quorumSize) now gets the same provably-safe ratio.
func (s *ServerState) GenerateRValue(objType int, objID int, numReplicas int, quorumSize int, ratioTryStep float64) float64 {
	if ratioTryStep <= 0 {
		ratioTryStep = 0.01
	}
	r, err := calcInitPrioRatio(numReplicas, quorumSize, ratioTryStep)
	if err != nil {
		panic(fmt.Errorf("no valid weight ratio for object (numReplicas=%d, quorumSize=%d): %w", numReplicas, quorumSize, err))
	}
	return r
}

// In ServerState — add quorumSize parameter
func (s *ServerState) AddObject(objID string, objType int, numReplicas int, quorumSize int, ratioTryStep float64) {
    s.Lock()
    defer s.Unlock()
    if _, exists := s.Objects[objID]; exists {
        return
    }

    rValue := s.GenerateRValue(objType, len(s.Objects), numReplicas, quorumSize, ratioTryStep)

    obj := &ObjectState{
        ID:      objID,
        ObjType: objType,
        RValue:  rValue,
        Weights: make(map[int]float64),
    }

    obj.GenerateWeights(numReplicas)
    obj.ComputeFastThreshold(quorumSize)  // ← now passes t+1, not numReplicas
    s.Objects[objID] = obj
}

func (o *ObjectState) GenerateWeights(numReplicas int) {
	o.Lock()
	defer o.Unlock()

	o.Weights = make(map[int]float64, numReplicas)
	cache := make([]float64, numReplicas)
	o.TotalWeight = 0

	for id := 0; id < numReplicas; id++ {
		w := math.Pow(o.RValue, float64(numReplicas-1-id))
		o.Weights[id] = w
		cache[id] = w
		o.TotalWeight += w
		if w > o.MaxWeight {
			o.MaxWeight = w
		}
	}
	o.weightCache.Store(&cache)
}

// GetWeightCache returns the object's current per-replica weight array. Safe
// to call without holding any lock - see weightCache's doc comment.
func (o *ObjectState) GetWeightCache() []float64 {
	if p := o.weightCache.Load(); p != nil {
		return *p
	}
	return nil
}

// ComputeFastThreshold sets the object's fast-path commit threshold to the
// paper's literal WT_i = 0.5 * sum(w_O_i) (Eq. 2) - half the object's total
// weight, not a quorumSize-dependent quantity. quorumSize (t+1) doesn't
// enter the threshold itself: the geometric weight-ratio invariant inherited
// from Cabinet already guarantees the top quorumSize weights sum to more
// than half the total, which is what makes a quorum of that size reachable
// at all - see ComputeFastThresholdExcluding for the case where some of
// that weight belongs to a now-dead replica.
func (o *ObjectState) ComputeFastThreshold(quorumSize int) {
    o.Lock()
    defer o.Unlock()
    o.ThresholdFast = o.TotalWeight / 2
}

// ComputeFastThresholdExcluding recomputes the threshold as half the sum of
// only the surviving (non-dead) replicas' weights. Must be called whenever
// the dead-replica set changes (see RecomputeFastThresholds) - a dead
// replica's weight must not stay baked into the threshold forever, or a
// dead high-weight replica can permanently wedge fast-path quorum for this
// object even though enough live replicas remain to satisfy Lemma 4's
// liveness argument.
func (o *ObjectState) ComputeFastThresholdExcluding(quorumSize int, dead map[int]bool) {
    o.Lock()
    defer o.Unlock()

    liveTotal := 0.0
    for id, w := range o.Weights {
        if dead[id] {
            continue
        }
        liveTotal += w
    }
    o.ThresholdFast = liveTotal / 2
}

// RecomputeFastThresholds recalculates every known object's fast-path quorum
// threshold against the current dead-replica set. Must be called whenever
// that set changes (see consensus.go's startReplicaHealthMonitor), or fast
// path can permanently lock up - see ComputeFastThresholdExcluding.
func (s *ServerState) RecomputeFastThresholds(quorumSize int, dead map[int]bool) {
	s.RLock()
	objs := make([]*ObjectState, 0, len(s.Objects))
	for _, o := range s.Objects {
		objs = append(objs, o)
	}
	s.RUnlock()

	for _, o := range objs {
		o.ComputeFastThresholdExcluding(quorumSize, dead)
	}
}

// ---------------- Object Commit ----------------
func (s *ServerState) UpdateObjectCommit(objID string, proposer int, value interface{}, pathUsed string) {
	s.Lock()
	defer s.Unlock()
	if obj, ok := s.Objects[objID]; ok {
		obj.Lock()
		defer obj.Unlock()
		obj.LastProposer = proposer
		obj.Value = value
		obj.LastCommitType = pathUsed
		obj.LastCommitTime = time.Now()
	}
}

// ---------------- Fast-Path Provisional Apply / Fallback ----------------

// NextSeq mints and returns a new seq number for this object, guaranteed
// greater than the seq of any value applied so far. The fast-path
// coordinator calls this once per attempt, mirroring the paper's CORA-CT
// "s <- s+1" (Algorithm 1, line 4).
func (o *ObjectState) NextSeq() int64 {
	o.Lock()
	defer o.Unlock()
	if o.seqCounter < o.Seq {
		o.seqCounter = o.Seq
	}
	o.seqCounter++
	return o.seqCounter
}

// ApplyFastProvisional applies a fast-path broadcast's value under a
// monotonicity guard: a proposal whose seq does not exceed the object's
// current seq is stale/regressive and is rejected (no-op, returns false).
// On acceptance, the previously-current value/seq/commit-type are stashed
// into Prev* so RevertIfSeqMatches can undo this apply if the coordinator
// never reaches quorum for this seq.
func (o *ObjectState) ApplyFastProvisional(proposer int, value interface{}, seq int64) bool {
	o.Lock()
	defer o.Unlock()
	if seq <= o.Seq {
		return false
	}
	o.PrevValue = o.Value
	o.PrevSeq = o.Seq
	o.PrevCommitType = o.LastCommitType

	o.Value = value
	o.Seq = seq
	o.LastProposer = proposer
	o.LastCommitType = "FAST"
	o.LastCommitTime = time.Now()
	return true
}

// RevertIfSeqMatches undoes a fast-path provisional apply for the given seq,
// but only if that seq is still current (i.e. no newer commit has since
// superseded it). Called when the coordinator broadcasts a fallback after
// failing to reach fast-path quorum. A no-op (returns false) if the object
// has already moved past seq, which happens whenever another replica's
// FallBack/next commit got there first, or this message arrived late.
func (o *ObjectState) RevertIfSeqMatches(seq int64) bool {
	o.Lock()
	defer o.Unlock()
	if o.Seq != seq {
		return false
	}
	o.Value = o.PrevValue
	o.Seq = o.PrevSeq
	o.LastCommitType = o.PrevCommitType
	return true
}

// ---------------- Object Helpers ----------------
func (s *ServerState) GetObject(objID string) *ObjectState {
	s.RLock()
	defer s.RUnlock()
	return s.Objects[objID]
}

func (s *ServerState) RegisterObject(obj *ObjectState) {
	s.Lock()
	defer s.Unlock()
	if _, exists := s.Objects[obj.ID]; !exists {
		s.Objects[obj.ID] = obj
	}
}

// DeleteObject removes an object from state (garbage collection)
func (s *ServerState) DeleteObject(objID string) {
	s.Lock()
	defer s.Unlock()
	if obj, exists := s.Objects[objID]; exists {
		obj.Lock()
		obj.Value = nil // Clear payload
		obj.Unlock()
		delete(s.Objects, objID)
	}
}

func (o *ObjectState) GetReplicaWeight(replicaID int) float64 {
	o.RLock()
	defer o.RUnlock()
	if w, ok := o.Weights[replicaID]; ok {
		return w
	}
	return 1.0
}

func (o *ObjectState) ObjWeight() float64 {
	o.RLock()
	defer o.RUnlock()
	return o.TotalWeight
}

func (o *ObjectState) RecomputeObjectWeights(newR float64, numReplicas int, quorumSize int) {
	o.Lock()
	defer o.Unlock()
	o.RValue = newR
	o.GenerateWeights(numReplicas)
	o.ComputeFastThreshold(quorumSize)
}

// ReassignWeights re-ranks which replica holds which weight value for this
// object, based on the order replicas actually responded in the most recent
// fast-path round — mirroring Cabinet's PriorityManager.UpdateFollowerPriorities
// for the slow path's global weight vector, but per-object. The coordinator
// always keeps the top weight; each responder in arrivalOrder gets the next
// highest; any replica that didn't respond in time gets the leftover (lowest)
// weights, assigned in sorted-ID order for determinism.
//
// This only permutes which replica ID holds which of the existing weight
// values - the value multiset itself (the geometric decay sequence from
// GenerateWeights) is unchanged. Because of that, the sum of any top-K
// weights is invariant under the permutation, so TotalWeight, MaxWeight, and
// ThresholdFast all stay correct without being recomputed here.
func (o *ObjectState) ReassignWeights(arrivalOrder []int, coordinatorID int, numReplicas int) {
	o.Lock()
	defer o.Unlock()

	current := o.GetWeightCache()
	if len(current) < numReplicas {
		return // cache not sized as expected; leave weights untouched
	}

	scheme := make([]float64, numReplicas)
	copy(scheme, current[:numReplicas])
	sort.Slice(scheme, func(i, j int) bool { return scheme[i] > scheme[j] })

	newWeights := make(map[int]float64, numReplicas)
	arranged := make(map[int]bool, numReplicas)
	schemeIdx := 0

	assign := func(replicaID int) {
		if arranged[replicaID] || schemeIdx >= len(scheme) {
			return
		}
		newWeights[replicaID] = scheme[schemeIdx]
		arranged[replicaID] = true
		schemeIdx++
	}

	assign(coordinatorID)
	for _, replicaID := range arrivalOrder {
		assign(replicaID)
	}

	unresponded := make([]int, 0, numReplicas)
	for id := 0; id < numReplicas; id++ {
		if !arranged[id] {
			unresponded = append(unresponded, id)
		}
	}
	sort.Ints(unresponded)
	for _, id := range unresponded {
		assign(id)
	}

	o.Weights = newWeights
	cache := make([]float64, numReplicas)
	for id, w := range newWeights {
		cache[id] = w
	}
	o.weightCache.Store(&cache)
}
