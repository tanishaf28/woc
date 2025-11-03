package smr

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// ---------------- ObjectState ----------------
// Tracks per-object metadata for WOC
type ObjectState struct {
	sync.RWMutex
	ID                string
	ObjType           int              // IndependentObject or CommonObject
	RValue            float64          // Decay factor (1.0 < R < 2.0)
	Weights           map[int]float64  // ReplicaID → weight
	TotalWeight       float64          // Sum of all replica weights
	MaxWeight         float64          // Highest replica weight
	ThresholdFast     float64          // Weighted quorum threshold (fast path)
	LastCommittedOpID string
	LastProposer      int
	Value             interface{}
	LastCommitType    string           // "FAST" or "SLOW"
	LastCommitTime    time.Time
}

type ServerState struct {
	sync.RWMutex
	myID     int
	leaderID int
	term     int
	votedFor bool
	logIndex int
	cmtIndex int
	prioClock int
	Objects  map[string]*ObjectState // ObjectID → ObjectState
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
func (s *ServerState) GenerateRValue(objType int, objID int, numReplicas int) float64 {
	baseOptions := []float64{1.3, 1.5, 1.7, 2.0}
	r := baseOptions[objID%len(baseOptions)]
	variance := 0.95 + 0.1*rand.Float64()
	r *= variance
	if r > 2.0 {
		r = 2.0
	}
	return r
}

func (s *ServerState) AddObject(objID string, objType int, numReplicas int) {
	s.Lock()
	defer s.Unlock()
	if _, exists := s.Objects[objID]; exists {
		return
	}

	rValue := s.GenerateRValue(objType, len(s.Objects), numReplicas)

	obj := &ObjectState{
		ID:      objID,
		ObjType: objType,
		RValue:  rValue,
		Weights: make(map[int]float64),
	}

	obj.GenerateWeights(numReplicas)
	obj.ComputeFastThreshold(numReplicas)
	s.Objects[objID] = obj
}

func (o *ObjectState) GenerateWeights(numReplicas int) {
	o.Lock()
	defer o.Unlock()

	o.Weights = make(map[int]float64, numReplicas)
	o.TotalWeight = 0

	for id := 0; id < numReplicas; id++ {
		w := math.Pow(o.RValue, float64(numReplicas-1-id))
		o.Weights[id] = w
		o.TotalWeight += w
		if w > o.MaxWeight {
			o.MaxWeight = w
		}
	}
}

// Compute fast path threshold: sum of top (quorumSize) weights
func (o *ObjectState) ComputeFastThreshold(quorumSize int) {
	o.Lock()
	defer o.Unlock()

	weights := make([]float64, 0, len(o.Weights))
	for _, w := range o.Weights {
		weights = append(weights, w)
	}
	sort.Slice(weights, func(i, j int) bool { return weights[i] > weights[j] })

	threshold := 0.0
	for i := 0; i < quorumSize && i < len(weights); i++ {
		threshold += weights[i]
	}
	o.ThresholdFast = o.TotalWeight / 2.0
}

// ---------------- Quorum Check ----------------
// Cabinet-style fast path: sum of top received replica weights ≥ ThresholdFast
func (o *ObjectState) HasFastQuorum(replies map[int]float64) bool {
	o.RLock()
	defer o.RUnlock()

	weights := make([]float64, 0, len(replies))
	for rid := range replies {
		if w, ok := o.Weights[rid]; ok {
			weights = append(weights, w)
		}
	}
	sort.Slice(weights, func(i, j int) bool { return weights[i] > weights[j] })

	total := 0.0
	for _, w := range weights {
		total += w
		if total >= o.ThresholdFast {
			return true
		}
	}
	return false
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