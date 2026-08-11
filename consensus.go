package main

import (
	"context"
	"sync"
	"sync/atomic"
	"math/rand"
	"time"
	"fmt"
	"os"
	"woc/eval"
	"woc/mongodb"
	"woc/smr"
)

var consensusLatencyDebug = os.Getenv("LATENCY_DEBUG") == "true"
type SlowPathRequest struct {
	Cmd        Command
	ReplyChan  chan SlowPathResult
	ReceivedAt time.Time
}

type SlowPathResult struct {
	Success  bool
	PathUsed string
}

var slowPathQueue = make(chan *SlowPathRequest, 10000)

// slowPathQueueHighWaterMark and slowPathDeadline bound how much work
// handleSlowPath will let pile up. Without them, a single stuck consensus
// round (e.g. a quorum that can never be reached once enough replicas are
// dead) backs up startSlowPathProcessor's one serialized goroutine, and
// every caller of handleSlowPath blocks forever on slowPathQueue<- or
// <-req.ReplyChan both unbuffered against time. Under load that turned
// into hundreds of thousands of permanently-blocked RPC-handler goroutines,
// which starved the Go scheduler badly enough that unrelated goroutines
// (including the replica health-monitor's pings) started missing their own
// deadlines, falsely declaring live replicas dead too.
const (
	slowPathQueueHighWaterMark = 500
	slowPathDeadline           = 8 * time.Second
)


type NetworkMetrics struct {
	mu         sync.Mutex
	avgRTT     time.Duration
	rttSamples []time.Duration
	maxSamples int
}

var netMetrics = &NetworkMetrics{
	avgRTT:     100 * time.Millisecond, // Conservative default
	rttSamples: make([]time.Duration, 0, 100),
	maxSamples: 100,
}

func (nm *NetworkMetrics) RecordRTT(rtt time.Duration) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	
	nm.rttSamples = append(nm.rttSamples, rtt)
	if len(nm.rttSamples) > nm.maxSamples {
		nm.rttSamples = nm.rttSamples[1:] // Keep last 100
	}
	
	var sum time.Duration
	for _, sample := range nm.rttSamples {
		sum += sample
	}
	nm.avgRTT = sum / time.Duration(len(nm.rttSamples))
}

func (nm *NetworkMetrics) GetTimeout() time.Duration {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	
	timeout := nm.avgRTT * 3
	
	// Bounds: 50ms minimum, 5s maximum
	if timeout < 50*time.Millisecond {
		timeout = 50 * time.Millisecond
	}
	if timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	
	return timeout
}

type ConsensusManager struct {
	mu           sync.Mutex  // For election/voting/leader state only (NOT for inFlight!)
	pmgr         *smr.PriorityManager
	pstate       *smr.PriorityState
	mystate      *smr.ServerState
	inFlight     sync.Map  //
	// objectWriteLocks holds one *sync.Mutex per ObjID, giving the fast path
	// a real per-object FIFO queue (paper §4.3: "a write that arrives while
	// another is in progress is enqueued... proceeds only after the
	// preceding write completes"), instead of inFlight's previous
	// reject-and-reroute-to-slow-path behavior on any write-write overlap.
	// See objectWriteLock and handleFastPath.
	objectWriteLocks sync.Map
	globalClock  int64
	clockMu      sync.Mutex
	serverPerfM  *eval.PerfMeter
	priorityUpdateQueue chan priorityUpdateTask
	weightReassignQueue chan weightReassignTask
	slowPathProcessorOnce sync.Once
}

// ensureSlowPathProcessorStarted launches startSlowPathProcessor exactly
// once per process lifetime, on whichever replica first becomes leader -
// whether that's at startup (main.go's hardcoded initial leader) or later,
// via a successful election (StartElection). Without this, only the
// startup-time leader ever had a goroutine consuming slowPathQueue: a
// replica that WINS an election later still had handleSlowPath enqueueing
// into it, but nothing was ever reading the other end, so every dependent-
// object write silently sat in the channel until the caller's own
// slowPathDeadline gave up - a permanent liveness loss for the slow path
// after any leader change, regardless of the election itself succeeding.
// Once started, the processor's own for{} loop runs forever, idling on
// <-slowPathQueue whenever this replica isn't the current leader (nothing
// enqueues to it then), so leaving it running after a later leadership loss
// is harmless.
func (cm *ConsensusManager) ensureSlowPathProcessorStarted() {
	cm.slowPathProcessorOnce.Do(func() {
		go cm.startSlowPathProcessor()
		log.Infof("Server %d: slow path processor started", cm.mystate.GetMyServerID())
	})
}

// weightReassignTask carries one fast-path round's arrival-order result to
// weightReassignWorker, which re-ranks an object's weights accordingly (see
// ObjectState.ReassignWeights). Modeled on priorityUpdateTask/
// priorityUpdateQueue's async-queue-plus-worker shape, but not the same
// queue: the per-object arrivalOrder payload doesn't fit priorityUpdateTask,
// which is a per-clock/global-leader-election shape.
type weightReassignTask struct {
	objID         string
	arrivalOrder  []int
	coordinatorID int
	numReplicas   int
}

func NewConsensusManager(state *smr.ServerState, pmgr *smr.PriorityManager, pstate *smr.PriorityState) *ConsensusManager {
	cm := &ConsensusManager{
		mystate:             state,
		pmgr:                pmgr,
		pstate:              pstate,
		globalClock:         0,
		priorityUpdateQueue: make(chan priorityUpdateTask, 10000),
		weightReassignQueue: make(chan weightReassignTask, 10000),
	}
	serverID := state.GetMyServerID()
	fileName := fmt.Sprintf("s%d_n%d_f%d_b%d_%s", serverID, numOfServers, quorum, batchsize, suffix)
	cm.serverPerfM = &eval.PerfMeter{}
	cm.serverPerfM.Init(1, batchsize, fileName)

	go cm.priorityUpdateWorker()
	go cm.weightReassignWorker()
	cm.startCleanupRoutine()

	return cm
}


func (cm *ConsensusManager) IncrementGlobalClock() int64 {
	cm.clockMu.Lock()
	defer cm.clockMu.Unlock()
	cm.globalClock++
	return cm.globalClock
}

func (cm *ConsensusManager) GetGlobalClock() int64 {
	cm.clockMu.Lock()
	defer cm.clockMu.Unlock()
	return cm.globalClock
}

// Leader election's wire types are VoteArgs/Reply (service.go), matching
// WocService.RequestVote's actual registered signature - see StartElection.

type Command struct {
	ClientID    int
	ClientClock int
	CmdType     CmdType
	ReadMode    int
	ObjID       string
	ObjType     int
	Payload     interface{}
	Timestamp   time.Time
	ForwardedBy int
	// AlreadyForwarded marks a command whose fast-path attempt (direct or
	// via owner-forward) already failed once and was handed to the slow
	// path. Without this, a non-leader that forwards such a command to the
	// leader gets it routed straight back into routeFastPath (HandleCommand
	// dispatches purely on ObjType), which re-derives the same owner and
	// can forward it right back - looping forever between two replicas
	// that keep disagreeing about who should coordinate it. See
	// fallbackToSlowPath and HandleCommand's IndependentObject case.
	AlreadyForwarded bool
	// ObjIDs/ObjTypes carry every element's real object when this command
	// represents more than one op bundled onto one wire message. Two
	// distinct meanings share these fields, disambiguated by MultiObject:
	//   - MultiObject == true: one atomic transaction spanning ObjIDs
	//     together - always routed dependent/slow-path regardless of the
	//     individual objects' types (paper §3.1: touching >1 object makes a
	//     transaction dependent even if every object is independent).
	//   - MultiObject == false: N independent single-object ops bundled
	//     only for wire efficiency (client's "object-specific"/"single_obj"/
	//     "mixed" compositions) - see HandleCommand's dispatch into
	//     handleIndependentBatch, which fans these out into N separate
	//     per-object consensus rounds instead of evaluating one shared round
	//     against only the first object's weight vector/threshold.
	// ObjID/ObjType above stay set to the first object for logging/
	// inFlight-keying compatibility with existing single-object code.
	ObjIDs      []string
	ObjTypes    []int
	MultiObject bool
}

type RequestBatch struct {
	requests []*Command
	replies  []chan *BatchReply
	timer    *time.Timer
	mu       sync.Mutex
}

type BatchReply struct {
	Success  bool
	PathUsed string
}


var (
	batchMu          sync.Mutex
	pendingBatch     *RequestBatch
	batchTimeout     = 500 * time.Microsecond // Accumulate for 500μs
	maxBatchSize     = 10                      // Max requests per consensus round
	serverBatchingEnabled = os.Getenv("SERVER_BATCHING") == "true"
)

type ReplyInfo struct {
	Reply   Reply
	ServerID int
	Latency float64
	ClientClock int
}

type priorityResponse struct {
    serverID int
    priority float64
}

type priorityUpdateTask struct {
	clock    int
	queue    chan int
	leaderID int
}

func (cm *ConsensusManager) HandleCommand(cmd Command) (bool, string) {
	if serverBatchingEnabled && cm.mystate.IsLeader() {
		return cm.handleCommandBatched(cmd)
	}

	// A non-atomic batch of >1 distinct objects (see Command.ObjIDs' doc):
	// fan out one independent consensus round per object instead of
	// evaluating a single shared round against only the first object's
	// weight vector/threshold/sequence number.
	if !cmd.MultiObject && hasDistinctObjects(cmd.ObjIDs) {
		_, ok, path := cm.handleIndependentBatch(cmd)
		return ok, path
	}

	var ok bool
	var path string
	switch cmd.ObjType {
	case IndependentObject:
		if cmd.AlreadyForwarded {
			// A fast-path attempt for this command already failed elsewhere
			// (see fallbackToSlowPath) - go straight to the slow path
			// instead of re-deriving the owner via routeFastPath, which is
			// what was bouncing commands between replicas forever.
			ok, path = cm.handleSlowPath(cmd)
			break
		}

		globalClock := int(cm.IncrementGlobalClock())
		cm.serverPerfM.RecordStarter(globalClock)

		ok, path = cm.routeFastPath(cmd)
		if ok && path == "FAST" {
			cm.serverPerfM.RecordFinisher(globalClock)
			cm.serverPerfM.IncFastPath(globalClock)
		} else {
			// Either the request failed outright, or the fast-path attempt
			// timed out and fell back to the slow path (see
			// fallbackToSlowPath): in the fallback case,
			// startSlowPathProcessor already recorded its own complete,
			// queue-wait-inclusive entry for this same operation under a
			// separate globalClock. Finalizing and classifying this outer
			// entry too would double-count one logical operation as two
			// completed rounds - inflating SaveToFile's throughput and
			// attributing the slow (or failed) latency to the fast-path
			// bucket. Discard it; the inner entry (or nothing, on outright
			// failure) is authoritative.
			cm.serverPerfM.DiscardRecord(globalClock)
		}
	case DependentObject:
		ok, path = cm.handleSlowPath(cmd)
	default:
		log.Errorf("unknown object type: %v", cmd.ObjType)
		return false, "INVALID"
	}
	if ok {
		log.Debugf("[HANDLE] ObjID=%s | Path=%s | Success=%v", cmd.ObjID, path, ok)
	} else {
		log.Warnf("[COMMIT FAIL] ObjID=%s | Path=%s", cmd.ObjID, path)
	}
	return ok, path
}

// HandleCommandDetailed is HandleCommand's per-element variant, for callers
// (conJobMongoDB) that need to know exactly which batch elements committed
// so they can write only the corresponding documents, instead of gating an
// all-or-nothing external write on whether every element in the batch
// happened to succeed. Returns a nil element slice for anything that isn't a
// split batch (single-object commands, and atomic MultiObject transactions),
// signaling the caller to fall back to its existing whole-batch handling.
func (cm *ConsensusManager) HandleCommandDetailed(cmd Command) ([]BatchElementResult, bool, string) {
	if !cmd.MultiObject && hasDistinctObjects(cmd.ObjIDs) {
		return cm.handleIndependentBatch(cmd)
	}
	ok, path := cm.HandleCommand(cmd)
	return nil, ok, path
}

// BatchElementResult reports one element's outcome from a split
// independent-object batch (see handleIndependentBatch). Index is the
// element's position in the original batch's payload/ObjIDs slices, so a
// caller can pick out just the corresponding query/payload for a follow-up
// write.
type BatchElementResult struct {
	ObjID string
	Index int
	OK    bool
	Path  string
}

// handleIndependentBatch splits cmd into one HandleCommand call per element
// of cmd.ObjIDs/cmd.ObjTypes/payload. A batch produced by the client's
// "object-specific"/"single_obj"/"mixed" compositions bundles N independent
// single-object operations onto one wire message purely for round-trip
// efficiency - each element is its own independent transaction per the
// paper's §3.1 definition, not one transaction spanning multiple objects
// (that's MultiObject, excluded from this path by HandleCommand's dispatch
// check). Evaluating the old shared single round - keyed entirely off the
// batch's first object - either silently applied every element's payload to
// that one object, or, had the write been indexed correctly, would still
// have evaluated quorum against only the first object's weight
// vector/threshold, violating quorum intersection (paper Lemma 1) for every
// other object in the batch. Each sub-round here gets its own object's
// routing, ownership, weight vector, threshold, and sequence number, exactly
// as a standalone single-object call would produce.
func (cm *ConsensusManager) handleIndependentBatch(cmd Command) ([]BatchElementResult, bool, string) {
	n := len(cmd.ObjIDs)
	results := make([]BatchElementResult, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			objType := cmd.ObjType
			if idx < len(cmd.ObjTypes) {
				objType = cmd.ObjTypes[idx]
			}
			sub := Command{
				ClientID:         cmd.ClientID,
				ClientClock:      cmd.ClientClock,
				ObjID:            cmd.ObjIDs[idx],
				ObjType:          objType,
				CmdType:          cmd.CmdType,
				ForwardedBy:      cmd.ForwardedBy,
				AlreadyForwarded: cmd.AlreadyForwarded,
			}
			switch v := cmd.Payload.(type) {
			case [][]byte:
				if idx < len(v) {
					sub.Payload = [][]byte{v[idx]}
				}
			case []mongodb.Query:
				if idx < len(v) {
					sub.Payload = []mongodb.Query{v[idx]}
				}
			}
			ok, path := cm.HandleCommand(sub)
			results[idx] = BatchElementResult{ObjID: sub.ObjID, Index: idx, OK: ok, Path: path}
		}(i)
	}
	wg.Wait()

	allOK, allFast, allSlow := true, true, true
	for _, r := range results {
		if !r.OK {
			allOK = false
		}
		if r.Path != "FAST" {
			allFast = false
		}
		if r.Path != "SLOW" {
			allSlow = false
		}
	}
	path := "MIXED"
	switch {
	case allFast:
		path = "FAST"
	case allSlow:
		path = "SLOW"
	}
	return results, allOK, path
}

// hasDistinctObjects reports whether ids contains more than one distinct
// value. A batch where every element targets the same object (the
// "single_obj" composition) is already handled correctly by the existing
// single-object path (every element commits against the same ObjectState in
// sequence via a single shared round), so it's deliberately excluded from
// handleIndependentBatch's per-element fan-out, which would otherwise make
// the batch's own elements race each other through that one object's
// inFlight lock and spuriously reroute most of them to the slow path as
// write-write "conflicts".
func hasDistinctObjects(ids []string) bool {
	if len(ids) < 2 {
		return false
	}
	first := ids[0]
	for _, id := range ids[1:] {
		if id != first {
			return true
		}
	}
	return false
}

// handleCommandBatched accumulates requests and processes them in batches
func (cm *ConsensusManager) handleCommandBatched(cmd Command) (bool, string) {
	batchMu.Lock()
	if pendingBatch == nil {
		pendingBatch = &RequestBatch{
			requests: make([]*Command, 0, maxBatchSize),
			replies:  make([]chan *BatchReply, 0, maxBatchSize),
		}
		
		// Start timer to flush batch after timeout
		pendingBatch.timer = time.AfterFunc(batchTimeout, func() {
			cm.flushBatch()
		})
	}
	
	// Add request to batch
	replyChan := make(chan *BatchReply, 1)
	cmdCopy := cmd
	pendingBatch.requests = append(pendingBatch.requests, &cmdCopy)
	pendingBatch.replies = append(pendingBatch.replies, replyChan)
	
	// Flush immediately if batch full
	if len(pendingBatch.requests) >= maxBatchSize {
		pendingBatch.timer.Stop()
		batchMu.Unlock()
		cm.flushBatch()
	} else {
		batchMu.Unlock()
	}
	
	// Wait for batch result
	reply := <-replyChan
	return reply.Success, reply.PathUsed
}

// flushBatch processes accumulated requests in a single consensus round
func (cm *ConsensusManager) flushBatch() {
	batchMu.Lock()
	batch := pendingBatch
	pendingBatch = nil
	batchMu.Unlock()
	
	if batch == nil || len(batch.requests) == 0 {
		return
	}
	
	//  MEASURE CONSENSUS ROUND (not individual requests)
	globalClock := int(cm.IncrementGlobalClock())
	cm.serverPerfM.RecordStarter(globalClock)
	defer cm.serverPerfM.RecordFinisher(globalClock)
	
	if latencyDebug {
		log.Debugf("[SERVER-BATCH] Flushing batch of %d requests", len(batch.requests))
	}
	
	// Classify requests by object type
	fastReqs := make([]*Command, 0)
	slowReqs := make([]*Command, 0)

	for _, req := range batch.requests {
		switch req.ObjType {
		case IndependentObject:
			fastReqs = append(fastReqs, req)
		case DependentObject:
			slowReqs = append(slowReqs, req)
		}
	}

	// Process each type in batch
	var wg sync.WaitGroup
	results := make([]BatchReply, len(batch.requests))

	// Fast path requests (parallel, ownership-routed - see routeFastPath)
	if len(fastReqs) > 0 {
		wg.Add(len(fastReqs))
		for i, req := range fastReqs {
			go func(idx int, cmd *Command) {
				defer wg.Done()
				ok, path := cm.routeFastPath(*cmd)
				results[idx] = BatchReply{Success: ok, PathUsed: path}
			}(i, req)
		}
	}

	// Slow path requests (sequential through leader)
	if len(slowReqs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, req := range slowReqs {
				ok, path := cm.handleSlowPath(*req)
				results[len(fastReqs)+i] = BatchReply{Success: ok, PathUsed: path}
			}
		}()
	}

	wg.Wait()

	// Record per-batch metrics for this consensus round, classified by each
	// request's actual outcome (results[i].PathUsed) - not by which list it
	// started in. A fastReq whose fast-path attempt timed out and fell back
	// to the slow path (see fallbackToSlowPath) comes back with
	// PathUsed=="SLOW"; counting it toward IncFastPath here regardless would
	// misattribute it (this batch's row would report it as fast-path).
	for i := range fastReqs {
		if results[i].PathUsed == "FAST" {
			cm.serverPerfM.IncFastPath(globalClock)
		} else {
			cm.serverPerfM.IncSlowPath(globalClock)
		}
	}
	for i := len(fastReqs); i < len(fastReqs)+len(slowReqs); i++ {
		cm.serverPerfM.IncSlowPath(globalClock)
	}

	// Send results to all waiting clients
	for i, replyChan := range batch.replies {
		replyChan <- &results[i]
	}

	if latencyDebug {
		log.Debugf("[SERVER-BATCH] Completed batch of %d requests (fast=%d, slow=%d)",
			len(batch.requests), len(fastReqs), len(slowReqs))
	}
}

func (cm *ConsensusManager) prepareArgs(cmd Command, prioClock int) *Args {
    args := &Args{
        ClientID:  cmd.ClientID,
		ClientClock: cmd.ClientClock,
        ObjID:     cmd.ObjID,
        ObjType:   cmd.ObjType,
        CmdType:   cmd.CmdType,
        ReadMode:  cmd.ReadMode,
        Type:      evalType,
        PrioClock: prioClock,
        AlreadyForwarded: cmd.AlreadyForwarded,
    }
	if len(cmd.ObjIDs) > 1 {
		args.ObjIDs = cmd.ObjIDs
		args.ObjTypes = cmd.ObjTypes
		args.MultiObject = true
	}
	if cm != nil && cm.pstate != nil {
        _, prio := cm.pstate.GetPriority()
        args.PrioVal = prio
    }
    switch v := cmd.Payload.(type) {
    case []mongodb.Query:
        args.Type = MongoDB
        args.CmdMongo = v
    case [][]byte:
        args.Type = PlainMsg
        args.CmdPlain = v
    case RingUpdate:
        args.Type = RingReconfig
        args.RingOwners = v.OwnerByIndex
        args.RingDead = v.DeadReplicas
    case nil:
        // Reads carry no write payload.
        args.Type = evalType
    default:
        log.Warnf("prepareArgs: unknown payload type %T for ObjID=%s", v, cmd.ObjID)
    }
    return args
}

// routeFastPath enforces object-mapping ownership (paper §4.2): only the
// replica that owns ObjID on the hash ring coordinates its fast path.
// Non-owners forward the request instead of coordinating it themselves.
func (cm *ConsensusManager) routeFastPath(cmd Command) (bool, string) {
	owner := ObjectOwner(cmd.ObjID)
	if owner == cm.mystate.GetMyServerID() {
		return cm.handleFastPath(cmd)
	}
	return cm.forwardToObjectOwner(cmd, owner)
}

// fallbackToSlowPath routes a command to the slow path after a fast-path
// attempt (direct, or via forwardToObjectOwner) has already failed for it.
// It marks the command AlreadyForwarded so that if this replica isn't the
// leader and forwards it on, the leader's HandleCommand won't re-derive
// ownership and retry fast-path routing - which previously could bounce a
// command between two replicas forever whenever they disagreed about who
// should coordinate it (e.g. mid-failover, once enough replicas are dead
// that fast-path quorum is no longer reachable for some objects).
func (cm *ConsensusManager) fallbackToSlowPath(cmd Command) (bool, string) {
	cmd.AlreadyForwarded = true
	return cm.handleSlowPath(cmd)
}

// broadcastFallBack best-effort-notifies every replica that the fast-path
// coordinator failed to reach quorum for (objID, seq), so anyone still
// holding that seq's provisional apply should revert it (see
// ObjectState.RevertIfSeqMatches, conJobFallBack in service.go). Errors are
// intentionally ignored: unlike RingReconfig, this doesn't need cluster
// agreement to be safe - a replica that misses this message self-corrects
// the moment a real commit (higher seq) lands for the same object.
func (cm *ConsensusManager) broadcastFallBack(objID string, seq int64) {
	args := &Args{Type: FallBack, ObjID: objID, Seq: seq}
	conns.RLock()
	defer conns.RUnlock()
	for _, conn := range conns.m {
		go func(c *ServerDock) {
			reply := Reply{}
			_ = c.txClient.Call("WocService.ConsensusService", args, &reply)
		}(conn)
	}
}

// broadcastMongoConfirm best-effort-notifies every other replica that a
// MongoDB fast-path round for objID actually reached quorum, so each should
// now physically apply queries to its own local MongoDB (see
// applyExecute's MongoDB/FastProvisional case, which deliberately skipped
// the real write during the vote, and conJobMongoConfirm). Errors are
// intentionally ignored, same as broadcastFallBack: no cluster agreement is
// needed to apply a value every recipient already voted yes for, and a
// replica that misses this message just serves stale reads for objID until
// its next write, never an incorrect commit.
func (cm *ConsensusManager) broadcastMongoConfirm(objID string, queries []mongodb.Query) {
	args := &Args{Type: MongoConfirm, ObjID: objID, CmdMongo: queries}
	conns.RLock()
	defer conns.RUnlock()
	for _, conn := range conns.m {
		go func(c *ServerDock) {
			reply := Reply{}
			_ = c.txClient.Call("WocService.ConsensusService", args, &reply)
		}(conn)
	}
}


// callObjectOwnerRPC invokes WocService.ConsensusService on conn with a
// bounded wait. A bare conn.txClient.Call can block indefinitely if the
// remote process is dead but the TCP connection hasn't yet surfaced an
// error - conns.go's rpc.Dial sets no keepalive or deadline, so that's not
// guaranteed to happen quickly. Every other RPC call site in this file
// already bounds its wait (Ping/election with a fixed 2s, handleFastPath's
// broadcast with netMetrics.GetTimeout()); this brings the owner-forwarding
// path in line with that instead of being the one place a single dead
// object-owner can stall a request-handling goroutine for an unbounded
// amount of time.
func callObjectOwnerRPC(conn *ServerDock, args *Args, reply *Reply) error {
	done := make(chan error, 1)
	go func() {
		done <- conn.txClient.Call("WocService.ConsensusService", args, reply)
	}()
	timeout := netMetrics.GetTimeout()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("object-owner RPC timed out after %v", timeout)
	}
}

// forwardToObjectOwner relays an independent-object command to its owning
// replica instead of coordinating it locally. Mirrors forwardToLeader, with
// one difference: PrioVal is explicitly zeroed because prepareArgs would
// otherwise stamp it with this replica's own (non-zero) priority, which
// would trip the "follower voting" immediate-ack short-circuit in
// service.go and skip real consensus on the owner's side.
func (cm *ConsensusManager) forwardToObjectOwner(cmd Command, ownerID int) (bool, string) {
	if isDeadReplica(ownerID) {
		log.Warnf("[OBJECT-MAP] owner %d already known dead for %s, rerouting locally", ownerID, cmd.ObjID)
		return cm.rerouteAroundDeadOwner(cmd, ownerID)
	}

	conns.RLock()
	ownerConn, ok := conns.m[ownerID]
	conns.RUnlock()
	if !ok {
		log.Warnf("[OBJECT-MAP] owner %d unreachable for %s, rerouting locally", ownerID, cmd.ObjID)
		return cm.rerouteAroundDeadOwner(cmd, ownerID)
	}

	args := cm.prepareArgs(cmd, 0)
	args.PrioVal = 0
	args.ForwardedBy = cm.mystate.GetMyServerID()
	reply := &Reply{}
	if err := callObjectOwnerRPC(ownerConn, args, reply); err != nil {
		log.Errorf("[OBJECT-MAP] forward to owner %d failed for %s: %v - rerouting locally", ownerID, cmd.ObjID, err)
		return cm.rerouteAroundDeadOwner(cmd, ownerID)
	}
	if reply.ErrorMsg != nil {
		log.Errorf("[OBJECT-MAP] owner %d returned error for %s: %v - falling back to slow path", ownerID, cmd.ObjID, reply.ErrorMsg)
		return cm.fallbackToSlowPath(cmd)
	}
	return reply.Success, reply.PathUsed
}

// rerouteAroundDeadOwner is the cheap, local repair for a single dead
// object-owner: rather than waiting for the leader's health monitor to
// confirm the failure (~9-12s: 3 missed pings at a 3s interval) and commit
// a cluster-wide ring update through consensus, it immediately recomputes
// ownership excluding just this observed-dead replica and retries once
// against whichever replica the ring now assigns - every replica computes
// the same answer independently, so no coordination is needed for this
// retry. Bounded to one extra hop: if that retry also fails, it falls back
// to the slow path exactly as before, so a genuinely degraded cluster still
// makes progress instead of retrying forever.
func (cm *ConsensusManager) rerouteAroundDeadOwner(cmd Command, deadOwnerID int) (bool, string) {
	nextOwner := ObjectOwnerExcluding(cmd.ObjID, map[int]bool{deadOwnerID: true})
	if nextOwner == deadOwnerID {
		// No alternative replica available (e.g. every other replica is
		// already excluded) - nothing left to retry locally.
		return cm.fallbackToSlowPath(cmd)
	}
	if nextOwner == cm.mystate.GetMyServerID() {
		return cm.handleFastPath(cmd)
	}

	conns.RLock()
	nextConn, ok := conns.m[nextOwner]
	conns.RUnlock()
	if !ok {
		return cm.fallbackToSlowPath(cmd)
	}

	args := cm.prepareArgs(cmd, 0)
	args.PrioVal = 0
	args.ForwardedBy = cm.mystate.GetMyServerID()
	reply := &Reply{}
	if err := callObjectOwnerRPC(nextConn, args, reply); err != nil {
		log.Errorf("[OBJECT-MAP] local reroute to %d also failed for %s: %v - falling back to slow path", nextOwner, cmd.ObjID, err)
		return cm.fallbackToSlowPath(cmd)
	}
	if reply.ErrorMsg != nil {
		log.Errorf("[OBJECT-MAP] reroute target %d returned error for %s: %v - falling back to slow path", nextOwner, cmd.ObjID, reply.ErrorMsg)
		return cm.fallbackToSlowPath(cmd)
	}
	return reply.Success, reply.PathUsed
}

// handleRead dispatches a read to the fast (local-only) or safe
// (weighted-quorum-confirmed) path. Mirrors the existing write split:
// independent objects are handled leaderlessly, dependent objects route
// through the global leader. Safe read is a weight-confirmation/liveness
// check (ReadIndex-style) — the codebase has no per-object sequence number
// to attach a stronger, versioned freshness guarantee to, so this is a
// deliberate, documented scope boundary rather than a full linearizable
// read.
func (cm *ConsensusManager) handleRead(cmd Command) (bool, interface{}, string) {
	// A batch of N distinct read keys bundled onto one wire message is N
	// independent reads, not one read of N keys - see handleReadBatch. Same
	// dispatch rule as HandleCommand's write-side split.
	if hasDistinctObjects(cmd.ObjIDs) {
		return cm.handleReadBatch(cmd)
	}

	if cmd.ObjType == IndependentObject && cmd.ReadMode != SafeRead {
		// Fast read: answer from this replica's own local state with zero
		// coordination — speculative, no safety guarantee, lowest latency.
		// Only valid for independent objects (single-writer assumption);
		// dependent objects are inherently contended/multi-writer, which is
		// exactly why they need global ordering through the leader, so the
		// design doc never specifies a fast/unsafe mode for them — always
		// fall through to the quorum path below regardless of ReadMode.
		found, value, label := cm.readLocalValue(cmd, "FASTREAD")
		if found {
			// readLocalValue never touches serverPerfM (only the write/commit
			// path does, via AddFastCommits/AddSlowCommits), so without this
			// every FASTREAD-served op — i.e. every read in MongoDB mode,
			// since reads never go through HandleCommand — was invisible to
			// the server-side CSV totals.
			cm.serverPerfM.AddFastCommits(1)
		}
		return found, value, label
	}

	if cmd.ObjType == IndependentObject {
		obj := cm.mystate.GetObject(cmd.ObjID)
		if obj == nil {
			return false, nil, "SAFEREAD"
		}
		myWeight := obj.GetReplicaWeight(cm.mystate.GetMyServerID())
		return cm.quorumRead(cmd, myWeight, obj.ThresholdFast)
	}

	// Dependent object: route through the leader, same as dependent writes.
	if !cm.mystate.IsLeader() {
		return cm.forwardRead(cmd)
	}
	clock, myPrio := cm.pstate.GetPriority()
	// Exclude known-dead replicas from the threshold, same fix and same
	// reason as startSlowPathProcessor's commit-quorum check: GetMajority()
	// is a static value that never shrinks on its own when a replica dies.
	return cm.quorumRead(cmd, myPrio, cm.pmgr.GetMajorityExcluding(clock, DeadReplicasSnapshot()))
}

// handleReadBatch splits cmd into one read per element of cmd.ObjIDs/
// ObjTypes, dispatched concurrently, each through the ordinary single-key
// handleRead (so it picks FastRead vs SafeRead/quorumRead/forwardRead per
// that element's own type and cmd.ReadMode, exactly as a standalone
// single-key read would). Without this split, a caller that only ever read
// cmd.ObjID (the first key) silently dropped every other key's read while
// still reporting the whole batch as successful - see the ConsensusService
// READ branch in service.go, which is the only caller that populates
// cmd.ObjIDs for a read.
func (cm *ConsensusManager) handleReadBatch(cmd Command) (bool, interface{}, string) {
	n := len(cmd.ObjIDs)
	values := make([]interface{}, n)
	oks := make([]bool, n)
	labels := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			objType := cmd.ObjType
			if idx < len(cmd.ObjTypes) {
				objType = cmd.ObjTypes[idx]
			}
			sub := Command{
				ClientID:    cmd.ClientID,
				ClientClock: cmd.ClientClock,
				CmdType:     cmd.CmdType,
				ReadMode:    cmd.ReadMode,
				ObjID:       cmd.ObjIDs[idx],
				ObjType:     objType,
			}
			ok, value, label := cm.handleRead(sub)
			oks[idx] = ok
			values[idx] = value
			labels[idx] = label
		}(i)
	}
	wg.Wait()

	allOK, allFast, allSafe := true, true, true
	for i, ok := range oks {
		if !ok {
			allOK = false
		}
		if labels[i] != "FASTREAD" {
			allFast = false
		}
		if labels[i] != "SAFEREAD" {
			allSafe = false
		}
	}
	label := "MIXED"
	switch {
	case allFast:
		label = "FASTREAD"
	case allSafe:
		label = "SAFEREAD"
	}
	return allOK, values, label
}

// readLocalValue returns this replica's own current value for cmd.ObjID,
// without any coordination. Shared by both read modes - the only
// difference safe read makes is confirming quorum weight beforehand.
func (cm *ConsensusManager) readLocalValue(cmd Command, label string) (bool, interface{}, string) {
	if evalType == MongoDB {
		if mongoDbFollower == nil {
			return false, nil, label
		}
		query := mongodb.Query{Op: mongodb.READ, Table: "usertable", Key: cmd.ObjID, Values: map[string]string{"<all fields>": ""}}
		result, _, err := mongoDbFollower.FollowerAPI([]mongodb.Query{query})
		if err != nil {
			return false, nil, label
		}
		// Unwrap FollowerAPI's outer per-query slice: it was called with
		// exactly one query, so result has at most one element. Returning
		// result[0] ([]map[string]string) instead of result ([][]map[string]
		// string) matters for more than shape - only []map[string]string is
		// gob-registered for the Reply.ReadResult interface{} field (see
		// conns.go's init); returning the doubly-nested type here would fail
		// every MongoDB read at RPC-encode time with "gob: type not
		// registered for interface".
		if len(result) == 0 {
			return true, []map[string]string{}, label
		}
		return true, result[0], label
	}

	obj := cm.mystate.GetObject(cmd.ObjID)
	if obj == nil {
		return false, nil, label
	}
	obj.RLock()
	value := obj.Value
	obj.RUnlock()
	return true, value, label
}

// quorumRead gathers Value from a quorum of replicas (self plus enough
// peers, via ReadVote, to cross threshold) and returns the value from
// whichever responding replica carries the highest weight. Under Cabinet's
// weight reassignment, the highest-weight replica in the responding set is
// the most recent fast responder — by definition the last proposer — so
// its value is the freshest available without a per-object version vector.
func (cm *ConsensusManager) quorumRead(cmd Command, selfWeight, threshold float64) (bool, interface{}, string) {
	const label = "SAFEREAD"

	found, selfValue, _ := cm.readLocalValue(cmd, label)
	bestWeight := 0.0
	var bestValue interface{}
	if found {
		bestWeight = selfWeight
		bestValue = selfValue
	}

	var total atomic.Uint64
	total.Store(uint64(selfWeight * 1000))
	thresholdScaled := uint64(threshold * 1000)
	if total.Load() > thresholdScaled {
		return found, bestValue, label
	}

	conns.RLock()
	peers := make([]*ServerDock, 0, len(conns.m))
	for _, conn := range conns.m {
		peers = append(peers, conn)
	}
	conns.RUnlock()

	if len(peers) == 0 {
		return total.Load() > thresholdScaled && found, bestValue, label
	}

	respCh := make(chan ReadVoteReply, len(peers))
	for _, conn := range peers {
		go func(c *ServerDock) {
			r := &ReadVoteReply{}
			if err := c.txClient.Call("WocService.ReadVote", &ReadVoteArgs{ObjID: cmd.ObjID, ObjType: cmd.ObjType}, r); err == nil {
				respCh <- *r
			} else {
				respCh <- ReadVoteReply{}
			}
		}(conn)
	}

	timeout := time.After(netMetrics.GetTimeout())
collectLoop:
	for received := 0; received < len(peers) && total.Load() < thresholdScaled; received++ {
		select {
		case r := <-respCh:
			if r.Accepted {
				found = true
				total.Add(uint64(r.Weight * 1000))
				if r.Weight > bestWeight {
					bestWeight = r.Weight
					bestValue = r.Value
				}
			}
		case <-timeout:
			break collectLoop
		}
	}

	return total.Load() > thresholdScaled && found, bestValue, label
}

// forwardRead relays a dependent-object safe read to the global leader,
// exactly like forwardToLeader does for writes - a real forward, not a
// vote, so PrioVal is explicitly zeroed.
func (cm *ConsensusManager) forwardRead(cmd Command) (bool, interface{}, string) {
	conns.RLock()
	leaderConn, ok := conns.m[cm.mystate.GetLeaderID()]
	conns.RUnlock()
	if !ok {
		return false, nil, "SAFEREAD"
	}

	args := cm.prepareArgs(cmd, 0)
	args.PrioVal = 0
	reply := &Reply{}
	if err := leaderConn.txClient.Call("WocService.ConsensusService", args, reply); err != nil {
		log.Errorf("forward read to leader failed: %v", err)
		return false, nil, "SAFEREAD"
	}
	return reply.Success, reply.ReadResult, reply.PathUsed
}

// objectWriteLock returns the per-object mutex used to serialize concurrent
// fast-path write attempts for objID into a real FIFO queue (see
// ConsensusManager.objectWriteLocks and handleFastPath). Lazily created and
// never removed - one *sync.Mutex per object touched by this replica for
// the life of the process, matching how ObjectState entries themselves are
// never removed either.
func (cm *ConsensusManager) objectWriteLock(objID string) *sync.Mutex {
	v, _ := cm.objectWriteLocks.LoadOrStore(objID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (cm *ConsensusManager) handleFastPath(cmd Command) (bool, string) {
	// Real per-object FIFO queue: a second concurrent write for the same
	// object blocks here until the first's entire fast-path attempt -
	// including a fallback-to-slow-path handoff, if that's how it resolves
	// - has finished, rather than being immediately rejected and rerouted
	// to the slow path the moment inFlight.LoadOrStore below finds the
	// object already claimed. Holding the lock across a fallback (rather
	// than releasing it right before the slow-path call) trades a slower
	// drain under sustained contention on one hot object for a much
	// simpler, unambiguously-correct single defer here.
	lock := cm.objectWriteLock(cmd.ObjID)
	lock.Lock()
	defer lock.Unlock()

	fastPathStart := time.Now()
	log.Debugf("[FAST] Processing request | ClientClock=%d | ObjID=%s", cmd.ClientClock, cmd.ObjID)
	cm.mystate.RLock()
	obj := cm.mystate.Objects[cmd.ObjID]
	cm.mystate.RUnlock()

	if obj == nil {
		// Real (e.g. MongoDB/YCSB) keys can't be pre-warmed individually
		// at startup the way the fixed synthetic obj-N pool is — there
		// are too many of them and they aren't known in advance. Create
		// on first sight instead, mirroring handleSlowPath's existing
		// on-demand AddObject. AddObject itself is the synchronization
		// point (locks + exists-check), so concurrent first-touches of
		// the same new key are safe.
		conns.RLock()
		numReplicas := len(conns.m) + 1
		conns.RUnlock()
		cm.mystate.AddObject(cmd.ObjID, cmd.ObjType, numReplicas, quorum, ratioTryStep)
		cm.mystate.RLock()
		obj = cm.mystate.Objects[cmd.ObjID]
		cm.mystate.RUnlock()
	}

	cmd.Timestamp = time.Now()
	actual, loaded := cm.inFlight.LoadOrStore(cmd.ObjID, cmd)

	if loaded {
		lastCmd := actual.(Command)
		if cmd.ClientID == lastCmd.ClientID && cmd.ClientClock == lastCmd.ClientClock {
			return true, "FAST"
		}
		if cmd.CmdType == WRITE && lastCmd.CmdType == WRITE {
			// Write-write conflict detected - fall back to slow path
			cm.inFlight.Delete(cmd.ObjID)
			return cm.fallbackToSlowPath(cmd)
		}
	}

	defer cm.inFlight.Delete(cmd.ObjID)

	// seq is this attempt's monotonic stamp for cmd.ObjID: followers use it
	// to reject stale/regressive applies and, if this round never reaches
	// quorum, to know whether their provisional apply is still the one to
	// revert on FallBack (see broadcastFallBack below).
	seq := obj.NextSeq()

	myServerID := cm.mystate.GetMyServerID()
	// One snapshot for the whole round: GetWeightCache() is a lock-free
	// atomic load (see ObjectState.weightCache), so this is safe even
	// though ReassignWeights can be rewriting the cache concurrently for a
	// different, already-completed round. Reading once here (rather than
	// per-connection below) also gives every replica's weight from the same
	// consistent view for this round.
	weights := obj.GetWeightCache()
	myWeight := weights[myServerID]
	var totalWeightAtomic atomic.Uint64
	totalWeightAtomic.Store(uint64(myWeight * 1000))
	
	thresholdScaled := uint64(obj.ThresholdFast * 1000)

	// Get number of replicas for channel buffer size
	conns.RLock()
	activeReplicas := len(conns.m)
	conns.RUnlock()
	quorumReached := make(chan struct{})

	responsesChan := make(chan struct {
		serverID int
		weight   float64
		accepted bool
	}, activeReplicas+1)

	pClock, _ := cm.pstate.GetPriority()
	baseArgs := cm.prepareArgs(cmd, pClock)
	baseArgs.Execute = true // every voting replica must actually apply this, not just ack
	baseArgs.Seq = seq
	baseArgs.FastProvisional = true // gate the apply through ApplyFastProvisional, not UpdateObjectCommit

	// Broadcast to all replicas (non-blocking)
	broadcastStart := time.Now()
	conns.RLock()
	for _, conn := range conns.m {
		cachedWeight := weights[conn.serverID] // from the round's snapshot above

		go func(c *ServerDock, a *Args, w float64) {
			reply := Reply{}
			err := c.txClient.Call("WocService.ConsensusService", a, &reply)
			
			select {
			case <-quorumReached:
				return  // Quorum reached, stop processing this response
			default:
			}
			
			if err == nil && reply.Accepted {
				responsesChan <- struct {
					serverID int
					weight   float64
					accepted bool
				}{c.serverID, w, true}
			} else {
				// Send negative response so we don't wait forever
				responsesChan <- struct {
					serverID int
					weight   float64
					accepted bool
				}{c.serverID, 0.0, false}
			}
		}(conn, baseArgs, cachedWeight) 
	}
	conns.RUnlock()
	broadcastLatency := time.Since(broadcastStart)

	quorumReachedFlag := false
	responseCount := 0
	var finalWeight uint64
		rpcStartTime := time.Now()
	timeout := time.After(netMetrics.GetTimeout())
	quorumStart := time.Now()
	// arrivalOrder records, in the order replies actually arrived, which
	// replicas accepted this round - fed to ReassignWeights on a successful
	// commit so the fast-path weight ranking adapts toward whoever responds
	// fastest, the same way the slow path's global priorities already do.
	arrivalOrder := make([]int, 0, activeReplicas)

	for !quorumReachedFlag {
		select {
		case resp := <-responsesChan:
			responseCount++

			// Record RTT for adaptive timeout
			netMetrics.RecordRTT(time.Since(rpcStartTime))

			if resp.accepted {
				arrivalOrder = append(arrivalOrder, resp.serverID)
				weightScaled := uint64(resp.weight * 1000)
				newTotal := totalWeightAtomic.Add(weightScaled)

				if newTotal > thresholdScaled {
					quorumReachedFlag = true
					finalWeight = newTotal
					close(quorumReached)  // Signal all RPCs to stop processing!
					goto commitFastPath
				}
			}
			
			if responseCount >= activeReplicas {
				goto checkQuorum
			}
			
		case <-timeout:
			goto checkQuorum
		}
	}

checkQuorum:
	finalWeight = totalWeightAtomic.Load()

	// Check if quorum was reached
	if quorumReachedFlag || finalWeight > thresholdScaled {
		// Close channel if not already closed
		select {
		case <-quorumReached:
		default:
			close(quorumReached)
		}
		goto commitFastPath
	}

	// Quorum not reached - fall back to slow path. Tell followers to revert
	// their provisional apply for this seq (best-effort; see
	// broadcastFallBack) before rerouting.
	close(quorumReached)  // Clean up channel
	cm.inFlight.Delete(cmd.ObjID)
	cm.serverPerfM.RecordFastPathFallback()
	go cm.broadcastFallBack(cmd.ObjID, seq)
	return cm.fallbackToSlowPath(cmd)

commitFastPath:
	quorumLatency := time.Since(quorumStart)
	commitStart := time.Now()
	
	// Quorum reached - commit transaction
	batchSize := 1
	switch v := cmd.Payload.(type) {
	case [][]byte:
		batchSize = len(v)
		// Per-operation commit: each operation gets its own object commit
		for i := 0; i < batchSize; i++ {
			objID := cmd.ObjID
			payload := [][]byte{v[i]} // Single-operation payload
			cm.mystate.UpdateObjectCommit(objID, myServerID, payload, "FAST")
			
			if opObj := cm.mystate.GetObject(objID); opObj != nil {
				opObj.Lock()
				opObj.LastCommitType = "FAST"
				opObj.LastCommitTime = time.Now()
				opObj.LastCommittedOpID = objID
				opObj.LastProposer = myServerID
				opObj.Unlock()
			}
		}
	case []mongodb.Query:
		batchSize = len(v)
		// Per-operation commit for MongoDB
		for i := 0; i < batchSize; i++ {
			objID := cmd.ObjID
			payload := []mongodb.Query{v[i]}
			cm.mystate.UpdateObjectCommit(objID, myServerID, payload, "FAST")

			if opObj := cm.mystate.GetObject(objID); opObj != nil {
				opObj.Lock()
				opObj.LastCommitType = "FAST"
				opObj.LastCommitTime = time.Now()
				opObj.LastCommittedOpID = objID
				opObj.LastProposer = myServerID
				opObj.Unlock()
			}
		}
		// Every other replica voted "yes" for this round without touching
		// its local MongoDB (see applyExecute's MongoDB/FastProvisional
		// case) - now that quorum is confirmed, tell them to apply the
		// already-agreed write for real. This replica's own copy is written
		// separately by the caller (conJobMongoDB), once HandleCommand here
		// returns success.
		go cm.broadcastMongoConfirm(cmd.ObjID, v)
	default:
		cm.mystate.UpdateObjectCommit(cmd.ObjID, myServerID, cmd.Payload, "FAST")
	}
	cm.mystate.AddCommitIndex(batchSize)
	
	commitLatency := time.Since(commitStart)
	totalLatency := time.Since(fastPathStart)
	cm.serverPerfM.AddFastCommits(batchSize)

	log.Debugf("[FAST] COMMIT | ClientClock=%d | ObjID=%s | Broadcast=%vμs | Quorum=%vμs | Commit=%vμs | Total=%vμs | Weight=%.2f/%.2f",
		cmd.ClientClock, cmd.ObjID, broadcastLatency.Microseconds(), quorumLatency.Microseconds(),
		commitLatency.Microseconds(), totalLatency.Microseconds(), float64(finalWeight)/1000, obj.ThresholdFast)

	if consensusLatencyDebug && totalLatency.Microseconds() > 500 {
		log.Infof("[FAST-PATH-BREAKDOWN] Total=%dμs | Broadcast=%dμs | Quorum=%dμs | Commit=%dμs | Weight=%.0f/%.0f",
			totalLatency.Microseconds(), broadcastLatency.Microseconds(),
			quorumLatency.Microseconds(), commitLatency.Microseconds(),
			float64(finalWeight)/1000, obj.ThresholdFast)
	}
	
	fastPathDuration := time.Since(fastPathStart)
	if latencyDebug {
		log.Debugf("[CONSENSUS-LATENCY] Fast path: %v", fastPathDuration)
	}

	// Reassign this object's weight ranking based on who responded fastest
	// this round. Async and best-effort: dropped under queue overload since
	// this is a throughput-tuning mechanism, not correctness-bearing (it
	// never touches ThresholdFast - see ReassignWeights), so a missed round
	// just means the ranking catches up on the next commit instead.
	select {
	case cm.weightReassignQueue <- weightReassignTask{
		objID:         cmd.ObjID,
		arrivalOrder:  arrivalOrder,
		coordinatorID: myServerID,
		numReplicas:   activeReplicas + 1,
	}:
	default:
	}

	return true, "FAST"
}


func (cm *ConsensusManager) handleSlowPath(cmd Command) (bool, string) {
	if len(cmd.ObjIDs) > 1 {
		// Multi-object transaction: on-demand-create every constituent
		// object, not just the primary cmd.ObjID.
		conns.RLock()
		numReplicas := len(conns.m) + 1
		conns.RUnlock()
		for i, id := range cmd.ObjIDs {
			if cm.mystate.GetObject(id) == nil {
				objType := DependentObject
				if i < len(cmd.ObjTypes) {
					objType = cmd.ObjTypes[i]
				}
				cm.mystate.AddObject(id, objType, numReplicas, quorum, ratioTryStep)
			}
		}
	}

	obj := cm.mystate.GetObject(cmd.ObjID)
	if obj == nil {
		conns.RLock()
		numReplicas := len(conns.m) + 1
		conns.RUnlock()
		cm.mystate.AddObject(cmd.ObjID, cmd.ObjType, numReplicas, quorum, ratioTryStep)
		obj = cm.mystate.GetObject(cmd.ObjID)
	}

	// Forward to leader if not leader
	if !cm.mystate.IsLeader() {
		return cm.forwardToLeader(cmd)
	}
	
	// Backpressure: reject immediately instead of joining a queue that's
	// already deep enough that we'd just be waiting out slowPathDeadline.
	if len(slowPathQueue) >= slowPathQueueHighWaterMark {
		log.Errorf("[SLOW] Queue overloaded (%d/%d) - rejecting | ObjID=%s", len(slowPathQueue), slowPathQueueHighWaterMark, cmd.ObjID)
		return false, "SLOW"
	}

	req := &SlowPathRequest{
		Cmd:        cmd,
		ReplyChan:  make(chan SlowPathResult, 1),
		ReceivedAt: time.Now(),
	}

	// One deadline covers both enqueueing and waiting for the result, so a
	// caller never blocks longer than slowPathDeadline end-to-end. ReplyChan
	// is buffered (cap 1), so if we give up here the processor's eventual
	// send into it still succeeds without blocking - no goroutine leak.
	deadline := time.NewTimer(slowPathDeadline)
	defer deadline.Stop()

	select {
	case slowPathQueue <- req:
	case <-deadline.C:
		log.Errorf("[SLOW] Timed out queueing request after %v | ObjID=%s", slowPathDeadline, cmd.ObjID)
		return false, "SLOW"
	}

	select {
	case result := <-req.ReplyChan:
		return result.Success, result.PathUsed
	case <-deadline.C:
		log.Errorf("[SLOW] Timed out waiting for slow-path result after %v | ObjID=%s", slowPathDeadline, cmd.ObjID)
		return false, "SLOW"
	}
}

func (cm *ConsensusManager) startSlowPathProcessor() {
	leaderPClock := 0
	
	log.Infof("Slow path processor started (Cabinet-style serialized)")
	
	for {
		req := <-slowPathQueue
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("[SLOW] PANIC in slow path processing | ObjID=%s | recover=%v", req.Cmd.ObjID, r)
					req.ReplyChan <- SlowPathResult{Success: false, PathUsed: "SLOW"}
				}
			}()
			cmd := req.Cmd

			// Record start time from when request arrived at queue for accurate metrics
			globalClock := int(cm.IncrementGlobalClock())
			cm.serverPerfM.RecordStarterAt(globalClock, req.ReceivedAt)

			consensusStart := time.Now()

			log.Debugf("[SLOW] Processing request | ClientClock=%d | LeaderClock=%d | ObjID=%s",
				cmd.ClientClock, leaderPClock, cmd.ObjID)

			// Check duplicate (lock-free!)
			if actual, exists := cm.inFlight.Load(cmd.ObjID); exists {
				lastCmd := actual.(Command)
				if cmd.ClientID == lastCmd.ClientID && cmd.ClientClock == lastCmd.ClientClock {
					req.ReplyChan <- SlowPathResult{Success: true, PathUsed: "SLOW"}
					return
				}
			}

			// Get priorities for this clock
			fpriorities := cm.pmgr.GetFollowerPriorities(leaderPClock)
			if len(fpriorities) == 0 {
				// On-demand initialization (WOC-style)
				prioQueue := make(chan int, numOfServers)
				if leaderPClock > 0 {
					prevPriorities := cm.pmgr.GetFollowerPriorities(leaderPClock - 1)
					if len(prevPriorities) > 0 {
						for id := range prevPriorities {
							if id != cm.mystate.GetMyServerID() {
								prioQueue <- id
							}
						}
					}
				}

				if err := cm.pmgr.UpdateFollowerPriorities(leaderPClock, prioQueue, cm.mystate.GetMyServerID()); err != nil {
					log.Errorf("[SLOW] Failed to initialize priorities for pClock %d: %v", leaderPClock, err)
					req.ReplyChan <- SlowPathResult{Success: false, PathUsed: "SLOW"}
					return
				}

				fpriorities = cm.pmgr.GetFollowerPriorities(leaderPClock)
			}

			myServerID := cm.mystate.GetMyServerID()
			leaderPrio, ok := fpriorities[myServerID]
			if !ok {
				log.Errorf("[SLOW] Leader priority missing for pClock=%d!", leaderPClock)
				req.ReplyChan <- SlowPathResult{Success: false, PathUsed: "SLOW"}
				return
			}

			prioSum := leaderPrio
			prioQueue := make(chan int, numOfServers)

			// The quorum threshold must exclude replicas the health monitor
			// has already declared dead, the same way the broadcast loop
			// below already skips dialing them: pm.GetMajority() is a
			// static value computed once at startup from ALL numOfServers
			// replicas' weights and never shrinks on its own (weight
			// reassignment only permutes which replica holds which value,
			// it doesn't remove a dead replica's value from the total - see
			// ObjectState.ReassignWeights's identical invariant on the fast
			// path). Without this, a dead replica's weight stays baked into
			// the threshold forever: if it held enough weight, the
			// survivors' combined weight can permanently fall short of the
			// stale majority, and slow-path commits fail forever even
			// though a new leader was successfully elected (StartElection
			// already gets this right via GetMajorityExcluding - this is
			// the same fix applied to the ongoing per-commit quorum check).
			majority := cm.pmgr.GetMajorityExcluding(leaderPClock, DeadReplicasSnapshot())

			// Track command in flight
			cmd.Timestamp = time.Now()
			cm.inFlight.Store(cmd.ObjID, cmd)
			defer cm.inFlight.Delete(cmd.ObjID)

			// Broadcast to followers
			receiver := make(chan ReplyInfo, numOfServers)
			responseCount := 1 // Leader's vote

			broadcastStart := time.Now()
			conns.RLock()
			activeServers := 1
			for _, conn := range conns.m {
				if conn.serverID != myServerID {
					// Skip replicas the health monitor has already declared
					// dead: conns.m never has them pruned (a connection to a
					// crashed process stays in the map forever), so without
					// this check every slow-path round keeps re-dialing them
					// and paying executeSlowRPC's per-connection jobQ-ordering
					// wait (up to 3s) for an RPC that's guaranteed to fail.
					// With the slow path fully serialized through one queue
					// (startSlowPathProcessor), that cost was multiplying
					// across every queued request instead of being paid once.
					if isDeadReplica(conn.serverID) {
						continue
					}

					activeServers++
					args := cm.prepareArgs(cmd, leaderPClock)
					args.PrioVal = fpriorities[conn.serverID]
					args.Execute = true // every voting follower must actually apply this, not just ack
					go executeSlowRPC(conn, "WocService.ConsensusService", args, receiver)
				}
			}
			conns.RUnlock()
			broadcastTime := time.Since(broadcastStart)

			// Collect votes
			quorumStart := time.Now()
			timeout := time.After(5 * time.Second)
			quorumReached := false
			respondedServers := make(map[int]bool)

			for !quorumReached && responseCount < activeServers {
				select {
				case rinfo := <-receiver:
					if rinfo.Reply.ErrorMsg == nil && rinfo.Reply.Accepted {
						responseCount++
						respondedServers[rinfo.ServerID] = true
						prioQueue <- rinfo.ServerID

						followerPrio, ok := fpriorities[rinfo.ServerID]
						if !ok {
							log.Warnf("[SLOW] Server %d priority not found", rinfo.ServerID)
							continue
						}

						prioSum += followerPrio

						if prioSum > majority {
							quorumReached = true
						}
					}

				case <-timeout:
					nonResponders := []int{}
					conns.RLock()
					for _, conn := range conns.m {
						if conn.serverID != myServerID && !respondedServers[conn.serverID] {
							nonResponders = append(nonResponders, conn.serverID)
						}
					}
					conns.RUnlock()

					log.Warnf("[SLOW] Timeout | pClock=%d | Responses=%d/%d | NonResponders=%v",
						leaderPClock, responseCount, activeServers, nonResponders)
					goto finishRequest
				}
			}

		finishRequest:
			quorumTime := time.Since(quorumStart)

			if !quorumReached {
				log.Warnf("[SLOW] Consensus FAILED | pClock=%d | prioSum=%.2f/%.2f",
					leaderPClock, prioSum, majority)
				req.ReplyChan <- SlowPathResult{Success: false, PathUsed: "SLOW"}
				cm.serverPerfM.RecordFinisher(globalClock)
				leaderPClock++
				return
			}

			// Commit transaction
			commitStart := time.Now()
			batchSize := 1

			// Multi-object transactions apply cmd.ObjIDs[i] <-> v[i] below
			// instead of collapsing every element onto cmd.ObjID. Note:
			// UpdateObjectCommit locks each object internally per call, so
			// applying the N objects isn't atomic from a concurrent reader's
			// perspective (no pre-locking is done here to avoid deadlocking
			// against UpdateObjectCommit's own per-object lock) - readers may
			// observe a partially-applied multi-object transaction mid-loop.
			// startSlowPathProcessor's single-consumer loop still guarantees
			// no two slow-path commands ever interleave with each other.
			switch v := cmd.Payload.(type) {
			case [][]byte:
				batchSize = len(v)
				for i := 0; i < batchSize; i++ {
					objID := cmd.ObjID
					if i < len(cmd.ObjIDs) {
						objID = cmd.ObjIDs[i]
					}
					payload := [][]byte{v[i]}
					cm.mystate.UpdateObjectCommit(objID, myServerID, payload, "SLOW")

					if opObj := cm.mystate.GetObject(objID); opObj != nil {
						opObj.Lock()
						opObj.LastCommitType = "SLOW"
						opObj.LastCommitTime = time.Now()
						opObj.Unlock()
					}
				}
			case []mongodb.Query:
				batchSize = len(v)
				for i := 0; i < batchSize; i++ {
					objID := cmd.ObjID
					if i < len(cmd.ObjIDs) {
						objID = cmd.ObjIDs[i]
					}
					payload := []mongodb.Query{v[i]}
					cm.mystate.UpdateObjectCommit(objID, myServerID, payload, "SLOW")

					if opObj := cm.mystate.GetObject(objID); opObj != nil {
						opObj.Lock()
						opObj.LastCommitType = "SLOW"
						opObj.LastCommitTime = time.Now()
						opObj.Unlock()
					}
				}
			case RingUpdate:
				// Applied only once quorum is confirmed above, so the
				// leader adopts the new ring at the same consensus order
				// position followers do (see applyRingUpdate).
				applyRingUpdate(v)
			default:
				cm.mystate.UpdateObjectCommit(cmd.ObjID, myServerID, cmd.Payload, "SLOW")
			}
			cm.mystate.AddCommitIndex(batchSize)
			commitTime := time.Since(commitStart)

			cm.serverPerfM.AddSlowCommits(batchSize)
			for i := 0; i < batchSize; i++ {
				cm.serverPerfM.IncSlowPath(globalClock)
			}

			totalTime := time.Since(consensusStart)
			queueWait := consensusStart.Sub(req.ReceivedAt)

			log.Debugf("[SLOW] COMMIT | pClock=%d | QueueWait=%vμs | Broadcast=%vμs | Quorum=%vμs | Commit=%vμs | Total=%vμs | prioSum=%.2f",
				leaderPClock, queueWait.Microseconds(), broadcastTime.Microseconds(), quorumTime.Microseconds(),
				commitTime.Microseconds(), totalTime.Microseconds(), prioSum)

			// Send result back
			req.ReplyChan <- SlowPathResult{Success: true, PathUsed: "SLOW"}
			cm.serverPerfM.RecordFinisher(globalClock)  // Stop timer before priority update

			// Update priorities for next round
			nextClock := leaderPClock + 1
			if err := cm.pmgr.UpdateFollowerPriorities(nextClock, prioQueue, myServerID); err != nil {
				log.Errorf("[SLOW] Priority update failed for clock %d: %v", nextClock, err)
			} else {
				if cm.pstate != nil {
					if newPrios := cm.pmgr.GetFollowerPriorities(nextClock); len(newPrios) > 0 {
						if newLeaderPrio, ok := newPrios[myServerID]; ok {
							cm.pstate.UpdatePriority(nextClock, newLeaderPrio)
						}
					}
				}
			}

			leaderPClock++
		}()
	}
}

func (cm *ConsensusManager) SaveServerMetrics() error {
	if cm.serverPerfM == nil {
		return fmt.Errorf("server performance meter not initialized")
	}
	log.Debugf("Saving server %d metrics | Total operations: %d", cm.mystate.GetMyServerID(), cm.globalClock)
	if err := cm.serverPerfM.SaveToFile(); err != nil {
		log.Errorf("Failed to save server metrics: %v", err)
		return err
	}
	log.Debugf("Server %d metrics saved successfully", cm.mystate.GetMyServerID())
	return nil
}

func (cm *ConsensusManager) forwardToLeader(cmd Command) (bool, string) {
	conns.RLock()
	leaderConn, ok := conns.m[cm.mystate.GetLeaderID()]
	conns.RUnlock()
	
	if !ok || cm.detectLeaderFailure(cm.mystate.GetLeaderID()) {
		cm.handleLeaderFailure()
		if cm.mystate.GetMyServerID() != cm.mystate.GetLeaderID() {
			conns.RLock()
			leaderConn, ok = conns.m[cm.mystate.GetLeaderID()]
			conns.RUnlock()
			if !ok {
				log.Errorf("New leader connection unavailable")
				return false, "SLOW"
			}
		} else {
			return cm.handleSlowPath(cmd)
		}
	}
	args := cm.prepareArgs(cmd, 0)
	args.PrioVal = 0 // real content forward, not a priority vote — see forwardToObjectOwner
	reply := &Reply{}
	if err := leaderConn.txClient.Call("WocService.ConsensusService", args, &reply); err != nil {
		log.Errorf("forward to leader failed: %v", err)
		if cm.detectLeaderFailure(cm.mystate.GetLeaderID()) {
		 cm.handleLeaderFailure()
		}
		return false, "SLOW"
	}
	if reply.ErrorMsg != nil {
			log.Errorf("Leader returned error: %v", reply.ErrorMsg)
			return false, "SLOW"
		}	
	return reply.Success, reply.PathUsed
}

func (cm *ConsensusManager) forwardToLeaderOptimized(args *Args, reply *Reply) (bool, string) {
	forwardStart := time.Now()
	conns.RLock()
	leaderConn, ok := conns.m[cm.mystate.GetLeaderID()]
	conns.RUnlock()
	
	if !ok || cm.detectLeaderFailure(cm.mystate.GetLeaderID()) {
		cm.handleLeaderFailure()
		if cm.mystate.GetMyServerID() != cm.mystate.GetLeaderID() {
			conns.RLock()
			leaderConn, ok = conns.m[cm.mystate.GetLeaderID()]
			conns.RUnlock()
			if !ok {
				log.Errorf("New leader connection unavailable")
				return false, "SLOW"
			}
		} else {
			// Became leader during failover - process locally
			var payload interface{}
			if args.Type == MongoDB {
				payload = args.CmdMongo
			} else {
				payload = args.CmdPlain
			}
			cmd := Command{
				ClientID:    args.ClientID,
				ClientClock: args.ClientClock,
				ObjID:       args.ObjID,
				ObjType:     args.ObjType,
				CmdType:     args.CmdType,
				Payload:     payload,
				ForwardedBy: cm.mystate.GetMyServerID(),
				AlreadyForwarded: args.AlreadyForwarded,
				ObjIDs:      args.ObjIDs,
				ObjTypes:    args.ObjTypes,
			}
			return cm.handleSlowPath(cmd)
		}
	}
	args.ForwardedBy = cm.mystate.GetMyServerID()
	
	rpcStart := time.Now()
	if err := leaderConn.txClient.Call("WocService.ConsensusService", args, reply); err != nil {
		log.Errorf("forward to leader failed: %v", err)
		if cm.detectLeaderFailure(cm.mystate.GetLeaderID()) {
			cm.handleLeaderFailure()
		}
		return false, "SLOW"
	}
	rpcLatency := time.Since(rpcStart)
	
	if reply.ErrorMsg != nil {
		log.Errorf("Leader returned error: %v", reply.ErrorMsg)
		return false, "SLOW"
	}
	
	totalForwardLatency := time.Since(forwardStart)
	if consensusLatencyDebug && totalForwardLatency.Microseconds() > 200 {
		log.Infof("[FORWARD-BREAKDOWN] Total=%dμs | RPC=%dμs | Overhead=%dμs",
			totalForwardLatency.Microseconds(), rpcLatency.Microseconds(),
			(totalForwardLatency - rpcLatency).Microseconds())
	}
	
	return reply.Success, reply.PathUsed
}

func (cm *ConsensusManager) clearCommand(cmd Command) {
	cm.inFlight.Delete(cmd.ObjID)  // ✅ Lock-free!
}


func executeSlowRPCWithContext(ctx context.Context, conn *ServerDock, service string, args *Args, receiver chan ReplyInfo) {
	reply := Reply{}
	
	stack := make(chan struct{}, 1)
	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] = stack
	conn.jobQMu.Unlock()
	
	if args.PrioClock > 0 {
		conn.jobQMu.RLock()
		prev, ok := conn.jobQ[args.PrioClock-1]
		conn.jobQMu.RUnlock()
		
		if ok {
			select {
			case <-prev:
			case <-ctx.Done():
				// Context cancelled - abort early
				conn.jobQMu.Lock()
				conn.jobQ[args.PrioClock] <- struct{}{}
				conn.jobQMu.Unlock()
				return
			case <-time.After(3 * time.Second):
				log.Warnf("[CLOUD-SLOW-RPC] Timeout waiting for prev pClock=%d", args.PrioClock-1)
			}
		}
	}
	
	// Make RPC call with context
	done := make(chan error, 1)
	go func() {
		done <- conn.txClient.Call(service, args, &reply)
	}()
	
	select {
	case err := <-done:
		if err != nil {
			log.Debugf("[CLOUD-SLOW-RPC] Call error | server=%d | err=%v", conn.serverID, err)
			reply.Accepted = false
			reply.ErrorMsg = err
		}
		
	case <-ctx.Done():
		log.Debugf("[CLOUD-SLOW-RPC] Cancelled | server=%d | reason=%v", conn.serverID, ctx.Err())
		reply.Accepted = false
		reply.ErrorMsg = ctx.Err()
	}
	
	rinfo := ReplyInfo{
		ServerID:    conn.serverID,
		Reply:       reply,
		ClientClock: args.ClientClock,
	}
	receiver <- rinfo
	
	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] <- struct{}{}
	conn.jobQMu.Unlock()
}

func executeSlowRPC(conn *ServerDock, service string, args *Args, receiver chan ReplyInfo) {
	reply := Reply{}
	
	stack := make(chan struct{}, 1)

	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] = stack
	conn.jobQMu.Unlock()

	if args.PrioClock > 0 {
		conn.jobQMu.RLock()
		prev, ok := conn.jobQ[args.PrioClock-1]
		conn.jobQMu.RUnlock()
		
		if ok {
			select {
			case <-prev:
			case <-time.After(3 * time.Second):
				log.Warnf("[SLOW] Timeout waiting for prev pClock=%d | server=%d | proceeding anyway", 
					args.PrioClock-1, conn.serverID)
			}
		}
	}

	start := time.Now()
	log.Debugf("[SLOW-RPC] Calling server=%d | PrioClock=%d | ObjID=%s | ClientClock=%d", 
		conn.serverID, args.PrioClock, args.ObjID, args.ClientClock)
	
	err := conn.txClient.Call(service, args, &reply)
	latency := time.Since(start).Seconds() * 1000
	
	log.Debugf("[SLOW-RPC] Response from server=%d | latency=%.2fms | err=%v | Accepted=%v", 
		conn.serverID, latency, err, reply.Accepted)
	
	if err != nil {
		log.Errorf("[SLOW] RPC call error | server=%d | ClientClock=%d | PrioClock=%d | err=%v", 
			conn.serverID, args.ClientClock, args.PrioClock, err)
		
		conn.jobQMu.Lock()
		conn.jobQ[args.PrioClock] <- struct{}{}
		conn.jobQMu.Unlock()
		
		rinfo := ReplyInfo{
			ServerID:    conn.serverID,
			Reply:       Reply{Accepted: false, ErrorMsg: err},
			Latency:     latency,
			ClientClock: args.ClientClock,
		}
		receiver <- rinfo
		return
	}

	if !reply.Accepted || reply.ErrorMsg != nil {
		log.Warnf("[SLOW] RPC rejected | server=%d | ClientClock=%d | PrioClock=%d | Accepted=%v | err=%v",
			conn.serverID, args.ClientClock, args.PrioClock, reply.Accepted, reply.ErrorMsg)
		
		conn.jobQMu.Lock()
		conn.jobQ[args.PrioClock] <- struct{}{}
		conn.jobQMu.Unlock()
		
		rinfo := ReplyInfo{
			ServerID:    conn.serverID,
			Reply:       reply,
			Latency:     latency,
			ClientClock: args.ClientClock,
		}
		receiver <- rinfo
		return
	}

	rinfo := ReplyInfo{
		ServerID:    conn.serverID,
		Reply:       reply,
		Latency:     latency,
		ClientClock: args.ClientClock,
	}
	receiver <- rinfo

	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] <- struct{}{}
	conn.jobQMu.Unlock()

	log.Debugf("[SLOW] RPC success | server=%d | ClientClock=%d | PrioClock=%d | latency=%.2fms",
		conn.serverID, args.ClientClock, args.PrioClock, latency)
}

func prepCrashList() (crashList []int) {
	switch crashMode {
	case 0:
		break
	case 1:
		for i := 1; i < quorum; i++ {
			crashList = append(crashList, i)
		}
	case 2:
		for i := 1; i < quorum; i++ {
			crashList = append(crashList, numOfServers-i)
		}
	case 3:
		rand.Seed(time.Now().UnixNano())
		for i := 1; i < quorum; i++ {
			contains := false
			for {
				crashID := rand.Intn(numOfServers-1) + 1
				for _, cID := range crashList {
					if cID == crashID {
						contains = true
						break
					}
				}
				if contains {
					contains = false
					continue
				} else {
					crashList = append(crashList, crashID)
					break
				}
			}
		}
	case 4:
		// Crash exactly one explicit replica, regardless of F - useful for
		// isolating weight-dependent bugs (e.g. killing a low-ID/high-weight
		// replica vs a high-ID/low-weight one behaves very differently under
		// the weighted fast-path quorum).
		if crashTarget >= 0 {
			crashList = append(crashList, crashTarget)
		}
	default:
		break
	}
	return
}

// startReplicaHealthMonitor runs on every replica but only acts while this
// replica is the leader (so it self-activates/deactivates across leader
// failovers with no extra wiring at election time). It implements paper
// §4.2's failure-handling half: "the leader detects the failure and removes
// r from the ring, reassigning its arc to the adjacent replica. The updated
// metadata is then disseminated to all replicas." Dissemination is a
// RingUpdate command committed through the normal slow-path consensus round
// (see conJobRingReconfig in service.go), so every replica adopts the new
// ring at the same global order position; pollOwnershipFromLeader (main.go)
// remains as a periodic-pull backstop for a replica that missed the round
// (e.g. it was mid-restart) rather than the primary dissemination path.
// A replica is marked dead after missedFailureThreshold consecutive failed
// pings and revived as soon as a ping to it succeeds again.
func (cm *ConsensusManager) startReplicaHealthMonitor() {
	const (
		pingInterval           = 3 * time.Second
		pingTimeout            = 2 * time.Second
		missedFailureThreshold = 3
	)
	missCounts := make(map[int]int)

	for {
		time.Sleep(pingInterval)
		if !cm.mystate.IsLeader() {
			continue
		}

		conns.RLock()
		targets := make([]*ServerDock, 0, len(conns.m))
		for _, c := range conns.m {
			targets = append(targets, c)
		}
		conns.RUnlock()

		changed := false
		for _, c := range targets {
			done := make(chan error, 1)
			go func(conn *ServerDock) {
				done <- conn.txClient.Call("WocService.Ping", &PingArgs{}, &Reply{})
			}(c)

			var pingErr error
			select {
			case pingErr = <-done:
			case <-time.After(pingTimeout):
				pingErr = fmt.Errorf("ping timeout")
			}

			wasDead := isDeadReplica(c.serverID)
			if pingErr != nil {
				missCounts[c.serverID]++
				if !wasDead && missCounts[c.serverID] == missedFailureThreshold {
					log.Warnf("[FAILOVER] Replica %d unresponsive (%d consecutive misses) — removing from object-ownership ring",
						c.serverID, missCounts[c.serverID])
					changed = true
				}
			} else if missCounts[c.serverID] > 0 || wasDead {
				missCounts[c.serverID] = 0
				if wasDead {
					log.Infof("[FAILOVER] Replica %d reachable again — restoring to object-ownership ring", c.serverID)
					changed = true
				}
			}
		}

		if !changed {
			continue
		}

		dead := make(map[int]bool)
		deadList := make([]int, 0)
		for sid, misses := range missCounts {
			if misses >= missedFailureThreshold {
				dead[sid] = true
				deadList = append(deadList, sid)
			}
		}

		conns.RLock()
		numReplicas := len(conns.m) + 1
		conns.RUnlock()
		owners := BuildOwnershipRing(numReplicas, dead)

		// Apply the dead-set locally on this (leader) replica *before*
		// proposing the ring update, not just after it commits via
		// applyRingUpdate. Without this, a newly-dead replica that held
		// enough weight creates a chicken-and-egg deadlock: the slow-path
		// quorum threshold (startSlowPathProcessor) is only corrected by
		// excluding replicas in the shared dead-set, but that set is only
		// populated by this very ring-reconfig command committing - which
		// itself has to clear that same, still-uncorrected threshold first.
		// This mirrors the existing pattern in forwardToObjectOwner/
		// rerouteAroundDeadOwner, which already act on locally-observed
		// failures immediately rather than waiting for consensus to
		// confirm them; applyRingUpdate calling SetDeadReplicas again with
		// the same data once the command commits is a harmless no-op.
		SetDeadReplicas(deadList)

		// Commit the new ring/dead-set through a slow-path consensus round
		// (RingUpdate/applyRingUpdate) instead of applying it locally and
		// then best-effort-pushing it to followers: that left a window
		// where this replica (and whichever followers the push RPC
		// happened to reach first) used the new ring while stragglers
		// still routed against the old one, with no guarantee they'd ever
		// converge at the same point relative to other in-flight commands.
		// Routing through consensus means every replica - including this
		// one, which only applies it once its own quorum check passes -
		// adopts the new ring at the same global order position. See
		// conJobRingReconfig in service.go.
		ringCmd := Command{
			ObjID:   ringObjectID,
			ObjType: DependentObject,
			CmdType: WRITE,
			Payload: RingUpdate{OwnerByIndex: owners, DeadReplicas: deadList},
		}
		if ok, _ := cm.HandleCommand(ringCmd); !ok {
			log.Errorf("[FAILOVER] ring reconfig consensus failed | dead=%v - will retry next health-check tick", deadList)
			continue
		}
		log.Infof("[FAILOVER] Ring reconfig committed | dead=%v", deadList)
	}
}

func (cm *ConsensusManager) detectLeaderFailure(leaderID int) bool {
    conns.RLock()
    leaderConn, ok := conns.m[leaderID]
    conns.RUnlock()
    if !ok {
        return true
    }
    
    pClock, _ := cm.pstate.GetPriority()
    args := &PingArgs{Clock: pClock}
    reply := &Reply{}
    done := make(chan error, 1)
    go func() {
        done <- leaderConn.txClient.Call("WocService.Ping", args, reply)
    }()
    
    select {
    case err := <-done:
        if err != nil {
            log.Errorf("Leader ping failed: %v", err)
            return true
        }
        return false
    case <-time.After(2 * time.Second):
        log.Errorf("Leader ping timeout")
        return true
    }
}

func (cm *ConsensusManager) handleLeaderFailure() {
    cm.mu.Lock()
    oldLeader := cm.mystate.GetLeaderID()
    pClock, _ := cm.pstate.GetPriority()
    newClock := pClock + 1
    cm.mu.Unlock()

    prioQueue := make(chan serverID, numOfServers)

    conns.RLock()
    for id := range conns.m {
        if id != oldLeader {
            prioQueue <- id
        }
    }
    conns.RUnlock()

    if err := cm.pmgr.UpdateFollowerPriorities(newClock, prioQueue, -1); err != nil {
        log.Errorf("Failed to update priorities: %v", err)
        return
    }

    // oldLeader just failed a liveness check (detectLeaderFailure) but may
    // not yet have crossed startReplicaHealthMonitor's missed-ping
    // threshold to land in the shared dead-replica set, so it's added
    // explicitly here alongside whatever's already known dead.
    excludeIDs := append(DeadReplicasSnapshot(), oldLeader)

    if cm.StartElection(excludeIDs) {
        log.Infof("Successfully elected new leader: Server %d", cm.mystate.GetMyServerID())
    } else {
        log.Warnf("Failed to elect new leader")
    }
}

// StartElection initiates a leader election process. excludeIDs are
// replicas already known or suspected dead (e.g. the just-failed leader,
// plus anything in the shared dead-replica set) - they're skipped when
// broadcasting vote requests, and GetMajorityExcluding removes their
// priority weight from the quorum threshold, so a crash that took a large
// share of the weighted total with it doesn't leave the threshold
// permanently unreachable for the survivors.
func (cm *ConsensusManager) StartElection(excludeIDs []int) bool {
    cm.mu.Lock()
    // Check if we've already voted in this term
    if cm.mystate.CheckVotedFor() {
        cm.mu.Unlock()
        return false
    }

    // Increment term and vote for self
    cm.mystate.SetTerm(cm.mystate.GetTerm() + 1)
    cm.mystate.SetVotedFor(true)
    currentTerm := cm.mystate.GetTerm()
    cm.mu.Unlock()

    excluded := make(map[int]bool, len(excludeIDs))
    for _, id := range excludeIDs {
        excluded[id] = true
    }

    // Get current priority clock and value
    clock, myPrio := cm.pstate.GetPriority()
    majority := cm.pmgr.GetMajorityExcluding(clock, excludeIDs)

    // Create vote request for broadcasting. Must be VoteArgs/Reply - the
    // actual types WocService.RequestVote (service.go) is registered with.
    // This used to be a separate VoteRequest/VoteReply pair declared in this
    // file; net/rpc's gob wire encoding matches by field name, so the
    // request side (identical fields) silently worked, but the reply side
    // didn't - the server signals a granted vote via Reply.Success, and
    // VoteReply.VoteGranted has no corresponding field anywhere in what the
    // server actually sends, so it was always false. No peer vote was ever
    // counted, meaning a candidate could only win if its own weight alone
    // exceeded the majority threshold - which the weight scheme's own
    // design invariant guarantees never happens. See the top of this
    // function's history for why that made leader failover permanently
    // broken after the very first leader failure.
    voteRequest := &VoteArgs{
        Term:        currentTerm,
        CandidateID: cm.mystate.GetMyServerID(),
        Priority:    myPrio,
    }

    // Broadcast vote requests and collect responses
    responses := make(chan float64, numOfServers)
    serverPrios := cm.pmgr.GetFollowerPriorities(clock)

    conns.RLock()
    for _, conn := range conns.m {
        if conn.serverID == cm.mystate.GetMyServerID() || excluded[conn.serverID] {
            continue
        }
        go func(conn *ServerDock) {
            // Send vote request through RPC
            var voteReply Reply
            err := conn.txClient.Call("WocService.RequestVote", voteRequest, &voteReply)
            if err == nil && voteReply.Success {
                if prio, ok := serverPrios[conn.serverID]; ok {
                    responses <- prio
                }
            }
        }(conn)
    }
    conns.RUnlock()

    timeoutChan := time.After(2 * time.Second)
    var responseCount int = 1 // counting self-vote
    prioSum := myPrio // Start with self-vote

    for responseCount < numOfServers {
        select {
        case prio := <-responses:
            responseCount++
            prioSum += prio

            // Check if we have quorum based on priorities
            if prioSum > majority {
                cm.mu.Lock()
                cm.mystate.SetLeaderID(cm.mystate.GetMyServerID())
                // Update priority clock on becoming leader
                cm.pstate.UpdatePriority(clock + 1, myPrio)
                cm.mu.Unlock()
                // A replica that becomes leader via election, rather than
                // being the hardcoded startup leader, still needs its own
                // slow-path processor goroutine running - see
                // ensureSlowPathProcessorStarted's doc comment for why this
                // was previously missing entirely.
                cm.ensureSlowPathProcessorStarted()
                return true
            }

        case <-timeoutChan:
            // Election timed out
            cm.mu.Lock()
            cm.mystate.SetVotedFor(false) // Reset vote since election failed
            cm.mu.Unlock()
            return false
        }
    }

    // If we get here, we didn't get quorum
    cm.mu.Lock()
    cm.mystate.SetVotedFor(false) // Reset vote since election failed
    cm.mu.Unlock()
    return false
}

func (cm *ConsensusManager) startCleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for range ticker.C {
			cm.cleanupStaleEntries()
		}
	}()
}

func (cm *ConsensusManager) cleanupStaleEntries() {
	now := time.Now()
	cleanedCount := 0
	
	// Clean inFlight map (backup - should already be cleared by defer)
	cm.inFlight.Range(func(key, value interface{}) bool {
		objID := key.(string)
		cmd := value.(Command)
		// Remove entries older than 2 minutes (safety net)
		if !cmd.Timestamp.IsZero() && now.Sub(cmd.Timestamp) > 2*time.Minute {
			cm.inFlight.Delete(objID)
			cleanedCount++
		}
		return true  // Continue iteration
	})
	
	if cleanedCount > 0 {
		log.Debugf("[CLEANUP] Removed %d stale inFlight entries", cleanedCount)
	}
	
	conns.RLock()
	for _, conn := range conns.m {
		conn.jobQMu.Lock()
		
		if len(conn.jobQ) > 100 {
			// Collect completed entries (non-blocking check)
			var completedClocks []int
			for clock, ch := range conn.jobQ {
				select {
				case <-ch:
					// Channel was signaled → entry completed
					completedClocks = append(completedClocks, clock)
				default:
					// Channel still active → DO NOT DELETE
				}
			}
			
			for _, clock := range completedClocks {
				delete(conn.jobQ, clock)
			}
			
			log.Debugf("[CLEANUP] Removed %d completed jobQ entries from server %d (remaining=%d)",
				len(completedClocks), conn.serverID, len(conn.jobQ))
		}
		
		conn.jobQMu.Unlock()
	}
	conns.RUnlock()
}

func (cm *ConsensusManager) priorityUpdateWorker() {
	for task := range cm.priorityUpdateQueue {
		if err := cm.pmgr.UpdateFollowerPriorities(task.clock, task.queue, task.leaderID); err != nil {
			log.Errorf("[PRIORITY-WORKER] Update failed for clock %d: %v", task.clock, err)
		} else {
			// Update local priority state
			if newPrios := cm.pmgr.GetFollowerPriorities(task.clock); len(newPrios) > 0 {
				if newLeaderPrio, ok := newPrios[cm.mystate.GetMyServerID()]; ok {
					cm.pstate.UpdatePriority(task.clock, newLeaderPrio)
				}
			}
		}
	}
}

// weightReassignWorker drains weightReassignQueue, re-ranking each object's
// fast-path weights based on the prior round's reply-arrival order (see
// ObjectState.ReassignWeights). Runs off the client-facing commit path so a
// slow reassignment (or a since-deleted object) never adds latency to a
// fast-path reply.
func (cm *ConsensusManager) weightReassignWorker() {
	for task := range cm.weightReassignQueue {
		if obj := cm.mystate.GetObject(task.objID); obj != nil {
			obj.ReassignWeights(task.arrivalOrder, task.coordinatorID, task.numReplicas)
		}
	}
}