package main

import (
	"errors"
	"fmt"
	"os"
	"time"
	"woc/mongodb"
)

var latencyDebug = os.Getenv("LATENCY_DEBUG") == "true"


type Args struct {
	ClientID    int
	ClientClock int
	ObjID       string
	ObjType     int
	Type        int
	CmdType     CmdType
	ReadMode    int
	CmdPlain    [][]byte
	CmdMongo    []mongodb.Query
	PrioClock   int
	PrioVal     float64
	// Execute marks a priority-vote broadcast (PrioVal > 0) that also
	// carries real content needing local application — set on the fast
	// path's peer broadcast and the slow path's follower broadcast, so
	// every voting replica actually applies the command instead of just
	// acking. Independent of PrioVal so the two concerns can't be
	// conflated again (see consensus.go's forwardToObjectOwner for the
	// other half of this fix).
	Execute     bool
	IsMixed     bool
	ObjIDs      []string
	ObjTypes    []int
	ForwardedBy int
	// AlreadyForwarded mirrors Command.AlreadyForwarded across the wire -
	// see consensus.go for why this exists (breaks an infinite forward loop
	// between replicas that disagree about object ownership).
	AlreadyForwarded bool
	// RingOwners/RingDead carry a RingUpdate's fields across the wire when
	// Type == RingReconfig (see objectmap.go's RingUpdate and
	// consensus.go's prepareArgs).
	RingOwners []int
	RingDead   []int
	// Seq is the per-object sequence number a fast-path broadcast is
	// provisionally applying (see ObjectState.NextSeq), or the sequence a
	// Type == FallBack message is asking followers to revert (see
	// ObjectState.RevertIfSeqMatches).
	Seq int64
	// FastProvisional marks a fast-path peer broadcast whose apply must go
	// through the Seq-guarded, undoable ObjectState.ApplyFastProvisional
	// instead of the unconditional UpdateObjectCommit. Deliberately a
	// dedicated flag rather than inferred from ObjType == IndependentObject:
	// fallbackToSlowPath never rewrites ObjType, so a command that fell back
	// to the slow path after a failed fast-path attempt still carries
	// ObjType == IndependentObject all the way through - an ObjType-based
	// check would wrongly seq-guard that slow-path follower apply too.
	FastProvisional bool
	// MultiObject marks a command as one atomic transaction spanning
	// ObjIDs/ObjTypes together (always routed dependent/slow-path),
	// distinct from IsMixed's "N independent single-object ops bundled for
	// wire efficiency" - the two are mutually exclusive interpretations of
	// the same ObjIDs/ObjTypes fields.
	MultiObject bool
}

type Reply struct {
	Accepted    bool
	ExeResult   string
	ErrorMsg    error
	PathUsed    string
	Latency     float64
	ClientClock int
	LeaderClock int
	Success     bool
	ReadResult  interface{}
}

type VoteArgs struct {
	Term        int
	CandidateID int
	Priority    float64
}

type PingArgs struct {
	Clock int
}

// applyExecute makes this replica actually apply a fast/slow-path
// broadcast's content (args.Execute == true) instead of just
// acknowledging it. This is the real per-replica "Respond/Execute" the
// paper's CORA-CT template calls for (§4.3-§4.4) — every voting replica
// converges on the committed value, not just the coordinator/leader.
// applyExecute's return value is the real accept/reject signal for the RPC:
// callers must propagate it into reply.Accepted rather than assuming
// success, because FastProvisional's seq guard (see ObjectState.
// ApplyFastProvisional) can genuinely reject a stale/regressive proposal.
// Counting a rejected follower's weight toward the coordinator's quorum
// anyway would silently defeat that guard - the coordinator could declare
// fast-path success while some "accepting" replicas never actually stored
// the value. Every other case here is unconditionally true, matching their
// existing fire-and-forget semantics (unchanged from before this comment).
func applyExecute(args *Args) bool {
	pathLabel := "SLOW"
	if args.ObjType == IndependentObject {
		pathLabel = "FAST"
	}
	switch args.Type {
	case PlainMsg:
		if args.FastProvisional {
			obj := cm.mystate.GetObject(args.ObjID)
			if obj == nil {
				return false
			}
			return obj.ApplyFastProvisional(myServerID, args.CmdPlain, args.Seq)
		} else if len(args.ObjIDs) > 1 && len(args.ObjIDs) == len(args.CmdPlain) {
			// Multi-object transaction: ObjIDs[i] <-> CmdPlain[i], apply each
			// constituent object's slice of the payload to that object, not
			// all of them to args.ObjID.
			for i, id := range args.ObjIDs {
				cm.mystate.UpdateObjectCommit(id, myServerID, [][]byte{args.CmdPlain[i]}, pathLabel)
			}
		} else {
			cm.mystate.UpdateObjectCommit(args.ObjID, myServerID, args.CmdPlain, pathLabel)
		}
	case MongoDB:
		if args.FastProvisional {
			// A fast-path vote must NOT write to the real, external MongoDB
			// yet: unlike PlainMsg's in-memory ObjectState.Value (cheaply
			// undone by RevertIfSeqMatches if the round never reaches
			// quorum), a MongoDB write has no cheap undo here, and this
			// replica has no way yet to know whether the round will
			// actually commit. Only the monotonicity guard runs now (same
			// seq check PlainMsg uses, via ApplyFastProvisional, keyed on
			// the object but not actually storing CmdMongo as its Value) so
			// a stale/regressive proposal is still correctly rejected and
			// doesn't count toward the coordinator's quorum. The real write
			// happens later, only for a round that actually committed - see
			// MongoConfirm (handleFastPath's commitFastPath broadcasts it
			// after reaching quorum) and conJobMongoConfirm.
			obj := cm.mystate.GetObject(args.ObjID)
			if obj == nil {
				return false
			}
			return obj.ApplyFastProvisional(myServerID, nil, args.Seq)
		}
		if mongoDbFollower != nil && len(args.CmdMongo) > 0 {
			// Slow-path follower vote (FastProvisional is false here): the
			// paper's slow path treats a follower's apply as provisional but
			// always reconciled to the leader's eventual decision (§4.4) -
			// the leader is the sole authority on commit order, and any
			// divergent state on a follower gets overridden by a
			// subsequent real commit or a new leader's log, so no separate
			// defer/confirm step is needed here the way FastProvisional
			// requires above. Synchronous: this replica's vote/ack must
			// reflect whether the write actually landed locally, not just
			// that consensus agreed on it.
			if _, _, err := mongoDbFollower.FollowerAPI(args.CmdMongo); err != nil {
				log.Errorf("[EXECUTE] replica %d failed to apply MongoDB batch for %s: %v", myServerID, args.ObjID, err)
				return false
			}
		}
	case RingReconfig:
		applyRingUpdate(RingUpdate{OwnerByIndex: args.RingOwners, DeadReplicas: args.RingDead})
	}
	return true
}

// ===== WOC SERVICE =====

type WocService struct{}

func NewWocService() *WocService {
	return &WocService{}
}

// ===== RPC HANDLERS =====

func (s *WocService) ConsensusService(args *Args, reply *Reply) error {
	activeRPCs.Add(1)
	defer activeRPCs.Add(-1)

	requestArrivalTime := time.Now()
	reply.ClientClock = args.ClientClock

	if latencyDebug {
		log.Debugf("[LATENCY] ConsensusService called | ClientClock=%d | PrioClock=%d | Type=%d | BatchSize=%d | ArrivalTime=%v",
			args.ClientClock, args.PrioClock, args.Type, len(args.CmdPlain), requestArrivalTime.UnixMilli())
	}
	
	if args.CmdType == READ {
		cmd := Command{
			ClientID:    args.ClientID,
			ClientClock: args.ClientClock,
			CmdType:     args.CmdType,
			ReadMode:    args.ReadMode,
			ObjID:       args.ObjID,
			ObjType:     args.ObjType,
		}
		if len(args.ObjIDs) > 1 {
			// A multi-key read batch (client.go sets ObjIDs/ObjTypes for
			// any read batch >1) - preserve every key so handleRead's
			// handleReadBatch split can read each one, instead of silently
			// only reading args.ObjID (the first key) while still reporting
			// the whole batch as successful.
			cmd.ObjIDs = append([]string(nil), args.ObjIDs...)
			cmd.ObjTypes = append([]int(nil), args.ObjTypes...)
		}
		ok, value, path := cm.handleRead(cmd)
		reply.Success = ok
		reply.Accepted = ok
		reply.PathUsed = path
		reply.ReadResult = value
		reply.Latency = time.Since(requestArrivalTime).Seconds() * 1000
		if !ok {
			reply.ErrorMsg = fmt.Errorf("%s read failed", path)
		}
		return reply.ErrorMsg
	}

	//processingStart := time.Now()
	var err error
	switch args.Type {
	case PlainMsg:
		err = conJobPlainMsg(args, reply)
	case MongoDB:
		err = conJobMongoDB(args, reply)
	case RingReconfig:
		err = conJobRingReconfig(args, reply)
	case FallBack:
		err = conJobFallBack(args, reply)
	case MongoConfirm:
		err = conJobMongoConfirm(args, reply)
	default:
		err = errors.New("unknown job type")
		reply.ErrorMsg = err
		return err
	}
	//processingTime := time.Since(processingStart)

	totalLatency := time.Since(requestArrivalTime)
	reply.Latency = totalLatency.Seconds() * 1000 
	
	if latencyDebug {
		log.Debugf("[LATENCY-BREAKDOWN] Full RPC processing=%vms (not in CSV)", 
			totalLatency.Milliseconds())
	}
	
	if err != nil {
		reply.ErrorMsg = err
	}
	return err
}

func conJobPlainMsg(args *Args, reply *Reply) error {
	start := time.Now()
	isLeader := cm.mystate.IsLeader()
	batchSize := len(args.CmdPlain)


	if args.PrioVal > 0 {
		if latencyDebug {
			log.Debugf("[LATENCY] Follower voting (immediate response) | ClientClock=%d | PrioClock=%d | ObjID=%s",
				args.ClientClock, args.PrioClock, args.ObjID)
		}

		reply.PathUsed = "SLOW"
		reply.LeaderClock = args.PrioClock
		reply.Success = true
		reply.Accepted = true
		reply.ExeResult = time.Since(start).String()

		if args.Execute {
			// Must reflect the real apply result: FastProvisional's seq
			// guard can genuinely reject this proposal, and a coordinator
			// that keeps counting it as accepted would count weight toward
			// quorum for a value this replica never actually stored.
			reply.Accepted = applyExecute(args)
			reply.Success = reply.Accepted
		}

		return nil
	}

	// ===== FOLLOWER HANDLING =====
	if !isLeader {
		followerStart := time.Now()
		if args.MultiObject && len(args.ObjIDs) > 0 {
			// A multi-object transaction is always dependent, regardless of
			// the individual objects' types - never treat it as an
			// independent-object fast-path candidate below.
			args.ObjID = args.ObjIDs[0]
			args.ObjType = DependentObject
		} else if len(args.ObjIDs) > 0 {
			// Non-atomic multi-object batch: IsMixed ("each element
			// independently typed"), or the default "object-specific"/
			// "single_obj" client compositions, which populate
			// ObjIDs/ObjTypes per element without setting IsMixed at all.
			// args.ObjID/ObjType (first element) still gate the routing
			// decision below and are used for logging/inFlight-key
			// compatibility, but every element's real ID/type is preserved
			// in cmd.ObjIDs/ObjTypes so HandleCommand can route/commit each
			// one against its own object instead of collapsing the whole
			// batch onto the first.
			args.ObjID = args.ObjIDs[0]
			if len(args.ObjTypes) > 0 {
				args.ObjType = args.ObjTypes[0]
			}
		}

		if args.ObjType == IndependentObject {
			// Follower acts as COORDINATOR for fast path
			cmd := Command{
				ClientID:    args.ClientID,
				ClientClock: args.ClientClock,
				ObjID:       args.ObjID,
				ObjType:     args.ObjType,
				CmdType:     args.CmdType,
				Payload:     args.CmdPlain,
				ForwardedBy: -1,
				AlreadyForwarded: args.AlreadyForwarded,
			}
			if len(args.ObjIDs) > 1 {
				cmd.ObjIDs = append([]string(nil), args.ObjIDs...)
				cmd.ObjTypes = append([]int(nil), args.ObjTypes...)
			}

			ok, pathUsed := cm.HandleCommand(cmd)
			followerLatency := time.Since(followerStart)

			if latencyDebug {
				log.Debugf("[LATENCY-BREAKDOWN] Follower fast-path coordination | ClientClock=%d | Path=%s | Latency=%vms",
					args.ClientClock, pathUsed, followerLatency.Milliseconds())
			}

			reply.Success = ok
			reply.PathUsed = pathUsed
			reply.Accepted = ok
			reply.LeaderClock = 0 // Fast path doesn't use leader clock
			reply.ExeResult = time.Since(start).String()

			if !ok {
				reply.ErrorMsg = fmt.Errorf("fast path consensus failed")
			}
			return reply.ErrorMsg
		}

		ok, pathUsed := cm.forwardToLeaderOptimized(args, reply)
		followerLatency := time.Since(followerStart)

		if latencyDebug {
			log.Debugf("[LATENCY-BREAKDOWN] Follower forward-to-leader | ClientClock=%d | Path=%s | Latency=%vms",
				args.ClientClock, pathUsed, followerLatency.Milliseconds())
		}

		reply.ExeResult = time.Since(start).String()

		if !ok {
			reply.ErrorMsg = fmt.Errorf("forward to leader failed")
		}
		return reply.ErrorMsg
	}

	// ===== LEADER PROCESSING =====
	leaderProcessingStart := time.Now()
	if latencyDebug {
		log.Debugf("[LATENCY] Leader processing batch | ClientClock=%d | BatchSize=%d | IsMixed=%v",
			args.ClientClock, batchSize, args.IsMixed)
	}

	// Analyze batch composition
	batchAnalysisStart := time.Now()
	fastCount, slowCount := 0, 0

	if args.MultiObject {
		// A transaction spanning >1 object is always dependent (paper
		// classification), regardless of the individual objects' types.
		slowCount = batchSize
	} else if args.IsMixed {
		for i := 0; i < batchSize; i++ {
			switch args.ObjTypes[i] {
			case IndependentObject:
				fastCount++
			case DependentObject:
				slowCount++
			}
		}
	} else {
		switch args.ObjType {
		case IndependentObject:
			fastCount = batchSize
		case DependentObject:
			slowCount = batchSize
		}
	}

	batchAnalysisTime := time.Since(batchAnalysisStart)

	if latencyDebug {
		log.Debugf("[LATENCY] Batch analysis | ClientClock=%d | Fast=%d Slow=%d | AnalysisTime=%vμs",
			args.ClientClock, fastCount, slowCount, batchAnalysisTime.Microseconds())
	}

	// Build command from args
	cmd := Command{
		ClientID:    args.ClientID,
		ClientClock: args.ClientClock,
		ObjID:       args.ObjID,
		ObjType:     args.ObjType,
		CmdType:     args.CmdType,
		Payload:     args.CmdPlain,
		ForwardedBy: args.ForwardedBy,
		AlreadyForwarded: args.AlreadyForwarded,
	}

	if args.MultiObject && len(args.ObjIDs) > 1 {
		// One atomic transaction spanning ObjIDs together - always
		// dependent, regardless of the individual objects' types.
		cmd.ObjID = args.ObjIDs[0]
		cmd.ObjType = DependentObject
		cmd.ObjIDs = append([]string(nil), args.ObjIDs...)
		cmd.ObjTypes = append([]int(nil), args.ObjTypes...)
		cmd.MultiObject = true
	} else if len(args.ObjIDs) > 0 {
		// Non-atomic multi-object batch (IsMixed, or the default
		// "object-specific"/"single_obj" client compositions, which
		// populate ObjIDs/ObjTypes per element without setting IsMixed at
		// all): preserve every element's real ID/type so
		// HandleCommand's handleIndependentBatch can route/commit each one
		// against its own object - via its own owner, weight vector,
		// threshold, and sequence number - instead of forcing the whole
		// batch through one shared round keyed on the first element
		// (previously that meant either collapsing every element's write
		// onto the first object, or, for the "needsSlowPath" case,
		// pessimistically forcing independent elements onto the slow path
		// just because some other element in the batch needed it).
		cmd.ObjID = args.ObjIDs[0]
		if len(args.ObjTypes) > 0 {
			cmd.ObjType = args.ObjTypes[0]
		}
		if len(args.ObjIDs) > 1 {
			cmd.ObjIDs = append([]string(nil), args.ObjIDs...)
			cmd.ObjTypes = append([]int(nil), args.ObjTypes...)
		}
	}

	processingStart := time.Now()
	ok, path := cm.HandleCommand(cmd)

	processingTime := time.Since(processingStart)
	totalTime := time.Since(start)
	leaderTotalTime := time.Since(leaderProcessingStart)

	if latencyDebug {
		log.Debugf("[LATENCY-BREAKDOWN] Path completed | ClientClock=%d | Path=%s | ConsensusProcessing=%vμs | BatchAnalysis=%vμs | Total=%vms",
			args.ClientClock, path, processingTime.Microseconds(),
			batchAnalysisTime.Microseconds(), totalTime.Milliseconds())
	}

	reply.Success = ok
	reply.PathUsed = path
	reply.Accepted = ok
	reply.LeaderClock = 0
	reply.Latency = totalTime.Seconds() * 1000 // milliseconds
	reply.ExeResult = fmt.Sprintf("Total:%vms|Processing:%vμs|LeaderTotal:%vms",
		totalTime.Milliseconds(), processingTime.Microseconds(), leaderTotalTime.Milliseconds())

	if !ok {
		reply.ErrorMsg = fmt.Errorf("%s path consensus failed", path)
	}

	// Override PathUsed for mixed batches to show composition
	if args.IsMixed && (fastCount > 0 || slowCount > 0) {
		reply.PathUsed = fmt.Sprintf("MIXED(FAST:%d,SLOW:%d)", fastCount, slowCount)
	}

	// Only log if there was an error
	if !ok {
		log.Warnf("[SLOW-PATH] Consensus failed | ClientClock=%d | Time=%s",
			args.ClientClock, totalTime)
	}

	return reply.ErrorMsg
}

// writesToApply picks which of a batch's Mongo queries should actually be
// written, given HandleCommandDetailed's per-element results. results == nil
// signals no split happened (a single-object command, or an atomic
// MultiObject transaction) - the whole batch shares one commit decision, so
// return every query if it succeeded, none otherwise. For a split batch,
// only the elements whose own sub-round actually committed get written:
// gating a successfully-committed element's write on an unrelated sibling's
// failure would mean that element's agreed value never reaches MongoDB for
// no reason connected to its own consensus outcome.
func writesToApply(results []BatchElementResult, all []mongodb.Query, aggregateOK bool) []mongodb.Query {
	if results == nil {
		if aggregateOK {
			return all
		}
		return nil
	}
	out := make([]mongodb.Query, 0, len(results))
	for _, r := range results {
		if r.OK && r.Index < len(all) {
			out = append(out, all[r.Index])
		}
	}
	return out
}

func conJobMongoDB(args *Args, reply *Reply) error {
	start := time.Now()
	isLeader := cm.mystate.IsLeader()
	batchSize := len(args.CmdMongo)

	fastCount, slowCount := 0, 0
	if args.IsMixed {
		for i := 0; i < batchSize && i < len(args.ObjTypes); i++ {
			switch args.ObjTypes[i] {
			case IndependentObject:
				fastCount++
			case DependentObject:
				slowCount++
			}
		}
	} else {
		switch args.ObjType {
		case IndependentObject:
			fastCount = batchSize
		case DependentObject:
			slowCount = batchSize
		}
	}

	needsSlowPath := slowCount > 0
	effectiveObjID := args.ObjID
	effectiveObjType := args.ObjType
	// Route by the first real YCSB key in the batch, not a synthetic
	// per-batch ID: a synthetic "batch-<clientID>-<clientClock>" string has
	// no rightful owner on the object hash ring, so fast-path coordination/
	// weights ended up computed for a pseudo-object instead of any key
	// actually being written. This mirrors plain-msg's pre-existing "mixed
	// batch" handling exactly (conJobPlainMsg's IsMixed branch): a batch of
	// independent per-key ops bundled only for round-trip efficiency is NOT
	// the same thing as one atomic multi-object transaction (that's what
	// MultiObject/Feature 3 is for) - forcing every multi-key batch onto the
	// slow path just because it spans >1 object would make fast path and
	// batching mutually exclusive for MongoDB workloads. Only force
	// dependent if some element in the batch actually needs it.
	if args.IsMixed && len(args.ObjIDs) > 0 && len(args.ObjTypes) > 0 {
		effectiveObjID = args.ObjIDs[0]
		if needsSlowPath {
			effectiveObjType = DependentObject
		} else {
			effectiveObjType = args.ObjTypes[0]
		}
	}

	if args.PrioVal > 0 {
		if latencyDebug {
			log.Debugf("[LATENCY] Follower voting (MongoDB immediate response) | ClientClock=%d | PrioClock=%d | ObjID=%s",
				args.ClientClock, args.PrioClock, args.ObjID)
		}

		applyAsSlowPath := args.PrioClock > 0 && effectiveObjType == DependentObject
		if applyAsSlowPath {
			reply.PathUsed = "SLOW"
		} else {
			reply.PathUsed = "FAST"
		}
		reply.LeaderClock = args.PrioClock
		reply.Success = true
		reply.Accepted = true
		reply.ExeResult = time.Since(start).String()

		if args.Execute {
			reply.Accepted = applyExecute(args)
			reply.Success = reply.Accepted
		}

		return nil
	}

	if mongoDbFollower == nil {
		err := fmt.Errorf("mongodb follower not initialized on server %d", myServerID)
		log.Errorf("%v", err)
		reply.ErrorMsg = err
		return err
	}

	if !isLeader {
		if latencyDebug {
			log.Debugf("[LATENCY] Follower MongoDB handling request | ClientClock=%d | ObjType=%d",
				args.ClientClock, args.ObjType)
		}

		if effectiveObjType == IndependentObject {
			cmd := Command{
				ClientID:    args.ClientID,
				ClientClock: args.ClientClock,
				ObjID:       effectiveObjID,
				ObjType:     effectiveObjType,
				CmdType:     args.CmdType,
				Payload:     args.CmdMongo,
				ForwardedBy: -1,
				AlreadyForwarded: args.AlreadyForwarded,
			}
			if len(args.ObjIDs) > 1 {
				cmd.ObjIDs = append([]string(nil), args.ObjIDs...)
				cmd.ObjTypes = append([]int(nil), args.ObjTypes...)
			}

			results, ok, pathUsed := cm.HandleCommandDetailed(cmd)

			reply.Success = ok
			reply.PathUsed = pathUsed
			reply.Accepted = ok
			reply.LeaderClock = 0

			// Only write the queries whose own sub-round actually committed
			// (see writesToApply) - for a batch spanning several distinct
			// keys, each key's commit decision came from its own object's
			// weight vector/threshold (HandleCommandDetailed's split), so a
			// sibling key's failure must not withhold this key's already-
			// agreed write.
			if queries := writesToApply(results, args.CmdMongo, ok); len(queries) > 0 {
				// Synchronous, matching the leader's own write below: a
				// client must not be told "success" before its write is
				// actually durable on this replica's local MongoDB. The
				// previous fire-and-forget goroutine here could report
				// consensus success while the real write silently failed,
				// with only a log line and no way for the caller to know.
				if _, _, err := mongoDbFollower.FollowerAPI(queries); err != nil {
					log.Errorf("MongoDB follower execution failed | ClientClock=%d | err: %v", args.ClientClock, err)
					reply.Success = false
					reply.Accepted = false
					reply.ErrorMsg = err
					reply.ExeResult = time.Since(start).String()
					return err
				}
			}
			reply.ExeResult = time.Since(start).String()

			if !ok {
				reply.ErrorMsg = fmt.Errorf("fast path MongoDB consensus failed")
			}
			return reply.ErrorMsg
		}
		
		ok, pathUsed := cm.forwardToLeaderOptimized(args, reply)
		followerLatency := time.Since(start)

		if latencyDebug {
			log.Debugf("[LATENCY-BREAKDOWN] Follower MongoDB forward-to-leader | ClientClock=%d | Path=%s | Latency=%vms",
				args.ClientClock, pathUsed, followerLatency.Milliseconds())
		}

		reply.ExeResult = time.Since(start).String()

		if !ok {
			reply.ErrorMsg = fmt.Errorf("MongoDB forward to leader failed")
		}
		return reply.ErrorMsg
	}

	cmd := Command{
		ClientID:    args.ClientID,
		ClientClock: args.ClientClock,
		ObjID:       effectiveObjID,
		ObjType:     effectiveObjType,
		CmdType:     args.CmdType,
		Payload:     args.CmdMongo,
		ForwardedBy: args.ForwardedBy,
		AlreadyForwarded: args.AlreadyForwarded,
	}
	if len(args.ObjIDs) > 1 {
		cmd.ObjIDs = append([]string(nil), args.ObjIDs...)
		cmd.ObjTypes = append([]int(nil), args.ObjTypes...)
	}

	results, ok, path := cm.HandleCommandDetailed(cmd)
	reply.PathUsed = path
	reply.Success = ok
	reply.Accepted = ok

	if queries := writesToApply(results, args.CmdMongo, ok); len(queries) > 0 {
		_, _, err := mongoDbFollower.FollowerAPI(queries)
		if err != nil {
			log.Errorf("MongoDB leader execution failed | ClientClock=%d | err: %v", args.ClientClock, err)
			reply.ErrorMsg = err
			reply.ExeResult = time.Since(start).String()
			return err
		}
	}

	totalTime := time.Since(start)
	reply.Latency = totalTime.Seconds() * 1000
	reply.ExeResult = fmt.Sprintf("Total:%vms", time.Since(start).Milliseconds())

	if args.IsMixed && (fastCount > 0 || slowCount > 0) {
		reply.PathUsed = fmt.Sprintf("MIXED(FAST:%d,SLOW:%d)", fastCount, slowCount)
	}

	if !ok {
		reply.ErrorMsg = fmt.Errorf("%s path MongoDB consensus failed", path)
	}

	return reply.ErrorMsg
}

// conJobRingReconfig handles a RingReconfig command: a ring/dead-replica-set
// update proposed by the leader's replica health monitor (see
// consensus.go's startReplicaHealthMonitor), routed through the same
// job-dispatch pattern as conJobPlainMsg/conJobMongoDB so every replica
// applies it at the same slow-path consensus order position instead of
// the leader applying it locally and best-effort-pushing it out.
func conJobRingReconfig(args *Args, reply *Reply) error {
	start := time.Now()

	if args.PrioVal > 0 {
		reply.PathUsed = "SLOW"
		reply.LeaderClock = args.PrioClock
		reply.Success = true
		reply.Accepted = true
		reply.ExeResult = time.Since(start).String()

		if args.Execute {
			reply.Accepted = applyExecute(args)
			reply.Success = reply.Accepted
		}

		return nil
	}

	if !cm.mystate.IsLeader() {
		ok, pathUsed := cm.forwardToLeaderOptimized(args, reply)
		reply.PathUsed = pathUsed
		if !ok {
			reply.ErrorMsg = fmt.Errorf("ring reconfig forward to leader failed")
		}
		return reply.ErrorMsg
	}

	cmd := Command{
		ClientID:    args.ClientID,
		ClientClock: args.ClientClock,
		ObjID:       ringObjectID,
		ObjType:     DependentObject,
		CmdType:     WRITE,
		Payload:     RingUpdate{OwnerByIndex: args.RingOwners, DeadReplicas: args.RingDead},
		ForwardedBy: args.ForwardedBy,
	}
	ok, path := cm.HandleCommand(cmd)
	reply.Success = ok
	reply.Accepted = ok
	reply.PathUsed = path
	if !ok {
		reply.ErrorMsg = fmt.Errorf("ring reconfig consensus failed")
	}
	return reply.ErrorMsg
}

// conJobFallBack handles a FallBack broadcast: the fast-path coordinator's
// best-effort notice that it failed to reach quorum for (ObjID, Seq), so any
// replica still holding that seq's provisional apply should revert it (see
// ObjectState.RevertIfSeqMatches). Deliberately NOT routed through
// HandleCommand/slow-path consensus - reverting a replica's own local,
// idempotent provisional state doesn't need cluster agreement, and missing
// this message is safe: the next real commit for the object (a fresh
// fast-path attempt, or the coordinator's own slow-path retry of the same
// txn) carries a higher seq and supersedes the stale value on any replica
// that never got this broadcast.
func conJobFallBack(args *Args, reply *Reply) error {
	if obj := cm.mystate.GetObject(args.ObjID); obj != nil {
		obj.RevertIfSeqMatches(args.Seq)
	}
	reply.Success = true
	reply.Accepted = true
	reply.PathUsed = "FALLBACK"
	return nil
}

// conJobMongoConfirm handles a MongoConfirm broadcast: the fast-path
// coordinator's best-effort notice that a MongoDB command actually reached
// quorum, so this replica should now physically apply it (see applyExecute's
// MongoDB case, and the MongoConfirm const doc in parameters.go for why the
// write is deferred to this point instead of happening during the vote).
// Deliberately NOT routed through consensus - every recipient already voted
// yes for this exact (ObjID, Seq) during the fast-path round, so applying
// the already-agreed value needs no further agreement. A replica that misses
// this message simply serves stale reads for that key until the next write
// reaches it; it never diverges into an incorrect state, since it never
// wrote anything for this round in the first place.
func conJobMongoConfirm(args *Args, reply *Reply) error {
	if mongoDbFollower != nil && len(args.CmdMongo) > 0 {
		if _, _, err := mongoDbFollower.FollowerAPI(args.CmdMongo); err != nil {
			log.Errorf("[MONGO-CONFIRM] replica %d failed to apply confirmed MongoDB batch for %s: %v", myServerID, args.ObjID, err)
			reply.Success = false
			reply.ErrorMsg = err
			return err
		}
	}
	reply.Success = true
	reply.Accepted = true
	reply.PathUsed = "MONGO-CONFIRM"
	return nil
}

func (s *WocService) RequestVote(args *VoteArgs, reply *Reply) error {
	if args.Term < cm.mystate.GetTerm() {
		reply.ErrorMsg = fmt.Errorf("stale term")
		return nil
	}

	// Observing a strictly newer term always makes this replica "catch up"
	// and become eligible to vote again this term, regardless of whether it
	// ends up granting its vote to THIS particular candidate - otherwise,
	// once any replica ever voted once, votedFor would stay true forever
	// (it's a single flag, not tracked per-term) and it could never vote in
	// any later election.
	if args.Term > cm.mystate.GetTerm() {
		cm.mystate.SetTerm(args.Term)
		cm.mystate.ResetVotedFor()
	}

	_, myPrio := cm.pstate.GetPriority()
	if args.Priority > myPrio && !cm.mystate.CheckVotedFor() {
		cm.mystate.SetLeaderID(args.CandidateID)
		cm.mystate.SetVotedFor(true)
		reply.Success = true
	}

	return nil
}

func (s *WocService) Ping(args *PingArgs, reply *Reply) error {
	reply.Success = true
	reply.ClientClock = args.Clock
	return nil
}

// ReadVote is the RPC used by safe reads (consensus.go's quorumRead) to
// gather both a weight and this replica's own value for an object —
// deliberately a separate RPC method rather than reusing
// ConsensusService/PrioVal, to avoid the exact ambiguous-tagging bug fixed
// for fast/slow-path broadcasts elsewhere. The coordinator returns the
// value from whichever responding replica carries the highest weight
// (the most recent fast responder under Cabinet's weight reassignment,
// i.e. the last proposer by definition).
type ReadVoteArgs struct {
	ObjID   string
	ObjType int
}

type ReadVoteReply struct {
	Weight   float64
	Value    interface{}
	Accepted bool
}

func (s *WocService) ReadVote(args *ReadVoteArgs, reply *ReadVoteReply) error {
	if args.ObjType == IndependentObject {
		obj := cm.mystate.GetObject(args.ObjID)
		if obj == nil {
			reply.Accepted = false
			return nil
		}
		reply.Weight = obj.GetReplicaWeight(cm.mystate.GetMyServerID())
	} else {
		_, myPrio := cm.pstate.GetPriority()
		reply.Weight = myPrio
	}

	found, value, _ := cm.readLocalValue(Command{ObjID: args.ObjID, ObjType: args.ObjType}, "")
	reply.Accepted = found
	reply.Value = value
	return nil
}

// GetObjectOwnership serves the object-mapping hash ring's result to
// followers (paper §4.2: the global leader builds the ring and
// disseminates the mapping). Only the leader can answer this — followers
// fetch from it once at startup instead of computing their own ring.
type ObjectOwnershipArgs struct{}

type ObjectOwnershipReply struct {
	OwnerByIndex []int
	DeadReplicas []int
}

func (s *WocService) GetObjectOwnership(args *ObjectOwnershipArgs, reply *ObjectOwnershipReply) error {
	if !cm.mystate.IsLeader() {
		return fmt.Errorf("server %d is not the leader", myServerID)
	}
	reply.OwnerByIndex = OwnershipSnapshot()
	reply.DeadReplicas = DeadReplicasSnapshot()
	return nil
}

// CreateObject is DEPRECATED - objects are pre-warmed at server startup
// Kept for backward compatibility with old clients, but does nothing
func (s *WocService) CreateObject(args *struct{ ObjID string; ObjType int }, reply *struct{ Success bool }) error {
	//  NO-OP: Objects already pre-warmed in main.go
	// Just return success for backward compatibility
	log.Debugf("[DEPRECATED] CreateObject called for %s - object already pre-warmed at startup", args.ObjID)
	reply.Success = true
	return nil
}