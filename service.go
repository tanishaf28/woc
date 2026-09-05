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
	Execute     bool
	IsMixed     bool
	ObjIDs      []string
	ObjTypes    []int
	ForwardedBy int
	AlreadyForwarded bool
	RingOwners []int
	RingDead   []int
	Seq int64
	FastProvisional bool
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

func applyExecute(args *Args) bool {
	pathLabel := "SLOW"
	if args.ObjType == IndependentObject {
		pathLabel = "FAST"
	}
	switch args.Type {
	case PlainMsg:
		if args.FastProvisional {
			conns.RLock()
			numReplicas := len(conns.m) + 1
			conns.RUnlock()
			obj := cm.mystate.GetOrCreateObject(args.ObjID, args.ObjType, numReplicas, quorum, ratioTryStep)
			return obj.ApplyFastProvisional(myServerID, args.CmdPlain, args.Seq)
		} else if len(args.ObjIDs) > 1 && len(args.ObjIDs) == len(args.CmdPlain) {
			for i, id := range args.ObjIDs {
				cm.mystate.UpdateObjectCommit(id, myServerID, [][]byte{args.CmdPlain[i]}, pathLabel)
			}
		} else {
			cm.mystate.UpdateObjectCommit(args.ObjID, myServerID, args.CmdPlain, pathLabel)
		}
	case MongoDB:
		if args.FastProvisional {
			conns.RLock()
			numReplicas := len(conns.m) + 1
			conns.RUnlock()
			obj := cm.mystate.GetOrCreateObject(args.ObjID, args.ObjType, numReplicas, quorum, ratioTryStep)
			return obj.ApplyFastProvisional(myServerID, nil, args.Seq)
		}
		if mongoDbFollower != nil && len(args.CmdMongo) > 0 {
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
			args.ObjID = args.ObjIDs[0]
			if len(args.ObjTypes) > 0 {
				args.ObjType = args.ObjTypes[0]
			}
		}

		if args.ObjType == IndependentObject {
			// Follower acts as COORDINATOR for fast path
			cmd := Command{
				ClientID:         args.ClientID,
				ClientClock:      args.ClientClock,
				ObjID:            args.ObjID,
				ObjType:          args.ObjType,
				CmdType:          args.CmdType,
				Payload:          args.CmdPlain,
				ForwardedBy:      -1,
				AlreadyForwarded: args.AlreadyForwarded,
			}
			if len(args.ObjIDs) > 1 {
				cmd.ObjIDs = append([]string(nil), args.ObjIDs...)
				cmd.ObjTypes = append([]int(nil), args.ObjTypes...)
			}

			ok, pathUsed := cm.HandleCommand(&cmd)
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
		ClientID:         args.ClientID,
		ClientClock:      args.ClientClock,
		ObjID:            args.ObjID,
		ObjType:          args.ObjType,
		CmdType:          args.CmdType,
		Payload:          args.CmdPlain,
		ForwardedBy:      args.ForwardedBy,
		AlreadyForwarded: args.AlreadyForwarded,
	}

	if args.MultiObject && len(args.ObjIDs) > 1 {
		cmd.ObjID = args.ObjIDs[0]
		cmd.ObjType = DependentObject
		cmd.ObjIDs = append([]string(nil), args.ObjIDs...)
		cmd.ObjTypes = append([]int(nil), args.ObjTypes...)
		cmd.MultiObject = true
	} else if len(args.ObjIDs) > 0 {
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
	ok, path := cm.HandleCommand(&cmd)

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
				ClientID:         args.ClientID,
				ClientClock:      args.ClientClock,
				ObjID:            effectiveObjID,
				ObjType:          effectiveObjType,
				CmdType:          args.CmdType,
				Payload:          args.CmdMongo,
				ForwardedBy:      -1,
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

			if queries := writesToApply(results, args.CmdMongo, ok); len(queries) > 0 {
				go func(qs []mongodb.Query, clientClock int) {
					if _, _, err := mongoDbFollower.FollowerAPI(qs); err != nil {
						log.Errorf("[ASYNC-MONGO] follower write failed after commit | ClientClock=%d | err: %v", clientClock, err)
					}
				}(queries, args.ClientClock)
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
		ClientID:         args.ClientID,
		ClientClock:      args.ClientClock,
		ObjID:            effectiveObjID,
		ObjType:          effectiveObjType,
		CmdType:          args.CmdType,
		Payload:          args.CmdMongo,
		ForwardedBy:      args.ForwardedBy,
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
		go func(qs []mongodb.Query, clientClock int) {
			if _, _, err := mongoDbFollower.FollowerAPI(qs); err != nil {
				log.Errorf("[ASYNC-MONGO] leader write failed after commit | ClientClock=%d | err: %v", clientClock, err)
			}
		}(queries, args.ClientClock)
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
	ok, path := cm.HandleCommand(&cmd)
	reply.Success = ok
	reply.Accepted = ok
	reply.PathUsed = path
	if !ok {
		reply.ErrorMsg = fmt.Errorf("ring reconfig consensus failed")
	}
	return reply.ErrorMsg
}

func conJobFallBack(args *Args, reply *Reply) error {
	if obj := cm.mystate.GetObject(args.ObjID); obj != nil {
		obj.RevertIfSeqMatches(args.Seq)
	}
	reply.Success = true
	reply.Accepted = true
	reply.PathUsed = "FALLBACK"
	return nil
}

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

func (s *WocService) CreateObject(args *struct {
	ObjID   string
	ObjType int
}, reply *struct{ Success bool }) error {
	log.Debugf("[DEPRECATED] CreateObject called for %s - object already pre-warmed at startup", args.ObjID)
	reply.Success = true
	return nil
}
