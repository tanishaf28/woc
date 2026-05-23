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
	CmdPlain    [][]byte
	CmdMongo    []mongodb.Query
	PrioClock   int
	PrioVal     float64
	IsMixed     bool
	ObjIDs      []string
	ObjTypes    []int
	ForwardedBy int  
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
}

type VoteArgs struct {
	Term        int
	CandidateID int
	Priority    float64
}

type PingArgs struct {
	Clock int
}

// ===== WOC SERVICE =====

type WocService struct{}

func NewWocService() *WocService {
	return &WocService{}
}

// ===== RPC HANDLERS =====

func (s *WocService) ConsensusService(args *Args, reply *Reply) error {
	requestArrivalTime := time.Now() 
	reply.ClientClock = args.ClientClock
	
	if latencyDebug {
		log.Debugf("[LATENCY] ConsensusService called | ClientClock=%d | PrioClock=%d | Type=%d | BatchSize=%d | ArrivalTime=%v",
			args.ClientClock, args.PrioClock, args.Type, len(args.CmdPlain), requestArrivalTime.UnixMilli())
	}
	
	//processingStart := time.Now()
	var err error
	switch args.Type {
	case PlainMsg:
		err = conJobPlainMsg(args, reply)
	case MongoDB:
		err = conJobMongoDB(args, reply)
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
		
		return nil
	}

	// ===== FOLLOWER HANDLING =====
	if !isLeader {
		followerStart := time.Now()
		if latencyDebug {
			log.Debugf("[LATENCY] Follower handling request | ClientClock=%d | ObjType=%d",
				args.ClientClock, args.ObjType)
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
		} else {
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
	}

	// ===== LEADER PROCESSING =====
	leaderProcessingStart := time.Now()
	if latencyDebug {
		log.Debugf("[LATENCY] Leader processing batch | ClientClock=%d | BatchSize=%d | IsMixed=%v",
			args.ClientClock, batchSize, args.IsMixed)
	}
	
	// Analyze batch composition
	batchAnalysisStart := time.Now()
	fastCount, slowCount, hotCount := 0, 0, 0
	
	if args.IsMixed {
		for i := 0; i < batchSize; i++ {
			switch args.ObjTypes[i] {
			case IndependentObject:
				fastCount++
			case CommonObject:
				slowCount++
			case HotObject:
				hotCount++
			}
		}
	} else {
		switch args.ObjType {
		case IndependentObject:
			fastCount = batchSize
		case CommonObject:
			slowCount = batchSize
		case HotObject:
			hotCount = batchSize
		}
	}

	needsSlowPath := (slowCount + hotCount) > 0
	batchAnalysisTime := time.Since(batchAnalysisStart)
	
	if latencyDebug {
		log.Debugf("[LATENCY] Batch analysis | ClientClock=%d | Fast=%d Slow=%d Hot=%d | AnalysisTime=%vμs",
			args.ClientClock, fastCount, slowCount, hotCount, batchAnalysisTime.Microseconds())
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
	}

	if args.IsMixed {
		cmd.ObjID = fmt.Sprintf("batch-%d-%d", args.ClientID, args.ClientClock)
		if needsSlowPath {
			cmd.ObjType = CommonObject
		} else {
			cmd.ObjType = IndependentObject
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
	if args.IsMixed && (fastCount > 0 || slowCount > 0 || hotCount > 0) {
		reply.PathUsed = fmt.Sprintf("MIXED(FAST:%d,SLOW:%d,HOT:%d)", fastCount, slowCount, hotCount)
	} else if hotCount > 0 {
		reply.PathUsed = fmt.Sprintf("HOT:%d", hotCount)
	}

	// Only log if there was an error
	if !ok {
		log.Warnf("[SLOW-PATH] Consensus failed | ClientClock=%d | Time=%s",
			args.ClientClock, totalTime)
	}

	return reply.ErrorMsg
}

func conJobMongoDB(args *Args, reply *Reply) error {
	start := time.Now()
	isLeader := cm.mystate.IsLeader()

	if args.PrioVal > 0 {
		if latencyDebug {
			log.Debugf("[LATENCY] Follower voting (MongoDB immediate response) | ClientClock=%d | PrioClock=%d | ObjID=%s",
				args.ClientClock, args.PrioClock, args.ObjID)
		}
		
		reply.PathUsed = "SLOW"
		reply.LeaderClock = args.PrioClock
		reply.Success = true
		reply.Accepted = true
		reply.ExeResult = time.Since(start).String()

		if mongoDbFollower != nil && len(args.CmdMongo) > 0 {
			queries := append([]mongodb.Query(nil), args.CmdMongo...)
			go func(clientClock int, asyncQueries []mongodb.Query) {
				if _, _, err := mongoDbFollower.FollowerAPI(asyncQueries); err != nil {
					log.Errorf("[SLOW-FOLLOWER] MongoDB async apply failed | ClientClock=%d | err: %v", clientClock, err)
				}
			}(args.ClientClock, queries)
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

		if args.ObjType == IndependentObject {
			cmd := Command{
				ClientID:    args.ClientID,
				ClientClock: args.ClientClock,
				ObjID:       args.ObjID,
				ObjType:     args.ObjType,
				CmdType:     args.CmdType,
				Payload:     args.CmdMongo,
				ForwardedBy: -1,
			}
			
			ok, pathUsed := cm.HandleCommand(cmd)
			
			reply.Success = ok
			reply.PathUsed = pathUsed
			reply.Accepted = ok
			reply.LeaderClock = 0
			reply.ExeResult = time.Since(start).String()
			
			if ok {
				_, _, err := mongoDbFollower.FollowerAPI(args.CmdMongo)
				if err != nil {
					log.Errorf("MongoDB follower execution failed | err: %v", err)
					reply.ErrorMsg = err
					return err
				}
			}
			
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
		ObjID:       args.ObjID,
		ObjType:     args.ObjType,
		CmdType:     args.CmdType,
		Payload:     args.CmdMongo,
		ForwardedBy: args.ForwardedBy,
	}

	ok, path := cm.HandleCommand(cmd)
	reply.PathUsed = path
	reply.Success = ok
	reply.Accepted = ok

	if ok {
		_, _, err := mongoDbFollower.FollowerAPI(args.CmdMongo)
		if err != nil {
			log.Errorf("MongoDB leader execution failed | err: %v", err)
			reply.ErrorMsg = err
			reply.ExeResult = time.Since(start).String()
			return err
		}
	}

	reply.ExeResult = fmt.Sprintf("Total:%vms", time.Since(start).Milliseconds())

	if !ok {
		reply.ErrorMsg = fmt.Errorf("%s path MongoDB consensus failed", path)
	}

	return reply.ErrorMsg
}

func (s *WocService) RequestVote(args *VoteArgs, reply *Reply) error {
	if args.Term < cm.mystate.GetTerm() {
		reply.ErrorMsg = fmt.Errorf("stale term")
		return nil
	}

	_, myPrio := cm.pstate.GetPriority()
	if args.Priority > myPrio && !cm.mystate.CheckVotedFor() {
		cm.mystate.SetTerm(args.Term)
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


// CreateObject is DEPRECATED - objects are pre-warmed at server startup
// Kept for backward compatibility with old clients, but does nothing
func (s *WocService) CreateObject(args *struct{ ObjID string; ObjType int }, reply *struct{ Success bool }) error {
	//  NO-OP: Objects already pre-warmed in main.go
	// Just return success for backward compatibility
	log.Debugf("[DEPRECATED] CreateObject called for %s - object already pre-warmed at startup", args.ObjID)
	reply.Success = true
	return nil
}