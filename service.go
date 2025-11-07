package main

import (
	"errors"
	"time"
	"fmt"
	"sync"
	"woc/mongodb"
)

// -------------------------------------------------------------------
// WOC Service (RPC entry point for all client requests)
// -------------------------------------------------------------------
type WocService struct{}

func NewWocService() *WocService {
	return &WocService{}
}

// -------------------------------------------------------------------
// RPC Structures
// -------------------------------------------------------------------
type Args struct {
	ClientID  int
	ClientClock int // client logical clock
	ObjID     string
	ObjType   int // IndependentObject | CommonObject
	Type      int // PlainMsg, MongoDB, TPCC, Python
	CmdType	CmdType //read,write etc 
	CmdPlain  [][]byte
	CmdMongo  []mongodb.Query
	PrioClock int
	PrioVal   float64
	// Mixed batching support
	IsMixed   bool     // True if batch contains multiple object types
	ObjIDs    []string // Per-operation object IDs (for mixed batches)
	ObjTypes  []int    // Per-operation object types (for mixed batches)
}

type Reply struct {
	Accepted    bool
	ExeResult   string
	ErrorMsg    error
	PathUsed   string
	Latency     float64 
	ClientClock  int // returned client clock
	LeaderClock  int // server clock (slow path)
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

// -------------------------------------------------------------------
// RPC Handler
// -------------------------------------------------------------------
func (s *WocService) ConsensusService(args *Args, reply *Reply) error {
	start := time.Now()
	reply.ClientClock = args.ClientClock
	
	// Log every RPC entry for debugging
	myServerID := cm.mystate.GetMyServerID()
	log.Debugf("[RPC-Entry] Server=%d | PrioClock=%d | ObjID=%s | ObjType=%d | ClientClock=%d | Type=%d", 
		myServerID, args.PrioClock, args.ObjID, args.ObjType, args.ClientClock, args.Type)

	// 2. Route based on job type
	var err error
	switch args.Type {
	case PlainMsg:
		err = conJobPlainMsg(args, reply)
	case MongoDB:
		err = conJobMongoDB(args, reply)
	default:
		err = errors.New("unknown job type")
		reply.ErrorMsg = err
		log.Errorf("err: %v | type: %v", err, args.Type)
		return err
	}

	reply.Latency = time.Since(start).Seconds() * 1000
	if err != nil {
		reply.ErrorMsg = err
	} else {
		reply.ExeResult = time.Since(start).String()
	}
	log.Infof("[WOC] Completed %v job | Obj=%s | PathUsed=%s | Latency=%.2fms | Success=%v",
		args.Type, args.ObjID, reply.PathUsed, reply.Latency, reply.ErrorMsg == nil)
	return err
}

// -------------------------------------------------------------------
// Plain Message Job
// -------------------------------------------------------------------
func conJobPlainMsg(args *Args, reply *Reply) error {
	start := time.Now()
	
	myServerID := cm.mystate.GetMyServerID()
	isLeader := cm.mystate.IsLeader()
	
	log.Debugf("[PlainMsg-Entry] Server=%d | IsLeader=%v | PrioClock=%d | ObjID=%s | ObjType=%d | IsMixed=%v | PrioVal=%.2f", 
		myServerID, isLeader, args.PrioClock, args.ObjID, args.ObjType, args.IsMixed, args.PrioVal)

	// Check if this is a slow path vote request from leader
	// Leader broadcasts have PrioVal set (follower's priority for this round)
	// Client/forwarded requests have PrioVal=0
	if args.PrioVal > 0 && !isLeader {
		// Follower receiving slow path proposal from leader
		// IMPORTANT: Don't call HandleCommand - that goes through full slow path logic
		// Just commit locally and return vote
		log.Infof("[PlainMsg-Vote] Follower %d VOTING for pClock=%d | ObjID=%s | ObjType=%d | ClientClock=%d", 
			myServerID, args.PrioClock, args.ObjID, args.ObjType, args.ClientClock)
		
		// Create/get object if needed
		obj := cm.mystate.GetObject(args.ObjID)
		if obj == nil {
			conns.RLock()
			numReplicas := len(conns.m) + 1
			conns.RUnlock()
			cm.mystate.AddObject(args.ObjID, args.ObjType, numReplicas)
			obj = cm.mystate.GetObject(args.ObjID)
			log.Debugf("[PlainMsg-Vote] Follower %d created object %s", myServerID, args.ObjID)
		}
		
		// Commit locally (follower's vote = local commit)
		cm.mystate.UpdateObjectCommit(args.ObjID, myServerID, args.CmdPlain, "SLOW")
		cm.mystate.AddCommitIndex(1)
		
		obj.Lock()
		obj.LastCommitType = "SLOW"
		obj.LastCommitTime = time.Now()
		obj.Unlock()
		
		// Return successful vote
		reply.PathUsed = "SLOW"
		reply.LeaderClock = args.PrioClock
		reply.Success = true
		reply.Accepted = true
		reply.ExeResult = time.Since(start).String()
		
		log.Infof("[PlainMsg-Vote] Follower %d vote SUCCESS | ObjID=%s | pClock=%d", 
			myServerID, args.ObjID, args.PrioClock)
		
		return nil
	}

	if args.IsMixed {
		// MIXED BATCH: Process ENTIRE batch as single unit (Cabinet/EPaxos style)
		// Count operation types to determine batch path
		fastCount, slowCount, hotCount := 0, 0, 0
		
		for i := 0; i < len(args.CmdPlain); i++ {
			switch args.ObjTypes[i] {
			case IndependentObject:
				fastCount++
			case CommonObject:
				slowCount++
			case HotObject:
				hotCount++
			}
		}
		
		log.Debugf("[PlainMsg-Mixed] Batch: %d fast, %d slow, %d hot ops (processing as single unit)", 
			fastCount, slowCount, hotCount)
		
		// Determine batch path based on composition
		var batchPath string
		if slowCount > 0 || hotCount > 0 {
			// Any slow/hot ops -> entire batch uses slow path
			batchPath = "SLOW"
		} else {
			// All fast ops -> entire batch uses fast path
			batchPath = "FAST"
		}
		
		// Process ENTIRE batch as one command
		cmd := Command{
			ClientID:    args.ClientID,
			ClientClock: args.ClientClock,
			ObjID:       fmt.Sprintf("batch-%d-%d", args.ClientID, args.ClientClock),
			ObjType:     CommonObject, // Treat batch as common object
			CmdType:     args.CmdType,
			Payload:     args.CmdPlain, // Send entire batch
		}
		
		ok, path := cm.HandleCommand(cmd)
		
		failCount := 0
		if !ok {
			failCount = len(args.CmdPlain)
			log.Warnf("[PlainMsg-Mixed] Batch %d FAILED | path=%s", args.ClientClock, path)
		}
		
		// OLD PER-OPERATION CODE REMOVED - Now processing batch atomically
		/*
		// Process fast path ops with high concurrency (they're independent)
		if len(fastOps) > 0 {
			maxFastWorkers := 50 // High concurrency for fast path
			if len(fastOps) < maxFastWorkers {
				maxFastWorkers = len(fastOps)
			}
			fastSem := make(chan struct{}, maxFastWorkers)
			var fastWg sync.WaitGroup
			
			for _, idx := range fastOps {
				fastWg.Add(1)
				fastSem <- struct{}{}
				
				go func(i int) {
					defer func() {
						<-fastSem
						fastWg.Done()
					}()
					
					cmd := Command{
						ClientID:    args.ClientID,
						ClientClock: args.ClientClock,
						ObjID:       args.ObjIDs[i],
						ObjType:     args.ObjTypes[i],
						CmdType:     args.CmdType,
						Payload:     [][]byte{args.CmdPlain[i]},
					}
					
					ok, path := cm.HandleCommand(cmd)
					
					mu.Lock()
					if ok && path == "FAST" {
						fastCount++
					} else {
						failCount++
						log.Warnf("[PlainMsg-Mixed] Fast op %d FAILED", i)
					}
					mu.Unlock()
				}(idx)
			}
			fastWg.Wait()
		}
		
		// Process slow path ops with controlled concurrency
		// Slow path ops on DIFFERENT objects can run in parallel
		// Leader's slowPathMu ensures serialization when needed
		if len(slowOps) > 0 {
			maxSlowWorkers := 10 // Limit concurrency to prevent overwhelming leader
			if len(slowOps) < maxSlowWorkers {
				maxSlowWorkers = len(slowOps)
			}
			slowSem := make(chan struct{}, maxSlowWorkers)
			var slowWg sync.WaitGroup
			
			for _, idx := range slowOps {
				slowWg.Add(1)
				slowSem <- struct{}{}
				
				go func(i int) {
					defer func() {
						<-slowSem
						slowWg.Done()
					}()
					
					cmd := Command{
						ClientID:    args.ClientID,
						ClientClock: args.ClientClock,
						ObjID:       args.ObjIDs[i],
						ObjType:     args.ObjTypes[i],
						CmdType:     args.CmdType,
						Payload:     [][]byte{args.CmdPlain[i]},
					}
					
					ok, path := cm.HandleCommand(cmd)
					
					mu.Lock()
					if ok && (path == "SLOW" || path == "LEADER") {
						slowCount++
					} else {
						failCount++
						log.Warnf("[PlainMsg-Mixed] Slow op %d FAILED | path=%s", i, path)
					}
					mu.Unlock()
				}(idx)
			}
			slowWg.Wait()
		}
		
		// Process hot path ops with controlled concurrency
		// Hot objects use shared ID, so they'll be serialized by leader anyway
		// But we can still submit them in parallel to the leader
		if len(hotOps) > 0 {
			maxHotWorkers := 10
			if len(hotOps) < maxHotWorkers {
				maxHotWorkers = len(hotOps)
			}
			hotSem := make(chan struct{}, maxHotWorkers)
			var hotWg sync.WaitGroup
			
			for _, idx := range hotOps {
				hotWg.Add(1)
				hotSem <- struct{}{}
				
				go func(i int) {
					defer func() {
						<-hotSem
						hotWg.Done()
					}()
					
					cmd := Command{
						ClientID:    args.ClientID,
						ClientClock: args.ClientClock,
						ObjID:       args.ObjIDs[i],
						ObjType:     args.ObjTypes[i],
						CmdType:     args.CmdType,
						Payload:     [][]byte{args.CmdPlain[i]},
					}
					
					ok, path := cm.HandleCommand(cmd)
					
					mu.Lock()
					if ok && (path == "SLOW" || path == "LEADER") {
						hotCount++
					} else {
						failCount++
						log.Warnf("[PlainMsg-Mixed] Hot op %d FAILED | path=%s", i, path)
					}
					mu.Unlock()
				}(idx)
			}
			hotWg.Wait()
		}
		*/
		// END OLD CODE
		
		reply.Success = (failCount == 0)
		reply.Accepted = reply.Success
		reply.LeaderClock = cm.GetLeaderPClock()
		
		// Report batch processing result (atomic batch, not per-operation)
		log.Infof("[PlainMsg-Mixed] Batch complete: Fast=%d, Slow=%d, Hot=%d ops | Path=%s", 
			fastCount, slowCount, hotCount, batchPath)
		
		// Build PathUsed string showing composition
		if fastCount > 0 && (slowCount > 0 || hotCount > 0) {
			reply.PathUsed = fmt.Sprintf("MIXED(FAST:%d,SLOW:%d,HOT:%d)", fastCount, slowCount, hotCount)
		} else if fastCount > 0 {
			reply.PathUsed = "FAST"
		} else if hotCount > 0 {
			reply.PathUsed = fmt.Sprintf("HOT:%d", hotCount)
		} else if slowCount > 0 {
			reply.PathUsed = "SLOW"
		} else {
			reply.PathUsed = "FAILED"
		}
		
		if !reply.Success {
			reply.ErrorMsg = fmt.Errorf("batch processing failed")
			return reply.ErrorMsg
		}
		
		reply.ExeResult = time.Since(start).String()
		return nil
	}

	// OBJECT-SPECIFIC BATCH: Process as single command
	log.Debugf("[PlainMsg] ClientClock=%d | ObjID=%s | ObjType=%d", args.ClientClock, args.ObjID, args.ObjType)
	
	cmd := Command{
		ClientID:    args.ClientID,
		ClientClock: args.ClientClock,
		ObjID:       args.ObjID,
		ObjType:     args.ObjType,
		CmdType:     args.CmdType,
		Payload:     args.CmdPlain,
	}
	ok, path := cm.HandleCommand(cmd)
	reply.PathUsed = path
	reply.LeaderClock = cm.GetLeaderPClock() // Get actual leader clock from consensus manager
	reply.Success = ok
	reply.Accepted = ok

	if !ok {
		reply.ErrorMsg = errors.New("consensus failed (plain msg)")
		return reply.ErrorMsg
	}

	reply.ExeResult = time.Since(start).String()
	return nil
}

// -------------------------------------------------------------------
// MongoDB Job
// -------------------------------------------------------------------
func conJobMongoDB(args *Args, reply *Reply) error {
	start := time.Now()
	
	// Check if this is a slow path vote request from leader (has PrioClock set)
	if args.PrioClock > 0 && !cm.mystate.IsLeader() {
		// Follower receiving slow path proposal from leader
		log.Debugf("[MongoDB-Vote] Follower %d voting for pClock=%d | ObjID=%s", 
			cm.mystate.GetMyServerID(), args.PrioClock, args.ObjID)
		
		cmd := Command{
			ClientID:    args.ClientID,
			ClientClock: args.ClientClock,
			ObjID:       args.ObjID,
			ObjType:     args.ObjType,
			CmdType:     args.CmdType,
			Payload:     args.CmdMongo,
		}
		ok, path := cm.HandleCommand(cmd)
		reply.PathUsed = path
		reply.LeaderClock = args.PrioClock
		reply.Success = ok
		reply.Accepted = ok
		
		if !ok {
			reply.ErrorMsg = errors.New("vote failed")
			return reply.ErrorMsg
		}
		
		// Execute MongoDB query
		if ok {
			_, queryLatency, err := mongoDbFollower.FollowerAPI(args.CmdMongo)
			if err != nil {
				log.Errorf("MongoDB execution failed | err: %v | latency: %v", err, queryLatency)
				reply.ErrorMsg = err
				return err
			}
		}
		
		reply.ExeResult = time.Since(start).String()
		return nil
	}
	
	if args.IsMixed {
		// MIXED BATCH: Process operations in parallel with controlled concurrency
		log.Debugf("[MongoDB-Mixed] Processing batch with %d operations", len(args.CmdMongo))
		
		// Limit concurrency to avoid overwhelming the system
		batchSize := len(args.CmdMongo)
		maxWorkers := 20  // Process up to 20 ops in parallel (balanced for localhost)
		if batchSize < maxWorkers {
			maxWorkers = batchSize
		}
		
		semaphore := make(chan struct{}, maxWorkers)
		var wg sync.WaitGroup
		var mu sync.Mutex
		fastCount, slowCount, failCount := 0, 0, 0
		var allQueries []mongodb.Query
		
		for i := 0; i < batchSize; i++ {
			wg.Add(1)
			semaphore <- struct{}{}  // Acquire slot
			
			go func(idx int) {
				defer func() {
					<-semaphore  // Release slot
					wg.Done()
				}()
				
				cmd := Command{
					ClientID:    args.ClientID,
					ClientClock: args.ClientClock,
					ObjID:       args.ObjIDs[idx],
					ObjType:     args.ObjTypes[idx],
					CmdType:     args.CmdType,
					Payload:     []mongodb.Query{args.CmdMongo[idx]},
				}
				
				ok, path := cm.HandleCommand(cmd)
				
				mu.Lock()
				if ok {
					if path == "FAST" {
						fastCount++
					} else if path == "SLOW" {
						slowCount++
					}
					allQueries = append(allQueries, args.CmdMongo[idx])
				} else {
					failCount++
				}
				mu.Unlock()
			}(i)
		}
		
		wg.Wait()
		
		reply.Success = (failCount == 0)
		reply.Accepted = reply.Success
		reply.LeaderClock = cm.GetLeaderPClock() // Get actual leader clock
		
		if fastCount > 0 && slowCount > 0 {
			reply.PathUsed = fmt.Sprintf("MIXED(FAST:%d,SLOW:%d)", fastCount, slowCount)
		} else if fastCount > 0 {
			reply.PathUsed = "FAST"
		} else if slowCount > 0 {
			reply.PathUsed = "SLOW"
		} else {
			reply.PathUsed = "FAILED"
		}
		
		if !reply.Success {
			reply.ErrorMsg = fmt.Errorf("mixed batch had %d failures", failCount)
			return reply.ErrorMsg
		}
		
		// Execute all successful queries
		if len(allQueries) > 0 {
			_, queryLatency, err := mongoDbFollower.FollowerAPI(allQueries)
			if err != nil {
				log.Errorf("MongoDB execution failed | err: %v | latency: %v", err, queryLatency)
				reply.ErrorMsg = err
				return err
			}
		}
		
		reply.ExeResult = time.Since(start).String()
		return nil
	}
	
	// OBJECT-SPECIFIC BATCH: Process as single command
	log.Debugf("[MongoDB] Server %d | ObjID=%s | ObjType=%d", myServerID, args.ObjID, args.ObjType)
	
	cmd := Command{
		ClientID:    args.ClientID,
		ClientClock: args.ClientClock,
		ObjID:       args.ObjID,
		ObjType:     args.ObjType,
		CmdType:     args.CmdType,
		Payload:     args.CmdMongo,
	}
	ok, path := cm.HandleCommand(cmd)
	reply.PathUsed = path
	reply.LeaderClock = cm.GetLeaderPClock() // Get actual leader clock from consensus manager
	reply.Success = ok
	reply.Accepted = ok
	
	if !ok {
		reply.ErrorMsg = errors.New("consensus failed (mongodb)")
		return reply.ErrorMsg
	}
	
	_, queryLatency, err := mongoDbFollower.FollowerAPI(args.CmdMongo)
	if err != nil {
		log.Errorf("MongoDB execution failed | err: %v | latency: %v", err, queryLatency)
		reply.ErrorMsg = err
		return err
	}

	reply.ExeResult = time.Since(start).String()
	log.Debugf("[MongoDB] ObjID=%s | Path=%s | Latency=%s", args.ObjID, path, reply.ExeResult)
	return nil
}

// Convert Args (from client) to internal Command
func argsToCommand(args *Args) Command {
	var payload interface{}
    if args.Type == MongoDB {
        payload = args.CmdMongo
    } else {
        payload = args.CmdPlain
    }
	return Command{
		ClientID: args.ClientID,
		ObjID:    args.ObjID,
		CmdType:  args.CmdType,
		ObjType:  args.ObjType,
		Payload:  payload,
	}
}

func (s *WocService) RequestVote(args *VoteArgs, reply *Reply) error {
    if args.Term < cm.mystate.GetTerm() {
        reply.ErrorMsg = fmt.Errorf("stale term")
        return nil
    }
    
    // Check priority-based voting using proper state access
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