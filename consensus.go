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
	globalClock  int64
	clockMu      sync.Mutex
	serverPerfM  *eval.PerfMeter
	priorityUpdateQueue chan priorityUpdateTask
}

func NewConsensusManager(state *smr.ServerState, pmgr *smr.PriorityManager, pstate *smr.PriorityState) *ConsensusManager {
	cm := &ConsensusManager{
		mystate:             state,
		pmgr:                pmgr,
		pstate:              pstate,
		globalClock:         0,
		priorityUpdateQueue: make(chan priorityUpdateTask, 10000),
	}
	serverID := state.GetMyServerID()
	fileName := fmt.Sprintf("s%d_n%d_f%d_b%d_%s", serverID, numOfServers, quorum, batchsize, suffix)
	cm.serverPerfM = &eval.PerfMeter{}
	cm.serverPerfM.Init(1, batchsize, fileName)

	go cm.priorityUpdateWorker()
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

type VoteRequest struct {
    Term        int
    CandidateID int
    Priority    float64
}

type VoteReply struct {
    Term        int
    VoteGranted bool
    Success     bool
}

type Command struct {
	ClientID    int
	ClientClock int
	CmdType     CmdType
	ObjID       string
	ObjType     int
	Payload     interface{}
	Timestamp   time.Time  
	ForwardedBy int        
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
	
	var ok bool
	var path string
	switch cmd.ObjType {
	case IndependentObject:
		globalClock := int(cm.IncrementGlobalClock())
		cm.serverPerfM.RecordStarter(globalClock)
		defer cm.serverPerfM.RecordFinisher(globalClock)

		ok, path = cm.handleFastPath(cmd)
		if ok {
			cm.serverPerfM.IncFastPath(globalClock)
		}
	case CommonObject:
		ok, path = cm.handleSlowPath(cmd)
	case HotObject:
		log.Debugf("[HOT] Routing hot object %s to SLOW path for conflict serialization", cmd.ObjID)
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
	hotReqs := make([]*Command, 0)
	
	for _, req := range batch.requests {
		switch req.ObjType {
		case IndependentObject:
			fastReqs = append(fastReqs, req)
		case CommonObject:
			slowReqs = append(slowReqs, req)
		case HotObject:
			hotReqs = append(hotReqs, req)
		}
	}
	
	// Process each type in batch
	var wg sync.WaitGroup
	results := make([]BatchReply, len(batch.requests))
	
	// Fast path requests (parallel)
	if len(fastReqs) > 0 {
		wg.Add(len(fastReqs))
		for i, req := range fastReqs {
			go func(idx int, cmd *Command) {
				defer wg.Done()
				ok, path := cm.handleFastPath(*cmd)
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
	
	// Hot path requests (sequential conflict resolution)
	if len(hotReqs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, req := range hotReqs {
				ok, path := cm.handleSlowPath(*req)
				results[len(fastReqs)+len(slowReqs)+i] = BatchReply{Success: ok, PathUsed: path}
			}
		}()
	}
	
	wg.Wait()
	
	// Record per-batch metrics for this consensus round
	for i := 0; i < len(fastReqs); i++ {
		cm.serverPerfM.IncFastPath(globalClock)
	}
	for i := 0; i < len(slowReqs); i++ {
		cm.serverPerfM.IncSlowPath(globalClock)
	}
	for i := 0; i < len(hotReqs); i++ {
		cm.serverPerfM.IncConflict(globalClock)
	}
	
	// Send results to all waiting clients
	for i, replyChan := range batch.replies {
		replyChan <- &results[i]
	}
	
	if latencyDebug {
		log.Debugf("[SERVER-BATCH] Completed batch of %d requests (fast=%d, slow=%d, hot=%d)",
			len(batch.requests), len(fastReqs), len(slowReqs), len(hotReqs))
	}
}

func (cm *ConsensusManager) prepareArgs(cmd Command, prioClock int) *Args {
    args := &Args{
        ClientID:  cmd.ClientID,
		ClientClock: cmd.ClientClock,
        ObjID:     cmd.ObjID,
        ObjType:   cmd.ObjType,
        CmdType:   cmd.CmdType,
        Type:      evalType,
        PrioClock: prioClock,
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
    default:
        log.Warnf("prepareArgs: unknown payload type %T for ObjID=%s", v, cmd.ObjID)
    }
    return args
}

func (cm *ConsensusManager) handleFastPath(cmd Command) (bool, string) {
	fastPathStart := time.Now()
	cm.mystate.RLock()
	obj := cm.mystate.Objects[cmd.ObjID]
	cm.mystate.RUnlock()
	
	if obj == nil {
		log.Fatalf("[FATAL] Object %s missing - pre-warming failed!", cmd.ObjID)
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
			return cm.handleSlowPath(cmd)
		}
	}
	
	defer cm.inFlight.Delete(cmd.ObjID)

	myServerID := cm.mystate.GetMyServerID()
	myWeight := obj.WeightCache[myServerID]
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
	
	// Broadcast to all replicas (non-blocking)
	broadcastStart := time.Now()
	conns.RLock()
	for _, conn := range conns.m {
		cachedWeight := obj.WeightCache[conn.serverID] // Lock-free access!
		
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
	
	for !quorumReachedFlag {
		select {
		case resp := <-responsesChan:
			responseCount++
			
			// Record RTT for adaptive timeout
			netMetrics.RecordRTT(time.Since(rpcStartTime))
			
			if resp.accepted {
				weightScaled := uint64(resp.weight * 1000)
				newTotal := totalWeightAtomic.Add(weightScaled)

				if newTotal >= thresholdScaled {
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
	if quorumReachedFlag || finalWeight >= thresholdScaled {
		// Close channel if not already closed
		select {
		case <-quorumReached:
		default:
			close(quorumReached)
		}
		goto commitFastPath
	}

	// Quorum not reached - fall back to slow path
	close(quorumReached)  // Clean up channel
	cm.inFlight.Delete(cmd.ObjID)
	cm.serverPerfM.RecordFastPathFallback()
	return cm.handleSlowPath(cmd)

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
	default:
		cm.mystate.UpdateObjectCommit(cmd.ObjID, myServerID, cmd.Payload, "FAST")
	}
	cm.mystate.AddCommitIndex(batchSize)
	
	commitLatency := time.Since(commitStart)
	totalLatency := time.Since(fastPathStart)
	cm.serverPerfM.AddFastCommits(batchSize)
	
	if consensusLatencyDebug && totalLatency.Microseconds() > 500 {
		log.Infof("[FAST-PATH-BREAKDOWN] Total=%dμs | Broadcast=%dμs | Quorum=%dμs | Commit=%dμs | Weight=%.0f/%.0f",
			totalLatency.Microseconds(), broadcastLatency.Microseconds(),
			quorumLatency.Microseconds(), commitLatency.Microseconds(),
			float64(finalWeight)/1000, obj.ThresholdFast)
	}
	
	obj.Lock()
	obj.Value = nil
	obj.Unlock()
	
	fastPathDuration := time.Since(fastPathStart)
	if latencyDebug {
		log.Debugf("[CONSENSUS-LATENCY] Fast path: %v", fastPathDuration)
	}
	
	return true, "FAST"
}


func (cm *ConsensusManager) handleSlowPath(cmd Command) (bool, string) {
	obj := cm.mystate.GetObject(cmd.ObjID)
	if obj == nil {
		conns.RLock()
		numReplicas := len(conns.m) + 1
		conns.RUnlock()
		cm.mystate.AddObject(cmd.ObjID, cmd.ObjType, numReplicas)
		obj = cm.mystate.GetObject(cmd.ObjID)
	}
	
	// Forward to leader if not leader
	if !cm.mystate.IsLeader() {
		return cm.forwardToLeader(cmd)
	}
	
	req := &SlowPathRequest{
		Cmd:        cmd,
		ReplyChan:  make(chan SlowPathResult, 1),
		ReceivedAt: time.Now(),
	}
	
	slowPathQueue <- req
	result := <-req.ReplyChan
	
	return result.Success, result.PathUsed
}

func (cm *ConsensusManager) startSlowPathProcessor() {
	leaderPClock := 0
	
	log.Infof("Slow path processor started (Cabinet-style serialized)")
	
	for {
		req := <-slowPathQueue
		func() {
			cmd := req.Cmd

			// Record start time from when request arrived at queue for accurate metrics
			globalClock := int(cm.IncrementGlobalClock())
			cm.serverPerfM.RecordStarterAt(globalClock, req.ReceivedAt)

			consensusStart := time.Now()

			log.Infof("[SLOW] Processing request | ClientClock=%d | LeaderClock=%d | ObjID=%s",
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

			// Track command in flight
			cmd.Timestamp = time.Now()
			cm.inFlight.Store(cmd.ObjID, cmd)
			defer cm.inFlight.Delete(cmd.ObjID)

			// Broadcast to followers
			receiver := make(chan ReplyInfo, numOfServers)
			responseCount := 1 // Leader's vote
			forwardingServerID := cmd.ForwardedBy
			if forwardingServerID >= 0 && forwardingServerID != myServerID {
				prioQueue <- forwardingServerID
				prioSum += fpriorities[forwardingServerID]
				responseCount++

				if consensusLatencyDebug {
					log.Debugf("[SLOW-OPT] Skipped broadcast to forwarding server %d (implicit vote) | prioSum=%.2f",
						forwardingServerID, prioSum)
				}
			}

			broadcastStart := time.Now()
			conns.RLock()
			activeServers := 1
			for _, conn := range conns.m {
				if conn.serverID != myServerID {
					if conn.serverID == forwardingServerID {
						continue // Skip forwarding server
					}

					activeServers++
					args := cm.prepareArgs(cmd, leaderPClock)
					args.PrioVal = fpriorities[conn.serverID]
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

						if prioSum > cm.pmgr.GetMajority() {
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
					leaderPClock, prioSum, cm.pmgr.GetMajority())
				req.ReplyChan <- SlowPathResult{Success: false, PathUsed: "SLOW"}
				cm.serverPerfM.RecordFinisher(globalClock)
				leaderPClock++
				return
			}

			// Commit transaction
			commitStart := time.Now()
			batchSize := 1
			switch v := cmd.Payload.(type) {
			case [][]byte:
				batchSize = len(v)
				for i := 0; i < batchSize; i++ {
					objID := cmd.ObjID
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
					payload := []mongodb.Query{v[i]}
					cm.mystate.UpdateObjectCommit(objID, myServerID, payload, "SLOW")

					if opObj := cm.mystate.GetObject(objID); opObj != nil {
						opObj.Lock()
						opObj.LastCommitType = "SLOW"
						opObj.LastCommitTime = time.Now()
						opObj.Unlock()
					}
				}
			default:
				cm.mystate.UpdateObjectCommit(cmd.ObjID, myServerID, cmd.Payload, "SLOW")
			}
			cm.mystate.AddCommitIndex(batchSize)
			commitTime := time.Since(commitStart)

			if cmd.ObjType == HotObject {
				cm.serverPerfM.AddConflictCommits(batchSize)
				for i := 0; i < batchSize; i++ {
					cm.serverPerfM.IncConflict(globalClock)
				}
			} else {
				cm.serverPerfM.AddSlowCommits(batchSize)
				for i := 0; i < batchSize; i++ {
					cm.serverPerfM.IncSlowPath(globalClock)
				}
			}

			totalTime := time.Since(consensusStart)

			log.Infof("[SLOW] COMMIT | pClock=%d | Broadcast=%vμs | Quorum=%vms | Commit=%vμs | Total=%vms | prioSum=%.2f",
				leaderPClock, broadcastTime.Microseconds(), quorumTime.Milliseconds(),
				commitTime.Microseconds(), totalTime.Milliseconds(), prioSum)

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

			// Garbage collection
			if obj := cm.mystate.GetObject(cmd.ObjID); obj != nil {
				obj.Lock()
				obj.Value = nil
				obj.Unlock()
			}
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
			cmd := Command{
				ClientID:    args.ClientID,
				ClientClock: args.ClientClock,
				ObjID:       args.ObjID,
				ObjType:     args.ObjType,
				CmdType:     args.CmdType,
				Payload:     args.CmdPlain,
				ForwardedBy: cm.mystate.GetMyServerID(), 
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
	default:
		break
	}
	return
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
    defer cm.mu.Unlock()
    
    oldLeader := cm.mystate.GetLeaderID()
    pClock, _ := cm.pstate.GetPriority()
    newClock := pClock + 1
    
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
    
    if cm.StartElection() {
        log.Infof("Successfully elected new leader: Server %d", cm.mystate.GetMyServerID())
    } else {
        log.Warnf("Failed to elect new leader")
    }
}
// StartElection initiates a leader election process
func (cm *ConsensusManager) StartElection() bool {
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

    // Get current priority clock and value
    clock, myPrio := cm.pstate.GetPriority()
    
    // Initialize votes with self-vote using current priority
    votes := map[int]float64{cm.mystate.GetMyServerID(): myPrio}
    
    // Create vote request for broadcasting
    voteRequest := &VoteRequest{
        Term:        currentTerm,
        CandidateID: cm.mystate.GetMyServerID(),
        Priority:    myPrio,
    }

    // Broadcast vote requests and collect responses
    responses := make(chan float64, numOfServers)
    serverPrios := cm.pmgr.GetFollowerPriorities(clock)
    
    conns.RLock()
    for _, conn := range conns.m {
        if conn.serverID != cm.mystate.GetMyServerID() {
            go func(conn *ServerDock) {
                // Send vote request through RPC
                var voteReply VoteReply
                err := conn.txClient.Call("WocService.RequestVote", voteRequest, &voteReply)
                if err == nil && voteReply.VoteGranted {
                    if prio, ok := serverPrios[conn.serverID]; ok {
                        responses <- prio
                    }
                }
            }(conn)
        }
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
            votes[responseCount] = prio

            // Check if we have quorum based on priorities
            if prioSum > cm.pmgr.GetMajority() {
                cm.mu.Lock()
                cm.mystate.SetLeaderID(cm.mystate.GetMyServerID())
                // Update priority clock on becoming leader
                cm.pstate.UpdatePriority(clock + 1, myPrio)
                cm.mu.Unlock()
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
func (cm *ConsensusManager) HandleVoteRequest(args *VoteRequest) bool {
    cm.mu.Lock()
    defer cm.mu.Unlock()

    _, myPrio := cm.pstate.GetPriority()
    currentTerm := cm.mystate.GetTerm()

    if args.Term < currentTerm {
        return false
    }

    if args.Term > currentTerm || (args.Term == currentTerm && !cm.mystate.CheckVotedFor() && args.Priority > myPrio) {
        cm.mystate.SetTerm(args.Term)
        cm.mystate.SetVotedFor(true)
        cm.mystate.SetLeaderID(args.CandidateID)
        return true
    }

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