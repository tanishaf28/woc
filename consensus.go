package main

import (
	"sync"
	"io"
	"net/rpc"
	"math/rand"
	"time"
	"woc/mongodb"
	"woc/smr"
)

type ConsensusManager struct {
	mu       sync.Mutex
	pmgr     *smr.PriorityManager
	pstate   *smr.PriorityState
	mystate  *smr.ServerState
	inFlight map[string]Command
	leaderPClock int 
	slowPathMu sync.Mutex // Serialize slow-path consensus rounds on leader (like Cabinet)
}

func NewConsensusManager(state *smr.ServerState, pmgr *smr.PriorityManager, pstate *smr.PriorityState) *ConsensusManager {
	return &ConsensusManager{
		mystate:  state,
		pmgr:     pmgr,
		pstate:   pstate,
		inFlight: make(map[string]Command),
		leaderPClock: 0,
	}
}

// GetLeaderPClock returns the current leader priority clock (thread-safe)
func (cm *ConsensusManager) GetLeaderPClock() int {
	cm.slowPathMu.Lock()
	defer cm.slowPathMu.Unlock()
	return cm.leaderPClock
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
	ClientID int
	ClientClock int
	CmdType  CmdType   //write or read
	ObjID    string
	ObjType  int // IndependentObject | CommonObject
	Payload  interface{}
}
type ReplyInfo struct {
	Reply   Reply
	ServerID int
	Latency float64
	ClientClock int
}
// Add this type at the top of the file with other type definitions
type priorityResponse struct {
    serverID int
    priority float64
}

func (cm *ConsensusManager) HandleCommand(cmd Command) (bool, string) {
	var ok bool
	var path string
	switch cmd.ObjType {
	case IndependentObject:
		ok, path = cm.handleFastPath(cmd)
	case CommonObject:
		ok, path = cm.handleSlowPath(cmd)
	case HotObject:
		// Hot objects always go to slow path for serialization
		// This ensures conflicting writes from multiple clients are serialized
		log.Infof("[HOT] Routing hot object %s to SLOW path for conflict serialization", cmd.ObjID)
		ok, path = cm.handleSlowPath(cmd)
	default:
		log.Errorf("unknown object type: %v", cmd.ObjType)
		return false, "INVALID"
	}
	// Only log commits for coordinator/leader, not for followers voting
	if ok {
		log.Debugf("[HANDLE] ObjID=%s | Path=%s | Success=%v", cmd.ObjID, path, ok)
	} else {
		log.Warnf("[COMMIT FAIL] ObjID=%s | Path=%s", cmd.ObjID, path)
	}
	return ok, path
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
	start := time.Now()
	obj := cm.mystate.GetObject(cmd.ObjID)
	numReplicas := len(conns.m)

	if obj == nil {
		cm.mystate.AddObject(cmd.ObjID,cmd.ObjType, numReplicas)
		obj = cm.mystate.GetObject(cmd.ObjID)
		if obj == nil { //Add safety check
            log.Errorf("[FAST] Failed to create object %s", cmd.ObjID)
            return false, "FAST"
        }
		log.Infof("[FAST][Init] Created new object %s | R=%.3f | TotalWeight=%.2f | ThresholdFast=%.2f",
			cmd.ObjID, obj.RValue, obj.TotalWeight, obj.ThresholdFast)
	} else {
		log.Infof("[FAST][Reuse] Using existing object %s | R=%.3f | TotalWeight=%.2f | ThresholdFast=%.2f",
			cmd.ObjID, obj.RValue, obj.TotalWeight, obj.ThresholdFast)
	}

	// Check if this command is already in-flight (receiving a proposal from coordinator)
	cm.mu.Lock()
	if last, exists := cm.inFlight[cmd.ObjID]; exists {
		// Same command already being processed - this is a VOTE request, not coordinator
		if cmd.ClientID == last.ClientID && cmd.ClientClock == last.ClientClock {
			cm.mu.Unlock()
			log.Debugf("[FAST][VOTE] Server %d voting YES for ObjID=%s (already in-flight)", 
				cm.mystate.GetMyServerID(), cmd.ObjID)
			// Just return success as a vote - don't re-broadcast or log commit
			return true, "FAST"
		}
		// Different command - this is a conflict
		if cmd.CmdType == WRITE && last.CmdType == WRITE {
			cm.mu.Unlock()
			log.Warnf("[FAST] Conflict detected on independent object | ObjID=%s | Falling back to SLOW", cmd.ObjID)
			return cm.handleSlowPath(cmd)
		}
	}
	// Register as new coordinator for this command
	cm.inFlight[cmd.ObjID] = cmd
	cm.mu.Unlock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	replies := make(map[int]float64)
	
	myServerID := cm.mystate.GetMyServerID()
	replies[myServerID] = obj.GetReplicaWeight(myServerID)

	conns.RLock()
	for _, conn := range conns.m {
		wg.Add(1)
		args := cm.prepareArgs(cmd, cm.pstate.NextClock())
		go executeFastRPC(conn, args, replies, obj, &mu, &wg)
	}
	conns.RUnlock()

	wg.Wait()
	latency := time.Since(start).Seconds() * 1000
    log.Debugf("[FAST] Obj=%s latency=%.2fms", cmd.ObjID, latency)

	mu.Lock()
	totalWeight := 0.0
	for serverID, weight := range replies {
		totalWeight += weight
		log.Debugf("[FAST] Response from server %d with weight %.2f", serverID, weight)
	}
	replyCount := len(replies)
	mu.Unlock()
	log.Infof("[FAST][%s] Reply Summary: %d responses | TotalWeight=%.2f | Required=%.2f | Replies=%v",cmd.ObjID, replyCount, totalWeight, obj.ThresholdFast, replies)

	if obj.HasFastQuorum(replies) {
		cm.mystate.UpdateObjectCommit(cmd.ObjID, cm.mystate.GetMyServerID(), cmd.Payload, "FAST")
		cm.mystate.AddCommitIndex(1)
		obj.Lock()
		obj.LastCommitType = "FAST"
		obj.LastCommitTime = time.Now()
		obj.LastCommittedOpID = cmd.ObjID
		obj.LastProposer = cm.mystate.GetMyServerID()
		obj.Unlock()

		log.Infof("[FAST][COMMIT] ObjID=%s | Quorum satisfied | TotalWeight=%.2f >= %.2f | cmtIndex=%d",
			cmd.ObjID, totalWeight, obj.ThresholdFast, cm.mystate.GetCommitIndex())

		cm.clearCommand(cmd)
		return true, "FAST"
	}
	
    log.Warnf("[FAST][FAIL] Quorum not reached | ObjID=%s | TotalWeight=%.2f < %.2f | Replies=%v",
		cmd.ObjID, totalWeight, obj.ThresholdFast, replies)

	cm.clearCommand(cmd)
	log.Infof("[FAST] Falling back to SLOW path for ObjID=%s", cmd.ObjID)
	ok, path := cm.handleSlowPath(cmd)
	if ok {
		return true, path
	}

	return false, "SLOW"
}

func (cm *ConsensusManager) handleSlowPath(cmd Command) (bool, string) {
    start := time.Now()
    
    // Ensure object exists
    obj := cm.mystate.GetObject(cmd.ObjID)
    if obj == nil {
        conns.RLock()
        numReplicas := len(conns.m) + 1
        conns.RUnlock()
        cm.mystate.AddObject(cmd.ObjID, cmd.ObjType, numReplicas)
        obj = cm.mystate.GetObject(cmd.ObjID)
        log.Infof("[SLOW] Created object %s for slow path", cmd.ObjID)
    }
    
    // Forward to leader if not leader (BEFORE any locking)
    if !cm.mystate.IsLeader() {
        log.Infof("[SLOW] Server %d forwarding to leader %d", 
            cm.mystate.GetMyServerID(), cm.mystate.GetLeaderID())
        return cm.forwardToLeader(cmd)
    }
    
    // ===== LEADER-ONLY CODE BELOW =====
    // CRITICAL: Serialize all slow-path rounds on the leader (like Cabinet's sequential loop)
    // This prevents leaderPClock from racing ahead of priority updates
    cm.slowPathMu.Lock()
    defer cm.slowPathMu.Unlock()
    
    // Check if already in-flight (duplicate request detection)
    cm.mu.Lock()
    if last, exists := cm.inFlight[cmd.ObjID]; exists {
        if cmd.ClientID == last.ClientID && cmd.ClientClock == last.ClientClock {
            cm.mu.Unlock()
            log.Debugf("[SLOW][DUP] Leader received duplicate request for ObjID=%s", cmd.ObjID)
            
            obj.Lock()
            obj.LastCommitType = "SLOW"
            obj.LastCommitTime = time.Now()
            obj.Unlock()
            
            return true, "SLOW"
        }
    }
    cm.mu.Unlock()
    
    log.Debugf("[SLOW][LEADER] Acquired slow-path lock for ObjID=%s | ClientClock=%d", cmd.ObjID, cmd.ClientClock)
    
    // Get current clock (don't increment yet)
    leaderPClock := cm.leaderPClock
    
    // CRITICAL: Ensure priorities exist for this clock before proceeding
    fpriorities := cm.pmgr.GetFollowerPriorities(leaderPClock)
    if len(fpriorities) == 0 {
        // Priorities don't exist - this means initialization issue or we need to create them
        log.Warnf("[SLOW] No priorities for pClock=%d, initializing from previous clock", leaderPClock)
        
        // Try to get priorities from previous clock
        if leaderPClock > 0 {
            prevPriorities := cm.pmgr.GetFollowerPriorities(leaderPClock - 1)
            if len(prevPriorities) > 0 {
                // Copy previous priorities to current clock as fallback
                prioQueue := make(chan int, cm.pmgr.GetNumServers())
                for id := range prevPriorities {
                    if id != cm.mystate.GetMyServerID() {
                        prioQueue <- id
                    }
                }
                if err := cm.pmgr.UpdateFollowerPriorities(leaderPClock, prioQueue, cm.mystate.GetMyServerID()); err != nil {
                    log.Errorf("[SLOW] Failed to initialize priorities for pClock=%d: %v", leaderPClock, err)
                    return false, "SLOW"
                }
                fpriorities = cm.pmgr.GetFollowerPriorities(leaderPClock)
                log.Infof("[SLOW] Initialized priorities for pClock=%d from previous clock", leaderPClock)
            } else {
                log.Errorf("[SLOW] No priorities available for pClock=%d or previous clock!", leaderPClock)
                return false, "SLOW"
            }
        } else {
            log.Errorf("[SLOW] No priorities for pClock=0 - initialization failed!")
            return false, "SLOW"
        }
    }
    
    // Now increment for next round
    cm.leaderPClock++
    
    log.Infof("[SLOW] Assigned pClock=%d to ObjID=%s | ClientClock=%d", leaderPClock, cmd.ObjID, cmd.ClientClock)
    
    log.Infof("[SLOW] pClock: %v | priorities: %+v", leaderPClock, fpriorities)
    log.Infof("[SLOW] pClock: %v | quorum size (t+1): %v | majority: %.2f", 
        leaderPClock, cm.pmgr.GetQuorumSize(), cm.pmgr.GetMajority())
    
    // Initialize with leader's priority
    myServerID := cm.mystate.GetMyServerID()
    leaderPrio, ok := fpriorities[myServerID]
    if !ok {
        log.Errorf("[SLOW] Leader priority missing from fpriorities map!")
        return false, "SLOW"
    }
    
    prioSum := leaderPrio
    prioQueue := make(chan int, cm.pmgr.GetNumServers()) // Track response order
    
    // Register command
    cm.mu.Lock()
    cm.inFlight[cmd.ObjID] = cmd
    cm.mu.Unlock()
    defer cm.clearCommand(cmd)
    
    // Broadcast to followers
    receiver := make(chan ReplyInfo, cm.pmgr.GetNumServers())
    
    conns.RLock()
    activeServers := 1 // Count leader
    for _, conn := range conns.m {
        if conn.serverID != myServerID {
            activeServers++
            args := cm.prepareArgs(cmd, leaderPClock)
            args.PrioVal = fpriorities[conn.serverID] // Send follower's priority
            go executeSlowRPC(conn, "WocService.ConsensusService", args, receiver)
        }
    }
    conns.RUnlock()
    
    // Collect votes (Cabinet-style)
    timeout := time.After(5 * time.Second)
    responseCount := 1 // Leader counts as 1
    
    quorumReached := false
    respondedServers := make(map[int]bool) // Track who responded
    
    for !quorumReached && responseCount < activeServers {
        select {
        case rinfo := <-receiver:
            if rinfo.Reply.ErrorMsg == nil && rinfo.Reply.Accepted {
                responseCount++
                respondedServers[rinfo.ServerID] = true
                
                // Add to priority queue IN ORDER OF ARRIVAL
                prioQueue <- rinfo.ServerID
                
                // Add follower's priority to sum
                followerPrio, ok := fpriorities[rinfo.ServerID]
                if !ok {
                    log.Warnf("[SLOW] Server %d priority not in fpriorities", rinfo.ServerID)
                    continue
                }
                
                prioSum += followerPrio
                
                log.Infof("[SLOW] recv pClock: %v | serverID: %v | prioSum: %.2f / %.2f",
                    leaderPClock, rinfo.ServerID, prioSum, cm.pmgr.GetMajority())
                
                // Cabinet-style quorum check
                if prioSum > cm.pmgr.GetMajority() {
                    quorumReached = true
                    // ⚠️ DON'T BREAK - continue collecting for full priority queue
                }
            }
            
        case <-timeout:
            // Timeout - stop waiting for more responses
            // Identify which servers didn't respond
            nonResponders := []int{}
            conns.RLock()
            for _, conn := range conns.m {
                if conn.serverID != myServerID && !respondedServers[conn.serverID] {
                    nonResponders = append(nonResponders, conn.serverID)
                }
            }
            conns.RUnlock()
            
            log.Warnf("[SLOW] Timeout | ObjID=%s | Responses=%d/%d | PrioSum=%.2f/%.2f | NonResponders=%v",
                cmd.ObjID, responseCount, activeServers, prioSum, cm.pmgr.GetMajority(), nonResponders)
            
            // Exit the loop - we'll handle incomplete queue below
            goto updatePriorities
        }
    }
    
updatePriorities:
    if !quorumReached {
        log.Warnf("[SLOW] Consensus NOT reached | ObjID=%s | PrioSum=%.2f/%.2f | Responses=%d/%d",
            cmd.ObjID, prioSum, cm.pmgr.GetMajority(), responseCount, activeServers)
        return false, "SLOW"
    }
    
    
    // ===== COMMIT =====
    latency := time.Since(start).Milliseconds()
    
    cm.mystate.UpdateObjectCommit(cmd.ObjID, myServerID, cmd.Payload, "SLOW")
    cm.mystate.AddCommitIndex(1)
    
    obj.Lock()
    obj.LastCommitType = "SLOW"
    obj.LastCommitTime = time.Now()
    obj.Unlock()
    
    perfM.RecordSlowCommit()
    
    log.Infof("[SLOW] Consensus reached | ObjID=%s | Latency=%dms | PrioSum=%.2f | cmtIndex=%d",
        cmd.ObjID, latency, prioSum, cm.mystate.GetCommitIndex())
    
    // ===== UPDATE PRIORITIES FOR NEXT ROUND =====
    // This is CRITICAL: must happen immediately so next concurrent request has priorities
    nextClock := leaderPClock + 1

    // Update priorities using response order (Cabinet's mechanism)
    if err := cm.pmgr.UpdateFollowerPriorities(nextClock, prioQueue, myServerID); err != nil {
        log.Errorf("[SLOW] UpdateFollowerPriorities failed for clock %d: %v", nextClock, err)
        // Still return success since we committed, but log error
    } else {
        newPrios := cm.pmgr.GetFollowerPriorities(nextClock)
        log.Infof("[SLOW] ✓ Priorities updated for clock %d: %+v", nextClock, newPrios)
        
        // Update leader's priority state
        if cm.pstate != nil {
            if newLeaderPrio, ok := newPrios[myServerID]; ok {
                cm.pstate.UpdatePriority(nextClock, newLeaderPrio)
            }
        }
    }
    
    return true, "SLOW"
}

func sumVotes(votes map[int]float64) float64 {
	total := 0.0
	for _, w := range votes {
		total += w
	}
	return total
}
func (cm *ConsensusManager) forwardToLeader(cmd Command) (bool, string) {
	conns.RLock()
	leaderConn, ok := conns.m[cm.mystate.GetLeaderID()]
	conns.RUnlock()
	
	if !ok || cm.detectLeaderFailure(cm.mystate.GetLeaderID()) {
		cm.handleLeaderFailure()
		// After election, try forwarding again if we're not the new leader
		if cm.mystate.GetMyServerID() != cm.mystate.GetLeaderID() {
			// Get updated leader connection
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
	// IMPORTANT: Use PrioClock=0 when forwarding to leader
	// Leader will assign the proper PrioClock when broadcasting
	args := cm.prepareArgs(cmd, 0)
	reply := &Reply{}
	if err := leaderConn.txClient.Call("WocService.ConsensusService", args, &reply); err != nil {
		log.Errorf("forward to leader failed: %v", err)
		// Try leader election on RPC failure
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
func (cm *ConsensusManager) hasConflict(cmd Command) bool {
    cm.mu.Lock()
    defer cm.mu.Unlock()

    if last, exists := cm.inFlight[cmd.ObjID]; exists {
        // Conflict only if:
        // 1. Both are WRITEs
        // 2. Commands are from DIFFERENT clients (different ClientID or ClientClock)
        if cmd.CmdType == WRITE && last.CmdType == WRITE {
            if cmd.ClientID != last.ClientID || cmd.ClientClock != last.ClientClock {
                log.Warnf("[CONFLICT] Detected concurrent writes to ObjID=%s | Cmd1: Client=%d Clock=%d | Cmd2: Client=%d Clock=%d",
                    cmd.ObjID, last.ClientID, last.ClientClock, cmd.ClientID, cmd.ClientClock)
                return true
            }
            // Same command (replicated across replicas) - not a conflict
            log.Debugf("[NO CONFLICT] Same command replicated | ObjID=%s | Client=%d Clock=%d", 
                cmd.ObjID, cmd.ClientID, cmd.ClientClock)
            return false
        }
    }

    // Register new inflight command
    cm.inFlight[cmd.ObjID] = cmd
    return false
}

func (cm *ConsensusManager) clearCommand(cmd Command) {
    cm.mu.Lock()
    delete(cm.inFlight, cmd.ObjID)
    cm.mu.Unlock()
}
func executeFastRPC(conn *ServerDock, args *Args, replies map[int]float64, obj *smr.ObjectState, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	const maxRetries = 3
	const timeout = 5 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		reply := Reply{}
		done := make(chan error, 1)

		go func() {
			done <- conn.txClient.Call("WocService.ConsensusService", args, &reply)
		}()

		select {
		case err := <-done:
			if err != nil {
				if err == rpc.ErrShutdown || err == io.EOF {
					conns.Lock()
					delete(conns.m, conn.serverID)
					conns.Unlock()
					return
				}
				log.Warnf("[FAST] RPC failed | server=%d | attempt=%d | err=%v", conn.serverID, attempt, err)
			} else {
				if reply.Accepted {  // Only count weight if accepted
					weight := obj.GetReplicaWeight(conn.serverID)
					mu.Lock()
					replies[conn.serverID] = weight
					mu.Unlock()
					log.Debugf("[FAST] RPC success | server=%d | weight=%.2f | Accepted=true", 
						conn.serverID, weight)
				} else {
					log.Debugf("[FAST] RPC rejected | server=%d", conn.serverID)
				}
				return
			}
		case <-time.After(timeout):
			log.Warnf("[FAST] RPC timeout | server=%d | attempt=%d", conn.serverID, attempt)
		}

		// Exponential backoff
		time.Sleep(time.Millisecond * 50 * time.Duration(attempt))
	}

	conns.Lock()
	delete(conns.m, conn.serverID)
	conns.Unlock()
	log.Errorf("[FAST] Server %d unresponsive → removed from pool", conn.serverID)
}

func executeSlowRPC(conn *ServerDock, service string, args *Args, receiver chan ReplyInfo) {
	reply := Reply{}
	
	stack := make(chan struct{}, 1)

	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] = stack
	conn.jobQMu.Unlock()

	// Wait for previous pClock to complete (Cabinet's pattern)
	if args.PrioClock > 0 {
		conn.jobQMu.RLock()
		prev, ok := conn.jobQ[args.PrioClock-1]
		conn.jobQMu.RUnlock()
		
		if ok {
			select {
			case <-prev:
				// Previous pClock completed successfully
			case <-time.After(3 * time.Second):
				log.Warnf("[SLOW] Timeout waiting for prev pClock=%d | server=%d | proceeding anyway", 
					args.PrioClock-1, conn.serverID)
			}
		}
	}

	// Execute RPC with retries
	start := time.Now()
	log.Debugf("[SLOW-RPC] Calling server=%d | PrioClock=%d | ObjID=%s | ClientClock=%d", 
		conn.serverID, args.PrioClock, args.ObjID, args.ClientClock)
	
	err := conn.txClient.Call(service, args, &reply)
	latency := time.Since(start).Seconds() * 1000 // milliseconds
	
	log.Debugf("[SLOW-RPC] Response from server=%d | latency=%.2fms | err=%v | Accepted=%v", 
		conn.serverID, latency, err, reply.Accepted)
	
	if err != nil {
		log.Errorf("[SLOW] RPC call error | server=%d | ClientClock=%d | PrioClock=%d | err=%v", 
			conn.serverID, args.ClientClock, args.PrioClock, err)
		
		// Signal completion even on error (critical!)
		conn.jobQMu.Lock()
		conn.jobQ[args.PrioClock] <- struct{}{}
		conn.jobQMu.Unlock()
		
		// CRITICAL: Must send error response to receiver so leader doesn't wait forever!
		rinfo := ReplyInfo{
			ServerID:    conn.serverID,
			Reply:       Reply{Accepted: false, ErrorMsg: err},
			Latency:     latency,
			ClientClock: args.ClientClock,
		}
		receiver <- rinfo
		return
	}

	// Check if reply was accepted
	if !reply.Accepted || reply.ErrorMsg != nil {
		log.Warnf("[SLOW] RPC rejected | server=%d | ClientClock=%d | PrioClock=%d | Accepted=%v | err=%v",
			conn.serverID, args.ClientClock, args.PrioClock, reply.Accepted, reply.ErrorMsg)
		
		// Signal completion
		conn.jobQMu.Lock()
		conn.jobQ[args.PrioClock] <- struct{}{}
		conn.jobQMu.Unlock()
		
		// CRITICAL: Send rejection response to receiver
		rinfo := ReplyInfo{
			ServerID:    conn.serverID,
			Reply:       reply,
			Latency:     latency,
			ClientClock: args.ClientClock,
		}
		receiver <- rinfo
		return
	}

	// Success - send to receiver
	rinfo := ReplyInfo{
		ServerID:    conn.serverID,
		Reply:       reply,
		Latency:     latency,
		ClientClock: args.ClientClock,
	}
	receiver <- rinfo

	// Signal completion (Cabinet's pattern: signal AFTER sending response)
	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] <- struct{}{}
	conn.jobQMu.Unlock()

	log.Infof("[SLOW] RPC success | server=%d | ClientClock=%d | PrioClock=%d | latency=%.2fms",
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
		// // evenly distributed
		// for i := 0; i < 5; i++ {
		// 	for j := 1; j <= (quorum-1) / 5; j++ {
		// 		crashList = append(crashList, i*(numOfServers/5) + j)
		// 	}
		// }

		// randomly distributed
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
        return true // Leader connection not available
    }
    
    // Try to ping leader
    args := &PingArgs{Clock: cm.pstate.NextClock()}
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
    newClock := cm.pstate.NextClock()
    
    // Create priority queue for alive servers
    prioQueue := make(chan serverID, cm.pmgr.GetNumServers())
    
    // Add alive servers to queue
    conns.RLock()
    for id := range conns.m {
        if id != oldLeader {
            prioQueue <- id
        }
    }
    conns.RUnlock()
    
    // Update priorities using Cabinet's mechanism
    if err := cm.pmgr.UpdateFollowerPriorities(newClock, prioQueue, -1); err != nil {
        log.Errorf("Failed to update priorities: %v", err)
        return
    }
    
    // Try to start an election
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
    responses := make(chan float64, cm.pmgr.GetNumServers())
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

    for responseCount < cm.pmgr.GetNumServers() {
        select {
        case prio := <-responses:
            responseCount++
            votes[responseCount] = prio

            // Check if we have quorum based on priorities
            if cm.pmgr.HasQuorum(votes, cm.pmgr.GetMajority()) {
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
// HandleVoteRequest handles incoming vote requests from candidates
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
//func (cm *ConsensusManager) cleanupInflight() {
   // cm.mu.Lock()
    //defer cm.mu.Unlock()
    
   // now := time.Now()
   // for objID, cmd := range cm.inFlight {
     //   if now.Sub(cmd.timestamp) > 30*time.Second {  // Add timestamp to Command struct
      //      delete(cm.inFlight, objID)
      //  }
 //   }
//}