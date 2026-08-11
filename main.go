package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"time"
	"sync/atomic"
	"github.com/sirupsen/logrus"
	"woc/config"
	"woc/eval"
	"woc/mongodb"
	"woc/smr"
)

// ------------------ GLOBAL VARIABLES ------------------
var (
	log             = logrus.New()
	mypriority      *smr.PriorityState
	perfM           eval.PerfMeter
	mongoDbFollower *mongodb.MongoFollower
	cm              *ConsensusManager
	myAddr          string
	pscheme         []float64
	serverConfigs   [][]string
	activeRPCs      atomic.Int64 
)

// Type aliases
type (
	serverID  = int
	prioClock = int
	priority  = float64
)

// ------------------ MAIN FUNCTION ------------------
func main() {
	fmt.Println("Program starts ...")

	loadCommandLineInputs()
	SetLogger(logLevel, myServerID, production)

	// Initialize server state
	mystate := smr.NewServerState()
	mystate.SetMyServerID(myServerID)
	mystate.SetLeaderID(0) // static leader ID for slow path

	// Initialize priority manager
	pmgr := &smr.PriorityManager{}
	pmgr.Init(numOfServers, quorum, 1, ratioTryStep, enablePriority)

	// Get initial priorities for clock 0
	initialPriorities := pmgr.GetFollowerPriorities(0)
	myInitialPrio := initialPriorities[myServerID]

	mypriority = &smr.PriorityState{
		PrioClock: 0,
		PrioVal:   myInitialPrio,
		Majority:  pmgr.GetMajority(),
	}
	pscheme = pmgr.GetPriorityScheme()

	log.Infof("Server %d initialized with priority %.2f at clock 0", myServerID, myInitialPrio)
	cm = NewConsensusManager(mystate, pmgr, mypriority)
	log.Infof("Priority system initialized (clock 0 ready) - priorities will evolve naturally")

	// Optional crash simulation
	crashList := prepCrashList()
	if len(crashList) > 0 {
		log.Infof("Crash list generated: %v", crashList)
	}
	scheduleCrash(crashList)

	// Initialize performance metrics
	fileName := fmt.Sprintf("s%d_n%d_f%d_b%d_%s", myServerID, numOfServers, quorum, batchsize, suffix)
	perfM.Init(1, batchsize, fileName)

	// Parse server configs
	serverConfigs = config.ParseClusterConfig(numOfServers, configPath)
	if len(serverConfigs) <= myServerID {
		log.Fatalf("Invalid server ID %d: config has only %d servers", myServerID, len(serverConfigs))
	}
	myConfig := serverConfigs[myServerID]
	myAddr = myConfig[config.ServerIP] + ":" + myConfig[config.ServerRPCListenerPort]

	printStartupInfo()

	// ------------------ ROLE HANDLING ------------------
	switch role {
	case 0: // SERVER
		runServerRole()
	case 1: // CLIENT
		runClientRole("single") // batchMode: "single" or "roundrobin"
	default:
		log.Fatalf("Invalid role specified: %d. Must be 0 (server) or 1 (client)", role)
	}
}

// scheduleCrash triggers os.Exit after crashTime commits, simulating a
// server crash mid-experiment. Mirrors epaxos's scheduleCrash; previously
// crashList was computed (prepCrashList) but nothing ever acted on it, so
// -cm/-ct were silently no-ops.
func scheduleCrash(crashList []int) {
	if crashMode == 0 || crashTime <= 0 {
		return
	}

	shouldCrash := false
	for _, id := range crashList {
		if id == myServerID {
			shouldCrash = true
			break
		}
	}
	if !shouldCrash {
		return
	}

	log.Warnf("[CRASH] Server %d scheduled to crash after %d commits", myServerID, crashTime)

	go func() {
		for {
			commitIndex := cm.mystate.GetCommitIndex()
			if commitIndex >= crashTime {
				log.Warnf("[CRASH] Server %d crashing now (commitIndex=%d >= crashTime=%d)",
					myServerID, commitIndex, crashTime)
				perfM.SaveToFile()
				os.Exit(1)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

func preWarmAllObjects() {
	log.Infof("Server %d: Pre-warming objects for cloud deployment...", myServerID)
	startTime := time.Now()

	conns.RLock()
	numReplicas := len(conns.m) + 1
	conns.RUnlock()

	InitObjectRegistry()
	for i := 0; i < numObjects; i++ {
		meta := ObjectByIndex(i)
		objQuorum := quorum
		if hotObjThreshold >= 0 && meta.ID == hotObjID {
			// -hotobjthreshold overrides just this one object's quorum size
			// (and therefore its weight vector/threshold - see
			// ComputeFastThreshold), independent of every other object's
			// -t. Demonstrates the paper's per-object independence claim
			// (§3.2): CORA can tune one object's fault-tolerance/latency
			// trade-off without touching any other object's.
			objQuorum = hotObjThreshold + 1
			log.Infof("Server %d: object %s using hot-object threshold t=%d (quorum=%d), independent of -t=%d for every other object",
				myServerID, meta.ID, hotObjThreshold, objQuorum, threshold)
		}
		cm.mystate.AddObject(meta.ID, meta.Type, numReplicas, objQuorum, ratioTryStep)
	}

	if cm.mystate.IsLeader() {
		AssignOwnership(BuildOwnershipRing(numReplicas, nil))
		log.Infof("Server %d (leader): computed object-ownership ring for %d objects", myServerID, numObjects)
	} else {
		owners, dead := fetchOwnershipFromLeader()
		AssignOwnership(owners)
		SetDeadReplicas(dead)
		if len(dead) > 0 {
			cm.mystate.RecomputeFastThresholds(quorum, toDeadSet(dead))
		}
		log.Infof("Server %d: received object-ownership ring from leader", myServerID)
	}

	// Keep ring/dead-set current across the run: the leader's health
	// monitor only acts while leader, the poll loop only acts while
	// follower, so both can run unconditionally on every replica and
	// self-activate across leader failovers.
	go cm.startReplicaHealthMonitor()
	go pollOwnershipFromLeader()

	duration := time.Since(startTime)
	log.Infof("Server %d: ✓ Pre-warmed %d objects (%d independent, %d dependent) in %v",
		myServerID, numObjects, len(independentIdx), len(dependentIdx), duration)
}

// fetchOwnershipFromLeader pulls the object-ownership mapping from the
// global leader (paper §4.2: the leader builds the hash ring and
// disseminates it). Retries with backoff since the leader may not have
// reached its own RPC-accept stage yet during cluster startup.
func fetchOwnershipFromLeader() (owners []int, dead []int) {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		conns.RLock()
		leaderConn, ok := conns.m[cm.mystate.GetLeaderID()]
		conns.RUnlock()
		if ok {
			reply := &ObjectOwnershipReply{}
			if err := leaderConn.txClient.Call("WocService.GetObjectOwnership", &ObjectOwnershipArgs{}, reply); err == nil {
				return reply.OwnerByIndex, reply.DeadReplicas
			} else {
				lastErr = err
			}
		} else {
			lastErr = fmt.Errorf("no connection to leader %d yet", cm.mystate.GetLeaderID())
		}
		time.Sleep(1 * time.Second)
	}
	log.Fatalf("Server %d: failed to fetch object ownership from leader after retries: %v", myServerID, lastErr)
	return nil, nil
}

// pollOwnershipFromLeader runs on every replica but only acts while this
// replica is a follower, periodically re-fetching the leader's
// object-ownership ring and dead-replica set. Ring changes are now
// disseminated primarily by committing a RingUpdate through slow-path
// consensus (see ConsensusManager.startReplicaHealthMonitor and
// conJobRingReconfig in service.go), so this loop is now a 5s-cycle
// backstop for a replica that missed that consensus round (e.g. it was
// mid-restart), not the primary dissemination path.
//
// It also doubles as this replica's only *independent* leader-liveness
// check. detectLeaderFailure (consensus.go) is otherwise invoked purely
// reactively, from forwardToLeader/forwardToLeaderOptimized, only when a
// dependent-object request happens to need the leader - under a workload
// with no dependent-object traffic (e.g. I2D=100/0) that check never
// fires, so a dead leader would otherwise go undetected indefinitely.
// Reusing this loop's existing GetObjectOwnership call as a heartbeat
// closes that gap without adding a second RPC or timer.
func pollOwnershipFromLeader() {
	const (
		pollInterval          = 5 * time.Second
		missedLeaderThreshold = 2 // consecutive misses (~10s) before calling the leader dead
	)
	missedLeaderPings := 0
	for {
		time.Sleep(pollInterval)
		if cm.mystate.IsLeader() {
			missedLeaderPings = 0
			continue
		}
		conns.RLock()
		leaderConn, ok := conns.m[cm.mystate.GetLeaderID()]
		conns.RUnlock()
		if !ok {
			continue
		}
		reply := &ObjectOwnershipReply{}
		if err := leaderConn.txClient.Call("WocService.GetObjectOwnership", &ObjectOwnershipArgs{}, reply); err != nil {
			missedLeaderPings++
			log.Warnf("Server %d: leader ownership poll failed (%d/%d consecutive misses): %v",
				myServerID, missedLeaderPings, missedLeaderThreshold, err)
			if missedLeaderPings >= missedLeaderThreshold {
				log.Warnf("Server %d: leader %d unresponsive for %d consecutive ownership polls - starting election",
					myServerID, cm.mystate.GetLeaderID(), missedLeaderPings)
				missedLeaderPings = 0
				cm.handleLeaderFailure()
			}
			continue
		}
		missedLeaderPings = 0
		AssignOwnership(reply.OwnerByIndex)
		SetDeadReplicas(reply.DeadReplicas)
		// Mirrors the leader's own startReplicaHealthMonitor: a follower
		// also coordinates fast path for any object it owns on the ring, so
		// it needs ThresholdFast recomputed against the current dead set
		// too, not just the leader's copy - see ComputeFastThresholdExcluding.
		cm.mystate.RecomputeFastThresholds(quorum, toDeadSet(reply.DeadReplicas))
	}
}

// toDeadSet converts a dead-replica ID slice (as carried over RPC) into the
// map[int]bool form ComputeFastThresholdExcluding/RecomputeFastThresholds need.
func toDeadSet(dead []int) map[int]bool {
	m := make(map[int]bool, len(dead))
	for _, id := range dead {
		m[id] = true
	}
	return m
}

func runServerRole() {
	wocService := &WocService{}
	if err := rpc.Register(wocService); err != nil {
		log.Fatalf("Server %d: rpc.Register failed: %v", myServerID, err)
	}

	if evalType == MongoDB {
		go mongoDBCleanUp()
		initMongoDB()
		log.Infof("Server %d: ✓ MongoDB initialized", myServerID)
	}

	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		log.Fatalf("Server %d: ListenTCP failed: %v", myServerID, err)
	}
	log.Infof("Server %d: listener created at %s (NOT accepting yet)", myServerID, myAddr)

	// STEP 1: Initialize connections to other servers (for slow path forwarding)
	log.Infof("Server %d: Establishing server connections...", myServerID)
	// Establish peer-to-peer RPC connections
	establishRPCs()
	log.Infof("Server %d: ✓ Server connections established", myServerID)

	//  STEP 2: Pre-warm ALL objects BEFORE accepting requests (saves ~400μs per request)
	log.Infof("Server %d: Pre-warming objects...", myServerID)
	preWarmAllObjects()
	log.Infof("Server %d: ✓ Objects pre-warmed and ready", myServerID)

	//  STEP 2.5: START SLOW PATH PROCESSOR (Cabinet-style serialized processing)
	// Also (re-)started from StartElection's success path, since leadership
	// can move to a different replica later - see ensureSlowPathProcessorStarted.
	if cm.mystate.IsLeader() {
		cm.ensureSlowPathProcessorStarted()
		log.Infof("Server %d: ✓ Slow path processor started (Cabinet-style serialized)", myServerID)
	}

	//  STEP 3: NOW start accepting RPC connections (clients can connect safely!)
	go rpc.Accept(listener)
	log.Infof("Server %d:  NOW ACCEPTING RPC connections at %s", myServerID, myAddr)

	// Shutdown handler: save metrics and wait for active RPCs.
	// activeRPCs is the package-level counter incremented/decremented around
	// WocService.ConsensusService (service.go) — using a fresh local var here
	// instead would always read 0, since nothing increments it.
	var serverShuttingDown atomic.Bool

	sigc := make(chan os.Signal, 10)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigc
		log.Infof("Server %d: Received signal %v - starting graceful shutdown...", myServerID, sig)
		fmt.Printf("\n  Server %d: Received signal %v - starting graceful shutdown...\n", myServerID, sig)
		
		serverShuttingDown.Store(true)
		
		// Stop accepting new connections
		listener.Close()
		log.Infof("Server %d: Stopped accepting new connections", myServerID)
		
		// Wait for active RPCs to complete (up to 10 seconds)
		deadline := time.Now().Add(10 * time.Second)
		for {
			active := activeRPCs.Load()
			if active == 0 {
				log.Infof("Server %d: ✓ All active RPCs completed", myServerID)
				fmt.Printf("Server %d: ✓ All active RPCs completed\n", myServerID)
				break
			}
			
			if time.Now().After(deadline) {
				log.Warnf("Server %d:  Timeout - %d RPCs still active", myServerID, active)
				fmt.Printf("Server %d:  Timeout - %d RPCs still active\n", myServerID, active)
				break
			}
			
			time.Sleep(100 * time.Millisecond)
		}
		
		// Save metrics
		log.Infof("Server %d:  Saving metrics...", myServerID)
		fmt.Printf("Server %d:  Saving metrics...\n", myServerID)
		
		if err := perfM.SaveToFile(); err != nil {
			log.Errorf("Server %d: Failed to save perf metrics: %v", myServerID, err)
		} else {
			log.Infof("Server %d: Performance metrics saved", myServerID)
		}
		
		if err := cm.SaveServerMetrics(); err != nil {
			log.Errorf("Server %d: Failed to save server metrics: %v", myServerID, err)
		} else {
			log.Infof("Server %d: Server metrics saved", myServerID)
		}
		
		// File flush
		time.Sleep(2 * time.Second)
		
		fmt.Printf("Server %d:  Shutdown complete\n", myServerID)
		os.Exit(0)
	}()

	// Keep server running
	select {}
}
// ------------------ CLIENT ROLE ------------------
func runClientRole(batchMode string) {
	log.Infof("Client %d: waiting for cluster to stabilize...", myServerID)
	time.Sleep(3 * time.Second)

	// Generate and send operations
	RunClient(myServerID, configPath, numOps, indepRatio, batchMode)
	fmt.Printf("Client %d finished execution.\n", myServerID)
}

// ------------------ UTILITY FUNCTIONS ------------------
func printStartupInfo() {
	fmt.Println("===================================================")
	fmt.Println("DualPath Consensus: Object-weighted Fast Path & Node-weighted Slow Path")
	fmt.Printf("Priority scheme slow path : %v\n", pscheme)
	fmt.Printf("Majority        : %.4f\n", mypriority.Majority)
	fmt.Println("---------------------------------------------------")
	fmt.Printf("Configuration   : servers=%d | t=%d | ops=%d | id=%d | role=%d\n",
		numOfServers, quorum, numOps, myServerID, role)
	fmt.Printf("EvalType        : %d (0=plain msg,1=mongodb)\n", evalType)
	fmt.Printf("Config Path     : %s\n", configPath)
	fmt.Printf("Client Ratios   : indep=%.2f%%\n", indepRatio)
	fmt.Println("===================================================")
}
