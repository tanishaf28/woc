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
	if logLevel == "debug" || logLevel == "trace" {
		logLevel = "info"  // Disable verbose logging in production
	}
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

func preWarmAllObjects() {
	log.Infof("Server %d: Pre-warming objects for cloud deployment...", myServerID)
	startTime := time.Now()
	
	conns.RLock()
	numReplicas := len(conns.m) + 1
	conns.RUnlock()
	maxClientID := numOfServers + 20  // Safe upper bound
	log.Infof("Server %d: Pre-warming objects for client IDs %d-%d", myServerID, 0, maxClientID-1)
	
	for clientID := 0; clientID < maxClientID; clientID++ {
		for i := 0; i < 100000; i++ {
			objID := fmt.Sprintf("obj-indep-%d-%d", clientID, i)
			cm.mystate.AddObject(objID, IndependentObject, numReplicas)
		}
	}

	for i := 0; i < 10000; i++ {
		objID := fmt.Sprintf("obj-common-%d", i)
		cm.mystate.AddObject(objID, CommonObject, numReplicas)
	}
	
	for i := 0; i < 10; i++ {
		objID := fmt.Sprintf("obj-HOT-%d", i)
		cm.mystate.AddObject(objID, HotObject, numReplicas)
	}
	
	duration := time.Since(startTime)
	totalWarmed := (maxClientID * 100000) + 10000 + 10
	log.Infof("Server %d: ✓ Pre-warmed %d objects in %v (Hot=10, Indep=%dM, Common=10K)", 
		myServerID, totalWarmed, duration, maxClientID)
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
	if cm.mystate.IsLeader() {
		go cm.startSlowPathProcessor()
		log.Infof("Server %d: ✓ Slow path processor started (Cabinet-style serialized)", myServerID)
	}

	//  STEP 3: NOW start accepting RPC connections (clients can connect safely!)
	go rpc.Accept(listener)
	log.Infof("Server %d:  NOW ACCEPTING RPC connections at %s", myServerID, myAddr)

	// Shutdown handler: save metrics and wait for active RPCs
	var serverShuttingDown atomic.Bool
	var activeRPCs atomic.Int64  // Track active RPC handlers
	
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
	RunClient(myServerID, configPath, numOps, indepRatio, commonRatio, batchMode)
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
	fmt.Printf("Client Ratios   : indep=%.2f%% | common=%.2f%%\n", indepRatio, commonRatio)
	fmt.Println("===================================================")
}
