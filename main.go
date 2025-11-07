package main

import (
	"fmt"
	"net"
	"net/rpc"
	"time"

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

	// Load configuration and initialize logger
	loadCommandLineInputs()
	SetLogger(logLevel, myServerID, production)

	// Initialize server state
	mystate := smr.NewServerState()
	mystate.SetMyServerID(myServerID)
	mystate.SetLeaderID(0) // static leader ID for slow path

	// Initialize priority manager
	pmgr := smr.NewPriorityManager(myServerID, numOfServers)
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

	// Initialize consensus manager
	cm = NewConsensusManager(mystate, pmgr, mypriority)

	// Optional crash simulation
	crashList := prepCrashList()
	if len(crashList) > 0 {
		log.Infof("Crash list generated: %v", crashList)
	}

	// Initialize performance metrics
	fileName := fmt.Sprintf("s%d_n%d_f%d_b%d_%s",
		myServerID, numOfServers, quorum, batchsize, suffix)
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

// ------------------ SERVER ROLE ------------------
func runServerRole() {
	// Both leader & followers just run RPC listener
	wocService := &WocService{}
	if err := rpc.Register(wocService); err != nil {
		log.Fatalf("Server %d: rpc.Register failed: %v", myServerID, err)
	}

	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		log.Fatalf("Server %d: ListenTCP failed: %v", myServerID, err)
	}
	log.Infof("Server %d: listening at %s", myServerID, myAddr)

	// Accept RPC connections
	go rpc.Accept(listener)

	// Initialize connections to other servers (for slow path forwarding)
	establishServerConnections()

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
	fmt.Printf("Client Ratios   : indep=%d%% | common=%d%%\n", indepRatio, commonRatio)
	fmt.Println("===================================================")
}

