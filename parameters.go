package main

import (
	"flag"
)

const (
	Localhost = iota
	Distributed
)

const (
	PlainMsg = iota
	MongoDB
	// RingReconfig marks a command carrying a RingUpdate payload (see
	// objectmap.go) - the object-ownership ring/dead-replica-set change a
	// replica's health monitor proposes, routed through the same slow-path
	// consensus round as any other dependent-object command so every
	// replica adopts it at the same global order position (see
	// conJobRingReconfig in service.go).
	RingReconfig
	// FallBack marks a best-effort, fire-and-forget broadcast the fast-path
	// coordinator sends when it fails to reach quorum before timing out: it
	// tells followers to revert their provisional apply for one (ObjID, Seq)
	// pair if it hasn't since been superseded (see conJobFallBack in
	// service.go, and ObjectState.RevertIfSeqMatches). Unlike RingReconfig,
	// this is NOT routed through slow-path consensus - reverting a replica's
	// own local provisional state doesn't need cluster agreement, and is
	// safe to miss (the next real commit for that object supersedes it via
	// the same seq guard).
	FallBack
	// MongoConfirm marks a best-effort, fire-and-forget broadcast the
	// fast-path coordinator sends AFTER reaching quorum for a MongoDB
	// command: unlike PlainMsg, a follower's fast-path vote for a MongoDB
	// proposal never applies the write to its local MongoDB (see
	// applyExecute's MongoDB case) - a real, external database write can't
	// be cheaply/safely undone the way an in-memory ObjectState.Value can if
	// the round times out (see the FallBack comment above), so followers
	// only physically write once the round is known to have committed.
	// MongoConfirm is that "now write it for real" signal (see
	// conJobMongoConfirm in service.go). Safe to miss: the next real commit
	// for the same key carries the current data forward regardless, and a
	// FastRead against a replica that missed one confirm is stale rather
	// than wrong.
	MongoConfirm
)

const (
	IndependentObject = iota
	DependentObject
)

type CmdType int

const (
	WRITE CmdType = iota
	READ
)

// Read modes: FastRead answers from whichever replica was asked, with no
// coordination (speculative, no safety guarantee). SafeRead first confirms
// a weighted quorum (the same per-object/global thresholds writes use)
// before returning its local value.
const (
	FastRead = iota
	SafeRead
)

var numOps int
var numOfServers int
var threshold int
var quorum int
var myServerID int
var configPath string
var production bool
var logLevel string
var mode int
var evalType int

var batchsize int
var msgsize int

var ratioTryStep float64
var enablePriority bool

// Pipeline mode parameters
var pipelineMode bool // true = pipeline mode, false = sequential
var maxInflight int   // max concurrent batches in pipeline mode

// Mongo DB input parameters
var mongoLoadType string
var mongoClientNum int

// crash test parameters
var crashTime int
var crashMode int
var crashTarget int // replica ID to kill alone when crashMode == 4

// suffix of files
var suffix string

var indepRatio float64  // % of independent objects; drives the object registry split (see objectmap.go)
var numObjects int      // total size of the fixed, hash-ring-mapped object pool (paper §3-§4)
var clientID int
var role int
var batchComposition string // "mixed" or "object-specific"

var readRatio float64   // % of ops that are reads; 0 (default) keeps today's all-write behavior
var readModeFlag string // raw -readmode flag value: "fast" or "safe"
var readMode int        // parsed into FastRead or SafeRead

var numClients int // total client processes in this run; used to spread "ownership" of real (e.g. YCSB) keys across clients for the MongoDB workload (see objectmap.go's keyOwnerClientIdx)
var pinServer int  // required, >=0: always send to this server ID (round-robin dispatch has been removed)

// hotObjThreshold/hotObjID let one specific object use a failure threshold
// (and therefore fast-path quorum size/weight vector) different from every
// other object's -t, within the same run - demonstrating the paper's
// per-object independence claim (§3.2), which a single global -t can't:
// every other consensus protocol here (Cabinet, Raft) has exactly one
// threshold for the whole system. Disabled (hotObjThreshold < 0) by
// default; wired into preWarmAllObjects (main.go).
var hotObjThreshold int
var hotObjID string

// targetObjID, when set, pins every client operation to this exact object
// ID for the whole run (instead of the usual random per-type pick), so a
// client can generate a workload measuring one specific object's own
// throughput/latency - see the -hotobjthreshold eval.
var targetObjID string

func loadCommandLineInputs() {

	flag.IntVar(&numOps, "ops", 1000, "number of operations to send")
	flag.IntVar(&numOfServers, "n", 10, "# of servers")

	flag.IntVar(&threshold, "t", 1, "# of quorum tolerated")
	// f = t + 1;

	flag.IntVar(&batchsize, "b", 1, "batch size")
	flag.IntVar(&myServerID, "id", 0, "this server ID")
	flag.StringVar(&configPath, "path", "./config/cluster_localhost.conf", "config file path")

	// Production mode stores log on disk at ./logs/
	flag.BoolVar(&production, "pd", false, "production mode?")
	flag.StringVar(&logLevel, "log", "debug", "trace, debug, info, warn, error, fatal, panic")
	flag.IntVar(&mode, "mode", 0, "0 -> localhost; 1 -> distributed")
	flag.IntVar(&evalType, "et", 0, "0 -> plain msg; 1 -> Mongodb")

	flag.Float64Var(&ratioTryStep, "rstep", 0.001, "rate for trying qualified ratio")

	flag.BoolVar(&enablePriority, "ep", true, "true -> cabinet; false -> raft")

	// Pipeline mode flags
	flag.BoolVar(&pipelineMode, "pipeline", true, "true = pipeline mode, false = sequential mode")
	flag.IntVar(&maxInflight, "maxinflight", 5, "max concurrent batches in pipeline mode")

	// Plain message input parameters
	flag.IntVar(&msgsize, "ms", 512, "message size")

	// MongoDB input parameters
	flag.StringVar(&mongoLoadType, "mload", "a", "mongodb load type")
	flag.IntVar(&mongoClientNum, "mcli", 16, "# of mongodb clients")

	// Object type parameters
	flag.Float64Var(&indepRatio, "indep", 90, "percentage of independent objects")
	flag.IntVar(&numObjects, "numobjects", 1000, "size of the fixed object pool mapped onto the replica hash ring")

	// crash test parameters
	flag.IntVar(&crashTime, "ct", 20, "# of rounds before crash")
	flag.IntVar(&crashMode, "cm", 0, "0 -> no crash; 1 -> strong machines; 2 -> weak machines; 3 -> random machines; 4 -> single explicit replica (see -crashtarget)")
	flag.IntVar(&crashTarget, "crashtarget", -1, "replica ID to kill alone when -cm=4")

	// suffix of files
	flag.StringVar(&suffix, "suffix", "woc", "suffix of files")
	flag.IntVar(&role, "role", 0, "0 -> server ; 1 -> client ")
	flag.StringVar(&batchComposition, "bcomp", "object-specific", "batch composition: 'mixed' = all object types in one batch | 'object-specific' = separate batches per object type")

	// Read parameters
	flag.Float64Var(&readRatio, "readratio", 0, "percentage of operations that are reads (0 = all writes)")
	flag.StringVar(&readModeFlag, "readmode", "fast", "read mode: 'fast' (any replica, no quorum) or 'safe' (weighted quorum confirmation)")

	flag.IntVar(&numClients, "numclients", 1, "total number of client processes in this run (used to spread ownership of real MongoDB/YCSB keys across clients)")
	// -pinserver only selects the client's initial contact point, not the
	// replica that actually coordinates a write: independent objects always
	// route to their owner on the hash ring (see objectmap.go's
	// ObjectOwner), and dependent objects always route to the leader,
	// regardless of which server the client happened to pin to. Required
	// (no round-robin default, matching EPaxos's client) - use -pinserver to
	// control load distribution across clients, not to control which
	// replica handles a particular object.
	flag.IntVar(&pinServer, "pinserver", -1, "pin client to specific server ID (required; no round-robin default)")

	flag.IntVar(&hotObjThreshold, "hotobjthreshold", -1, "failure threshold (t) for the single object named by -hotobjid, independent of -t for every other object (-1 = disabled, use -t for every object)")
	flag.StringVar(&hotObjID, "hotobjid", "obj-0", "object ID -hotobjthreshold applies to when enabled")
	flag.StringVar(&targetObjID, "targetobjid", "", "if set, every client operation targets exactly this object ID (empty = normal random per-type selection)")

	flag.Parse()
	quorum = threshold + 1

	if readModeFlag == "safe" {
		readMode = SafeRead
	} else {
		readMode = FastRead
	}

	log.Debugf("CommandLine parameters:\n - numOfServers:%v\n - myServerID:%v\n - independentRatio:%v%%",
		numOfServers, myServerID, indepRatio)
}
