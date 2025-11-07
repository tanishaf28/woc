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
)

const (
	IndependentObject = iota
	CommonObject
	HotObject // Shared object for conflict testing - forces slow path serialization
)

type CmdType int
const (
    WRITE CmdType = iota
    READ
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

// Mongo DB input parameters
var mongoLoadType string
var mongoClientNum int

// crash test parameters
var crashTime int
var crashMode int

// suffix of files
var suffix string

var indepRatio float64 // % of independent objects
var commonRatio float64      // % of common objects
var clientID int
var role int
var conflictrate int
var batchComposition string // "mixed" or "object-specific" 

func loadCommandLineInputs() {
	flag.IntVar(&numOps, "ops", 1000, "number of operations to send")
	flag.IntVar(&numOfServers, "n", 10, "# of servers")

	flag.IntVar(&threshold, "t", 1, "# of quorum tolerated")
	// f = t + 1;
	quorum = threshold + 1

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

	// Plain message input parameters
	flag.IntVar(&msgsize, "ms", 512, "message size")

	// MongoDB input parameters
	flag.StringVar(&mongoLoadType, "mload", "a", "mongodb load type")
	flag.IntVar(&mongoClientNum, "mcli", 16, "# of mongodb clients")

	// Object type parameters
	flag.Float64Var(&indepRatio, "indep", 90, "percentage of independent objects")
	flag.Float64Var(&commonRatio, "common", 10, "percentage of common objects")

	// crash test parameters
	flag.IntVar(&crashTime, "ct", 20, "# of rounds before crash")
	flag.IntVar(&crashMode, "cm", 0, "0 -> no crash; 1 -> strong machines; 2 -> weak machines; 3 -> random machines")

	// suffix of files
	flag.StringVar(&suffix, "suffix", "xxx", "suffix of files")
	flag.IntVar(&role,"role",0,  "0 -> server ; 1 -> client " )
	flag.IntVar(&conflictrate,"conflictrate",20,"Conflict Rate ")
	flag.StringVar(&batchComposition, "bcomp", "mixed", "batch composition: 'mixed' = all object types in one batch | 'object-specific' = separate batches for indep/common/hot")
	flag.Parse()

	log.Debugf("CommandLine parameters:\n - numOfServers:%v\n - myServerID:%v\n - independentRatio:%v%% - commonRatio:%v%%",
		numOfServers, myServerID, indepRatio, commonRatio)
}
