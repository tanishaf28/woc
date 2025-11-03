package main

import (
	"errors"
	"time"
	"fmt"
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

	for _, msg := range args.CmdPlain {
		log.Infof("[PlainMsg] ClientClock=%d | msg=%x", args.ClientClock, msg)
	}
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
	reply.LeaderClock = args.PrioClock 
	reply.Success = ok
	reply.Accepted = ok  // Important: mark as accepted for vote counting

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
	log.Debugf("Server %d executing MongoDB ObjID=%s", myServerID, args.ObjID)
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
	reply.Accepted = ok  // Important: mark as accepted for vote counting
	
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
	log.Infof("MongoDB ObjID=%s committed via %s path | ClientClock=%d | duration=%s", args.ObjID, path, args.ClientClock, reply.ExeResult)
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