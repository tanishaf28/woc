package main

import (
	"encoding/gob"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
	"woc/config"
	"woc/mongodb"
)

var conns = struct {
	sync.RWMutex
	m map[int]*ServerDock
}{
	m: make(map[int]*ServerDock),
}

// gob requires every concrete type ever assigned to an interface{} field
// (Reply.ReadResult) to be registered before it crosses the RPC wire,
// regardless of role/evalType, so this runs unconditionally at startup.
func init() {
	gob.Register([][]byte{})
	gob.Register([]map[string]string{})
	// handleReadBatch (consensus.go) boxes a multi-key read's N per-key
	// values into []interface{} for Reply.ReadResult - a distinct concrete
	// type from either of the above, needing its own registration.
	gob.Register([]interface{}{})
}

type ServerDock struct {
	serverID int
	addr     string
	txClient *rpc.Client
	jobQMu   sync.RWMutex
	jobQ     map[prioClock]chan struct{}
}

// startLeaderRPCServer starts RPC server for the leader to accept client connections
func startLeaderRPCServer() {
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	ipIndex := config.ServerIP
	rpcPortIndex := config.ServerRPCListenerPort

	myAddr := serverConfig[myServerID][ipIndex] + ":" + serverConfig[myServerID][rpcPortIndex]
	log.Infof("Leader starting RPC server on %s for client connections", myAddr)

	if err := rpc.Register(NewWocService()); err != nil {
		log.Fatalf("rpc.Register failed | error: %v", err)
		return
	}

	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		log.Fatalf("Leader RPC Listen error: %v", err)
		return
	}

	log.Infof("Leader RPC server listening on %s", myAddr)
	rpc.Accept(listener)
}

func runFollower() {
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	ipIndex := config.ServerIP
	rpcPortIndex := config.ServerRPCListenerPort

	myAddr := serverConfig[myServerID][ipIndex] + ":" + serverConfig[myServerID][rpcPortIndex]
	log.Debugf("config: serverID %d | addr: %s", myServerID, myAddr)

	switch evalType {
	case PlainMsg:
		// nothing needs to be done
	case MongoDB:
		go mongoDBCleanUp()
		initMongoDB()
	}

	if err := rpc.Register(NewWocService()); err != nil {
		log.Fatalf("rpc.Register failed | error: %v", err)
		return
	}

	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		log.Fatalf("ListenTCP error: %v", err)
		return
	}

	log.Infof("Follower %d: RPC server listening on %s", myServerID, myAddr)
	rpc.Accept(listener)
}

// establishRPCs creates RPC connections from this server to all other servers
func establishRPCs() {
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	id := config.ServerID
	ip := config.ServerIP
	portOfRPCListener := config.ServerRPCListenerPort

	for i := 0; i < numOfServers; i++ {
		if i == myServerID {
			continue
		}

		serverID, err := strconv.Atoi(serverConfig[i][id])
		if err != nil {
			log.Errorf("%v", err)
			return
		}

		newServer := &ServerDock{
			serverID: serverID,
			addr:     serverConfig[i][ip] + ":" + serverConfig[i][portOfRPCListener],
			txClient: nil,
			jobQMu:   sync.RWMutex{},
			jobQ:     map[prioClock]chan struct{}{},
		}

		log.Infof("Connecting to server %d at %v", i, newServer.addr)

		// Retry connection with backoff
		var txClient *rpc.Client
		for retry := 0; retry < 10; retry++ {
			txClient, err = rpc.Dial("tcp", newServer.addr)
			if err == nil {
				break
			}
			if retry < 9 {
				log.Debugf("Connection to %v failed (attempt %d/10), retrying...", newServer.addr, retry+1)
				time.Sleep(time.Second)
			}
		}
		if err != nil {
			log.Errorf("txClient rpc.Dial failed to %v after 10 attempts | error: %v", newServer.addr, err)
			continue
		}

		newServer.txClient = txClient
		log.Infof("txClient connected to server %d at %v", i, newServer.addr)

		conns.Lock()
		conns.m[serverID] = newServer
		conns.Unlock()
	}

	log.Infof("Successfully established connections to %d servers", len(conns.m))
}

func initMongoDB() {
	gob.Register([]mongodb.Query{})

	if mode == Localhost {
		mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), myServerID)
	} else {
		mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), 0)
	}

	if mongoDbFollower == nil {
		log.Errorf("mongodb follower initialization failed")
		return
	}

	// Each replica has its own independent, standalone MongoDB instance (see
	// start_mongodb_hetero.sh / eval_1_indep_ratio.sh / eval_2_batching.sh -
	// no replica set), so every server, not just server 0, must wait for its
	// own local instance to be ready and load the initial dataset itself:
	// there's no MongoDB-level replication to propagate one server's load to
	// the others. (This used to skip bootstrap on servers != 0, on the
	// assumption of a shared MongoDB replica set doing that propagation -
	// that assumption no longer holds now that each instance is independent.)
	if mode == Distributed {
		if err := mongoDbFollower.WaitForWritablePrimary(60 * time.Second); err != nil {
			log.Errorf("mongodb bootstrap waiting for local instance to be ready failed | err: %v", err)
			return
		}
	}

	queriesToLoad, err := mongodb.ReadQueryFromFile(mongodb.DataPath + "workload.dat")
	if err != nil {
		log.Errorf("getting load data failed | error: %v", err)
		return
	}

	err = mongoDbFollower.ClearTable("usertable")
	if err != nil {
		log.Errorf("clean up table failed | err: %v", err)
		return
	}

	log.Debugf("loading data to Mongo DB")
	_, _, err = mongoDbFollower.FollowerAPI(queriesToLoad)
	if err != nil {
		log.Errorf("load data failed | error: %v", err)
		return
	}

	log.Infof("mongo DB initialization done on server %d", myServerID)
}

// mongoDBCleanUp cleans up client connections to DB upon ctrl+C
func mongoDBCleanUp() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Debugf("clean up MongoDb follower")
		err := mongoDbFollower.CleanUp()
		if err != nil {
			log.Errorf("clean up MongoDB follower failed | err: %v", err)
			return
		}
		log.Infof("clean up MongoDB follower succeeded")
		os.Exit(1)
	}()
}

func cleanupConnections() {
	conns.Lock()
	defer conns.Unlock()

	for id, server := range conns.m {
		if server.txClient != nil {
			server.txClient.Close()
		}
		delete(conns.m, id)
	}
}
