package main

import (
	"woc/config"
	"woc/mongodb"
	"encoding/gob"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var conns = struct {
	sync.RWMutex
	m map[int]*ServerDock
}{
	m: make(map[int]*ServerDock),
}

type ServerDock struct {
	serverID   int
	addr       string
	txClient   *rpc.Client //for transaction msgs
	jobQMu     sync.RWMutex
	jobQ       map[prioClock]chan struct{}
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

	err := rpc.Register(NewWocService())
	if err != nil {
		log.Fatalf("rp.Reister failed | error: %v", err)
		return
	}

	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		log.Fatalf("ListenTCP error: %v", err)
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
			return
		}

		go rpc.ServeConn(conn)
	}
}

// establishServerConnections creates RPC connections from this server to ALL other servers
// Used by both leader and followers for:
// - Fast path: peer-to-peer consensus (replica ↔ replica)
// - Slow path: leader coordination (leader ↔ followers)
func establishServerConnections() {
    serverConfig := config.ParseClusterConfig(numOfServers, configPath)
    ipIndex := config.ServerIP
    rpcPortIndex := config.ServerRPCListenerPort

    const maxRetries = 10
    const retryDelay = 1 * time.Second

    for id := 0; id < numOfServers; id++ {
        if id == myServerID {
            continue // Skip self
        }

        addr := serverConfig[id][ipIndex] + ":" + serverConfig[id][rpcPortIndex]
        
        var client *rpc.Client
        var err error
        
        // Retry logic for robust connection establishment
        for attempt := 1; attempt <= maxRetries; attempt++ {
            client, err = rpc.Dial("tcp", addr)
            if err == nil {
                log.Infof("Server %d → Server %d: connected on attempt %d | addr=%s", 
                    myServerID, id, attempt, addr)
                break
            }
            
            if attempt < maxRetries {
                log.Warnf("Server %d → Server %d: connection attempt %d/%d failed: %v | Retrying in %v...", 
                    myServerID, id, attempt, maxRetries, err, retryDelay)
                time.Sleep(retryDelay)
            } else {
                log.Errorf("Server %d → Server %d: failed after %d attempts: %v | Skipping", 
                    myServerID, id, maxRetries, err)
            }
        }
        
        if err != nil {
            log.Warnf("Server %d: could not connect to peer %d - continuing without it", myServerID, id)
            continue // Skip this server, continue with others
        }

        // Store connection in global map
        conns.Lock()
        conns.m[id] = &ServerDock{
            serverID: id,
            addr:     addr,
            txClient: client,
            jobQ:     make(map[prioClock]chan struct{}),
        }
        conns.Unlock()

        log.Infof("Server %d: established connection to peer %d at %s", myServerID, id, addr)
    }
    
    conns.RLock()
    connectedServers := len(conns.m)
    conns.RUnlock()
    
    log.Infof("Server %d: connection establishment complete | connected to %d/%d peers", 
        myServerID, connectedServers, numOfServers-1)
}


func initMongoDB() {
	gob.Register([]mongodb.Query{})

	if mode == Localhost {
		mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), myServerID)
	} else {
		mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), 0)
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

	log.Infof("mongo DB initialization done")
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