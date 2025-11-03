package main

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"woc/config"
	"woc/mongodb"
)

// RunClient sends operations to replicas in round-robin order or batch mode
func RunClient(clientID int, configPath string, numOps int, indepRatio float64,commonRatio float64, batchMode string ){
	// --- 1. Parse cluster configuration ---
	clusterConf := config.ParseClusterConfig(numOfServers, configPath)
	cluster := make(map[int]string)
	for sid, info := range clusterConf {
		cluster[sid] = info[config.ServerIP] + ":" + info[config.ServerRPCListenerPort]
	}

	// --- 2. Connect to servers ---
	conns := make(map[int]*rpc.Client)
    for sid, addr := range cluster {
        if sid >= numOfServers {  // Only check if it's a server ID
            continue
        }
        c, err := rpc.Dial("tcp", addr)
		if err != nil {
			log.Warnf("Client %d: failed to connect to server %d (%s): %v", clientID, sid, addr, err)
			continue
		}
		conns[sid] = c
		log.Infof("Client %d: connected to server %d at %s", clientID, sid, addr)
	}
	if len(conns) == 0 {
		log.Fatalf("Client %d: no server connections available", clientID)
	}

	// --- 3. Graceful shutdown ---
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// --- 4. Initialize logical clock and performance meter ---
	var clientClock int
	var clockLock sync.Mutex
	incrementClock := func() int {
		clockLock.Lock()
		defer clockLock.Unlock()
		clientClock++
		return clientClock
	}

	fileSuffix := fmt.Sprintf("client%d_eval", clientID)
	perfM.Init(1, batchsize, fileSuffix)

	rand.Seed(time.Now().UnixNano())
	serverIDs := make([]int, 0, len(conns))
	for sid := range conns {
		serverIDs = append(serverIDs, sid)
	}

	opsLabel := fmt.Sprintf("%d", numOps)
	if numOps <= 0 {
		opsLabel = "infinite"
	}
	
	// Validate that ratios add up to 100%
	totalRatio := float64(conflictrate) + indepRatio + commonRatio
	if totalRatio != 100.0 {
		log.Warnf("Client %d: Object type ratios don't add to 100%% (Hot=%d%% + Indep=%.1f%% + Common=%.1f%% = %.1f%%)",
			clientID, conflictrate, indepRatio, commonRatio, totalRatio)
	}
	
	fmt.Printf("Client %d started | Total Ops: %s | Batch Mode: %s | Batch Size: %d\n", clientID, opsLabel, batchMode, batchsize)
	fmt.Printf("  Object Distribution: Hot=%d%% | Independent=%.0f%% | Common=%.0f%% (Total=%.0f%%)\n", 
		conflictrate, indepRatio, commonRatio, totalRatio)
	log.Infof("Client %d: Hot objects use shared ID 'obj-HOT-shared' to trigger conflicts", clientID)

	// --- 5. Preload MongoDB queries if needed ---
	var mongoDBQueries []mongodb.Query
	if evalType == MongoDB {
		filePath := fmt.Sprintf("%srun_workload%s.dat", mongodb.DataPath, mongoLoadType)
		var err error
		mongoDBQueries, err = mongodb.ReadQueryFromFile(filePath)
		if err != nil {
			log.Errorf("ReadQueryFromFile failed | err: %v", err)
			return
		}
	}

	// --- 6. Job queue for controlled concurrency per server ---
	jobQ := make(map[int]map[int]chan struct{}) // serverID -> clientClock -> chan
	for sid := range conns {
		jobQ[sid] = make(map[int]chan struct{})
	}

	serverIdx := 0
	op := 0

	// --- 7. Main operation loop ---
	// Run indefinitely if numOps <= 0, otherwise run for numOps operations
	infinite := numOps <= 0
	for infinite || op < numOps {
		select {
		case <-stop:
			fmt.Printf("Client %d interrupted. Exiting.\n", clientID)
			for _, c := range conns {
				c.Close()
			}
			// Save metrics before exiting
			perfM.SaveToFile()
			fmt.Printf("Client %d: metrics saved to ./eval/client%d_eval.csv\n", clientID, clientID)
			return
		default:
			// Decide batch size
			currentBatch := batchsize
			if !infinite && op+batchsize > numOps {
				currentBatch = numOps - op
			}

			// Prepare ONE batched command with multiple ops
			// Decide object type and ID for this batch
			// Distribution: conflictrate% hot, indepRatio% independent, rest is common
			var objType int
			var objID string
			
			randVal := rand.Float64() * 100
			if randVal < float64(conflictrate) {
				// Hot object - shared across all clients for conflict testing
				objType = HotObject
				objID = "obj-HOT-shared" // Same ID for all clients
			} else if randVal < float64(conflictrate)+indepRatio {
				objType = IndependentObject
				objID = fmt.Sprintf("obj-indep-%d-%d", clientID, op)
			} else {
				objType = CommonObject
				objID = fmt.Sprintf("obj-common-%d-%d", clientID, op)
			}

			CClock := incrementClock()
			perfM.RecordStarter(CClock)

			// Build batch payload (multiple operations in ONE RPC)
			cmd := &Args{
				ClientID:    clientID,
				ClientClock: CClock,
				ObjID:       objID,
				CmdType:     WRITE,
				ObjType:     objType,
				Type:        evalType,
			}

			switch evalType {
			case PlainMsg:
				// Batch multiple messages into one command
				batch := make([][]byte, currentBatch)
				for b := 0; b < currentBatch; b++ {
					batch[b] = genRandomBytes(msgsize)
				}
				cmd.CmdPlain = batch
			case MongoDB:
				// Batch multiple queries into one command
				batch := make([]mongodb.Query, currentBatch)
				for b := 0; b < currentBatch; b++ {
					batch[b] = mongoDBQueries[(op+b)%len(mongoDBQueries)]
				}
				cmd.CmdMongo = batch
			}

			// --- 8. Send ONE RPC for the entire batch ---
			// Decide server based on batch mode
			var sid int
			if batchMode == "single" {
				sid = serverIDs[serverIdx]
			} else {
				sid = serverIDs[serverIdx%len(serverIDs)]
			}
			conn := conns[sid]

			// Controlled concurrency per server
			stack := make(chan struct{}, 1)
			jobQ[sid][cmd.ClientClock] = stack
			if prev, ok := jobQ[sid][cmd.ClientClock-1]; ok && cmd.ClientClock > 1 {
				<-prev
				// Cleanup old entry to prevent memory leak in infinite mode
				delete(jobQ[sid], cmd.ClientClock-1)
			}

			// Send the batch as a single RPC call
			reply := &Reply{}
			start := time.Now()
			err := conn.Call("WocService.ConsensusService", cmd, reply)
			latency := time.Since(start)
			stack <- struct{}{}

			if err != nil {
				log.Warnf("[Client %d] Batch %d failed on server %d: %v", clientID, cmd.ClientClock, sid, err)
				// Record conflict for each op in batch
				for b := 0; b < currentBatch; b++ {
					perfM.IncConflict(cmd.ClientClock)
				}
			} else {
				// Record metrics for the batch
				switch reply.PathUsed {
				case "FAST":
					perfM.RecordFastCommit()
					for b := 0; b < currentBatch; b++ {
						perfM.IncFastPath(cmd.ClientClock)
					}
				case "SLOW":
					perfM.RecordSlowCommit()
					for b := 0; b < currentBatch; b++ {
						perfM.IncSlowPath(cmd.ClientClock)
					}
				default:
					for b := 0; b < currentBatch; b++ {
						perfM.IncConflict(cmd.ClientClock)
					}
				}

				perfM.RecordFinisher(cmd.ClientClock)
				log.Infof("[Client %d] Batch %d | size=%d | ServerClock=%d committed on server %d | obj=%s | type=%v | path=%s | latency=%dms",
					clientID, cmd.ClientClock, currentBatch, reply.LeaderClock, sid, cmd.ObjID, cmd.ObjType, reply.PathUsed, latency.Milliseconds())
			}

			op += currentBatch
			if batchMode == "single" {
				serverIdx = (serverIdx + 1) % len(serverIDs)
			} else {
				serverIdx++
			}
		}
	}

	// --- 9. Close connections and save metrics ---
	for _, c := range conns {
		c.Close()
	}
	perfM.SaveToFile()
	if numOps <= 0 {
		fmt.Printf("Client %d stopped after %d ops (infinite mode).\n", clientID, op)
	} else {
		fmt.Printf("Client %d finished all %d ops.\n", clientID, numOps)
	}
}


// Helper to print eval type
func evalTypeName(et int) string {
	switch et {
	case PlainMsg:
		return "Plain Message"
	case MongoDB:
		return "MongoDB"
	default:
		return "Unknown"
	}
}
