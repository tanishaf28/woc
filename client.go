package main

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"woc/config"
	"woc/mongodb"
)

// Helper function to record batch metrics (used in both sequential and pipelined modes)
func recordBatchMetrics(reply *Reply, clockVal int, batchSize int) {
	// Handle MIXED path results: "MIXED(FAST:X,SLOW:Y,HOT:Z)"
	if len(reply.PathUsed) >= 5 && reply.PathUsed[:5] == "MIXED" {
		// Parse mixed path counts from the reply
		// Extract fast, slow, and hot counts from "MIXED(FAST:X,SLOW:Y,HOT:Z)"
		var fastOps, slowOps, hotOps int
		n, _ := fmt.Sscanf(reply.PathUsed, "MIXED(FAST:%d,SLOW:%d,HOT:%d)", &fastOps, &slowOps, &hotOps)
		
		if n < 3 {
			// Try old format without HOT count: "MIXED(FAST:X,SLOW:Y)"
			fmt.Sscanf(reply.PathUsed, "MIXED(FAST:%d,SLOW:%d)", &fastOps, &slowOps)
		}
		
		// Record based on actual counts
		if fastOps > 0 {
			atomic.AddInt64(&perfM.FastCommits, int64(fastOps))
		}
		if slowOps > 0 {
			atomic.AddInt64(&perfM.SlowCommits, int64(slowOps))
		}
		if hotOps > 0 {
			// Hot objects are both SLOW path and CONFLICT
			atomic.AddInt64(&perfM.SlowCommits, int64(hotOps))
			atomic.AddInt64(&perfM.ConflictCommits, int64(hotOps))
		}
		
		// Record individual operation metrics
		for i := 0; i < fastOps && i < batchSize; i++ {
			perfM.IncFastPath(clockVal)
		}
		for i := 0; i < slowOps && i < batchSize; i++ {
			perfM.IncSlowPath(clockVal)
		}
		for i := 0; i < hotOps && i < batchSize; i++ {
			perfM.IncConflict(clockVal)
		}
	} else if len(reply.PathUsed) >= 3 && reply.PathUsed[:3] == "HOT" {
		// Handle HOT-only replies: "HOT:5"
		var hotOps int
		fmt.Sscanf(reply.PathUsed, "HOT:%d", &hotOps)
		
		atomic.AddInt64(&perfM.SlowCommits, int64(hotOps))
		atomic.AddInt64(&perfM.ConflictCommits, int64(hotOps))
		
		for i := 0; i < hotOps && i < batchSize; i++ {
			perfM.IncConflict(clockVal)
		}
	} else {
		// Simple path results: "FAST" or "SLOW"
		switch reply.PathUsed {
		case "FAST":
			atomic.AddInt64(&perfM.FastCommits, int64(batchSize))
			for b := 0; b < batchSize; b++ {
				perfM.IncFastPath(clockVal)
			}
		case "SLOW":
			atomic.AddInt64(&perfM.SlowCommits, int64(batchSize))
			for b := 0; b < batchSize; b++ {
				perfM.IncSlowPath(clockVal)
			}
		default:
			// Unknown path or failure
			atomic.AddInt64(&perfM.ConflictCommits, int64(batchSize))
			for b := 0; b < batchSize; b++ {
				perfM.IncConflict(clockVal)
			}
		}
	}
}

// RunClient sends operations to replicas in round-robin order or batch mode
func RunClient(clientID int, configPath string, numOps int, indepRatio float64,commonRatio float64, batchMode string ){
	// Check if pipelined mode is enabled (allows high throughput)
	pipelined := os.Getenv("PIPELINE_MODE") == "true"
	maxInflight := 10  // Default
	if val := os.Getenv("MAX_INFLIGHT"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &maxInflight); err == nil && n == 1 && maxInflight > 0 {
			// Valid value
		} else {
			maxInflight = 10
		}
	}
	
	var sem chan struct{}
	if pipelined {
		sem = make(chan struct{}, maxInflight)
		log.Infof("Client %d: PIPELINED mode enabled (max %d concurrent batches)", clientID, maxInflight)
	} else {
		log.Infof("Client %d: SEQUENTIAL mode enabled (ordered batches)", clientID)
	}
	
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
	
	fmt.Printf("Client %d started | Total Ops: %s | Batch Mode: %s | Batch Size: %d | Composition: %s\n", 
		clientID, opsLabel, batchMode, batchsize, batchComposition)
	fmt.Printf("  Object Distribution: Hot=%d%% | Independent=%.0f%% | Common=%.0f%% (Total=%.0f%%)\n", 
		conflictrate, indepRatio, commonRatio, totalRatio)
	
	if batchComposition == "mixed" {
		log.Infof("Client %d: MIXED batch mode - each batch contains diverse objects and types", clientID)
	} else {
		log.Infof("Client %d: OBJECT-SPECIFIC batch mode - each batch targets one object", clientID)
	}
	log.Infof("Client %d: Hot objects use numbered IDs 'obj-HOT-0' through 'obj-HOT-9' to distribute conflicts", clientID)

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

			CClock := incrementClock()
			perfM.RecordStarter(CClock)

			// Build batch payload based on batch composition mode
			cmd := &Args{
				ClientID:    clientID,
				ClientClock: CClock,
				CmdType:     WRITE,
				Type:        evalType,
			}

			// Determine batch composition
			if batchComposition == "mixed" {
				// MIXED MODE: Each operation in batch has different object/type
				cmd.IsMixed = true
				cmd.ObjIDs = make([]string, currentBatch)
				cmd.ObjTypes = make([]int, currentBatch)
				
				// Generate different object for each operation in the batch
				for b := 0; b < currentBatch; b++ {
					randVal := rand.Float64() * 100
					if randVal < float64(conflictrate) {
						// Hot object - distributed across 10 hot object IDs (obj-HOT-0 through obj-HOT-9)
						// Multiple clients will compete for these 10 hot objects
						cmd.ObjTypes[b] = HotObject
						hotID := (op + b) % 10  // Distribute across 10 hot objects
						cmd.ObjIDs[b] = fmt.Sprintf("obj-HOT-%d", hotID)
					} else if randVal < float64(conflictrate)+indepRatio {
						// Independent object - unique per operation
						cmd.ObjTypes[b] = IndependentObject
						cmd.ObjIDs[b] = fmt.Sprintf("obj-indep-%d-%d", clientID, op+b)
					} else {
						// Common object
						cmd.ObjTypes[b] = CommonObject
						cmd.ObjIDs[b] = fmt.Sprintf("obj-common-%d-%d", clientID, op+b)
					}
				}
				// Set first object as primary (for backward compatibility)
				cmd.ObjID = cmd.ObjIDs[0]
				cmd.ObjType = cmd.ObjTypes[0]
			} else {
				// OBJECT-SPECIFIC MODE: All ops in batch target same object
				cmd.IsMixed = false
				var objType int
				var objID string
				
				randVal := rand.Float64() * 100
				if randVal < float64(conflictrate) {
					// Hot object - distributed across 10 hot object IDs
					objType = HotObject
					hotID := op % 10  // Distribute across 10 hot objects
					objID = fmt.Sprintf("obj-HOT-%d", hotID)
				} else if randVal < float64(conflictrate)+indepRatio {
					objType = IndependentObject
					objID = fmt.Sprintf("obj-indep-%d-%d", clientID, op)
				} else {
					objType = CommonObject
					objID = fmt.Sprintf("obj-common-%d-%d", clientID, op)
				}
				
				cmd.ObjID = objID
				cmd.ObjType = objType
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

			// Check if pipelined mode is enabled
			if pipelined {
				// PIPELINED MODE: Send batches concurrently without waiting
				// Use semaphore to limit max in-flight batches
				sem <- struct{}{}  // Acquire slot
				
				go func(clockVal int, connection *rpc.Client, command *Args, serverID int, batchSize int) {
					defer func() { <-sem }()  // Release slot
					
					reply := &Reply{}
					start := time.Now()
					err := connection.Call("WocService.ConsensusService", command, reply)
					latency := time.Since(start)
					
					if err != nil {
						log.Warnf("[Client %d] Batch %d failed on server %d: %v", clientID, clockVal, serverID, err)
						atomic.AddInt64(&perfM.ConflictCommits, int64(batchSize))
						for b := 0; b < batchSize; b++ {
							perfM.IncConflict(clockVal)
						}
					} else {
						recordBatchMetrics(reply, clockVal, batchSize)
					}
					
					perfM.RecordFinisher(clockVal)
					
					log.Infof("[Client %d] Batch %d | size=%d | ServerClock=%d | server=%d | %s | latency=%dms",
						clientID, clockVal, batchSize, reply.LeaderClock, serverID, reply.PathUsed, latency.Milliseconds())
				}(cmd.ClientClock, conn, cmd, sid, currentBatch)
				
			} else {
				// SEQUENTIAL MODE: Wait for previous batch to complete (original behavior)
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
					atomic.AddInt64(&perfM.ConflictCommits, int64(currentBatch))
					for b := 0; b < currentBatch; b++ {
						perfM.IncConflict(cmd.ClientClock)
					}
				} else {
					// Record metrics using helper function
					recordBatchMetrics(reply, cmd.ClientClock, currentBatch)
				}

				perfM.RecordFinisher(cmd.ClientClock)

				log.Infof("[Client %d] Batch %d | size=%d | ServerClock=%d | server=%d | %s | latency=%dms",
					clientID, cmd.ClientClock, currentBatch, reply.LeaderClock, sid, reply.PathUsed, latency.Milliseconds())
			}

			// --- 9. Update counters ---
			op += currentBatch
			if batchMode == "single" {
				serverIdx = (serverIdx + 1) % len(serverIDs)
			} else {
				serverIdx++
			}
		}

		// --- 10. Infinite mode check ---
		if numOps <= 0 {
			continue  // Infinite mode
		}
	}

	// Wait for all pipelined batches to complete
	if pipelined {
		for i := 0; i < maxInflight; i++ {
			sem <- struct{}{}  // Drain semaphore
		}
		log.Infof("Client %d: All pipelined batches completed", clientID)
	}

	// --- 10. Save metrics ---
	if err := perfM.SaveToFile(); err != nil {
		log.Errorf("Client %d: failed to save metrics: %v", clientID, err)
	} else {
		log.Infof("Client %d: saved performance metrics", clientID)
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
