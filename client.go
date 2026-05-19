package main

import (
	"fmt"
	"math/rand"
	"net"
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

var clientLatencyDebug = os.Getenv("LATENCY_DEBUG") == "true"

var (
	hotObjIDs    [10]string
	indepObjIDs  [100000]string
	commonObjIDs [10000]string
	objIDsOnce   sync.Once
)

func initObjectIDPool(clientID int) {
	objIDsOnce.Do(func() {
		start := time.Now()
		
		for i := 0; i < 10; i++ {
			hotObjIDs[i] = fmt.Sprintf("obj-HOT-%d", i)
		}
		for i := 0; i < 100000; i++ {
			indepObjIDs[i] = fmt.Sprintf("obj-indep-%d-%d", clientID, i)
		}
		for i := 0; i < 10000; i++ {
			commonObjIDs[i] = fmt.Sprintf("obj-common-%d", i)
		}
		
		log.Infof("[Client %d] Pre-generated 110,010 object IDs in %v (latency optimization)", 
			clientID, time.Since(start))
	})
}

type SimpleLimiter struct {
	semaphore chan struct{}
	maxInflight int
	clientID int
}

func NewSimpleLimiter(maxInflight int, clientID int) *SimpleLimiter {
	return &SimpleLimiter{
		semaphore: make(chan struct{}, maxInflight),
		maxInflight: maxInflight,
		clientID: clientID,
	}
}

func (sl *SimpleLimiter) Acquire() {
	sl.semaphore <- struct{}{} // Blocks if full (lock-free!)
}

func (sl *SimpleLimiter) Release() {
	<-sl.semaphore
}

func (sl *SimpleLimiter) GetStats() (int, float64) {
	return sl.maxInflight, 1.0 // Static limiter, assume 100% efficiency
}

func (sl *SimpleLimiter) AdjustLimit(pathUsed string, latencyMs int64) {
	// SimpleLimiter is static - no adjustment needed
}

// NoOpLimiter: For localhost testing - zero overhead
type NoOpLimiter struct{}

func NewNoOpLimiter(maxInflight int, clientID int) *NoOpLimiter {
	log.Infof("[Client %d] NoOpLimiter enabled - ZERO overhead (localhost mode)", clientID)
	return &NoOpLimiter{}
}

func (nl *NoOpLimiter) Acquire()           {}
func (nl *NoOpLimiter) Release()           {}
func (nl *NoOpLimiter) GetStats() (int, float64) { return 999999, 1.0 }
func (nl *NoOpLimiter) AdjustLimit(pathUsed string, latencyMs int64) {}

// ChannelLimiter: Non-blocking limiter for low latency (Cabinet-style)
type ChannelLimiter struct {
	tokens   chan struct{}
	clientID int
	max      int
	acquired atomic.Int64
	released atomic.Int64
}

func NewChannelLimiter(maxInflight int, clientID int) *ChannelLimiter {
	cl := &ChannelLimiter{
		tokens:   make(chan struct{}, maxInflight),
		clientID: clientID,
		max:      maxInflight,
	}
	
	// Pre-fill with tokens
	for i := 0; i < maxInflight; i++ {
		cl.tokens <- struct{}{}
	}
	
	log.Infof("[Client %d] ChannelLimiter enabled (non-blocking, max %d)", clientID, maxInflight)
	return cl
}

func (cl *ChannelLimiter) Acquire() {
	<-cl.tokens  // Non-blocking channel read
	cl.acquired.Add(1)
}

func (cl *ChannelLimiter) Release() {
	cl.released.Add(1)
	cl.tokens <- struct{}{}  // Return token
}

func (cl *ChannelLimiter) GetStats() (int, float64) {
	acq := cl.acquired.Load()
	rel := cl.released.Load()
	active := acq - rel
	utilization := float64(active) / float64(cl.max)
	return cl.max, utilization
}

func (cl *ChannelLimiter) AdjustLimit(pathUsed string, latencyMs int64) {
	// Static limit - no adjustment (Cabinet philosophy: simple = fast)
}

// Limiter interface for polymorphism
type Limiter interface {
	Acquire()
	Release()
	GetStats() (int, float64)
	AdjustLimit(pathUsed string, latencyMs int64)
}

// dialClientRPC creates an optimized RPC connection for clients
func dialClientRPC(address string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	
	// CRITICAL: Enable TCP_NODELAY for low-latency RPCs (disables Nagle's algorithm)
	// Without this, pipelined small RPCs experience 40ms+ delays
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			log.Warnf("Failed to set TCP_NODELAY: %v", err)
		}
		if err := tcpConn.SetKeepAlive(true); err != nil {
			log.Warnf("Failed to set KeepAlive: %v", err)
		}
		if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
			log.Warnf("Failed to set KeepAlivePeriod: %v", err)
		}
	}
	
	return rpc.NewClient(conn), nil
}
type AdaptiveLimiter struct {
	mu             sync.Mutex
	cond           *sync.Cond
	fastPathRatio  float64
	maxInflight    int
	minInflight    int
	currentLimit   int
	currentActive  int
	lastAdjustTime time.Time
	clientID       int
}

func NewAdaptiveLimiter(maxInflight int, clientID int) *AdaptiveLimiter {
	// Unified limiter for both localhost and cloud
	var minInflight, initialLimit int
	var initialFastRatio float64
	
	minInflight = 1
	initialLimit = 3
	initialFastRatio = 0.5
	
	if maxInflight <= 5 {
			initialLimit = 1
			initialFastRatio = 0.1
		} else if maxInflight <= 15 {
			initialLimit = 3
			initialFastRatio = 0.5
		} else {
			initialLimit = 5
			initialFastRatio = 0.95
		}
	
	if maxInflight < minInflight {
		minInflight = maxInflight
	}
	if initialLimit < minInflight {
		initialLimit = minInflight
	}
	if initialLimit > maxInflight {
		initialLimit = maxInflight
	}
	
	al := &AdaptiveLimiter{
		maxInflight:    maxInflight,
		minInflight:    minInflight,
		currentLimit:   initialLimit,
		currentActive:  0,
		fastPathRatio:  initialFastRatio,
		lastAdjustTime: time.Now(),
		clientID:       clientID,
	}
	al.cond = sync.NewCond(&al.mu)
	return al
}

func (al *AdaptiveLimiter) Acquire() {
	al.mu.Lock()
	for al.currentActive >= al.currentLimit {
		al.cond.Wait()  
	}
	al.currentActive++
	al.mu.Unlock()
}

func (al *AdaptiveLimiter) Release() {
	al.mu.Lock()
	if al.currentActive > 0 {
		al.currentActive--
	}
	al.mu.Unlock()
	al.cond.Signal()  // Wake one waiting goroutine
}

func (al *AdaptiveLimiter) AdjustLimit(pathUsed string, latencyMs int64) {
	al.mu.Lock()
	defer al.mu.Unlock()
	
	isFast := pathUsed == "FAST" || 
	          (len(pathUsed) >= 5 && pathUsed[:5] == "MIXED" && pathUsed != "MIXED(FAST:0,SLOW:0,HOT:0)")
	
	// Update fast path ratio with EMA
	if isFast {
		al.fastPathRatio = 0.85*al.fastPathRatio + 0.15*1.0
	} else {
		al.fastPathRatio = 0.85*al.fastPathRatio + 0.15*0.0
	}
	
	if time.Since(al.lastAdjustTime) < 100*time.Millisecond {
		return
	}
	al.lastAdjustTime = time.Now()
	
	oldLimit := al.currentLimit
	
	// Unified adjustment logic for both localhost and cloud
	if al.maxInflight <= 5 {
		if al.fastPathRatio > 0.6 && latencyMs < 200 && al.currentLimit < al.maxInflight {
			al.currentLimit++  // Increment by 1 only
			al.cond.Signal()
		}
		// Aggressive backoff to protect serialized server
		if al.fastPathRatio < 0.4 || latencyMs > 300 {
			if al.currentLimit > al.minInflight {
				al.currentLimit--
			}
		}
		
	} else if al.maxInflight <= 15 {
		if al.fastPathRatio > 0.75 && latencyMs < 100 && al.currentLimit < al.maxInflight {
			increase := 2
			if al.currentLimit + increase > al.maxInflight {
				increase = al.maxInflight - al.currentLimit
			}
			al.currentLimit += increase
			for i := 0; i < increase; i++ {
				al.cond.Signal()
			}
		} else if al.fastPathRatio > 0.60 && latencyMs < 150 && al.currentLimit < al.maxInflight {
			al.currentLimit++
			al.cond.Signal()
		}
		// Normal backoff
		if (al.fastPathRatio < 0.4 || latencyMs > 250) && al.currentLimit > al.minInflight {
			al.currentLimit--
		}
		
	} else {
		if al.fastPathRatio > 0.85 && latencyMs < 50 && al.currentLimit < al.maxInflight {
			increase := 5  // Jump by 5
			if al.currentLimit + increase > al.maxInflight {
				increase = al.maxInflight - al.currentLimit
			}
			al.currentLimit += increase
			for i := 0; i < increase; i++ {
				al.cond.Signal()
			}
		} else if al.fastPathRatio > 0.70 && latencyMs < 100 && al.currentLimit < al.maxInflight {
			increase := 2
			if al.currentLimit + increase > al.maxInflight {
				increase = al.maxInflight - al.currentLimit
			}
			al.currentLimit += increase
			for i := 0; i < increase; i++ {
				al.cond.Signal()
			}
		}
		// Gentle backoff (fast path issues may be temporary)
		if (al.fastPathRatio < 0.3 || latencyMs > 200) && al.currentLimit > al.minInflight {
			decrease := 2
			for i := 0; i < decrease && al.currentLimit > al.minInflight; i++ {
				al.currentLimit--
			}
		}
	}
	
	if oldLimit != al.currentLimit {
		log.Debugf("[Client %d] Limit %d→%d (active=%d, fast=%.0f%%, lat=%dms)", 
			al.clientID, oldLimit, al.currentLimit, al.currentActive, 
			al.fastPathRatio*100, latencyMs)
	}
}

func (al *AdaptiveLimiter) GetStats() (int, float64) {
	al.mu.Lock()
	defer al.mu.Unlock()
	return al.currentLimit, al.fastPathRatio
}


// Helper function to record batch metrics
func recordBatchMetrics(reply *Reply, clockVal int, batchSize int) {
	if len(reply.PathUsed) >= 5 && reply.PathUsed[:5] == "MIXED" {
		var fastOps, slowOps, hotOps int
		n, _ := fmt.Sscanf(reply.PathUsed, "MIXED(FAST:%d,SLOW:%d,HOT:%d)", &fastOps, &slowOps, &hotOps)
		if n < 3 {
			fmt.Sscanf(reply.PathUsed, "MIXED(FAST:%d,SLOW:%d)", &fastOps, &slowOps)
		}
		if fastOps > 0 {
			atomic.AddInt64(&perfM.FastCommits, int64(fastOps))
		}
		if slowOps > 0 {
			atomic.AddInt64(&perfM.SlowCommits, int64(slowOps))
		}
		if hotOps > 0 {
			atomic.AddInt64(&perfM.SlowCommits, int64(hotOps))
			atomic.AddInt64(&perfM.ConflictCommits, int64(hotOps))
		}
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
		var hotOps int
		fmt.Sscanf(reply.PathUsed, "HOT:%d", &hotOps)
		atomic.AddInt64(&perfM.SlowCommits, int64(hotOps))
		atomic.AddInt64(&perfM.ConflictCommits, int64(hotOps))
		for i := 0; i < hotOps && i < batchSize; i++ {
			perfM.IncConflict(clockVal)
		}
	} else {
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
			atomic.AddInt64(&perfM.ConflictCommits, int64(batchSize))
			for b := 0; b < batchSize; b++ {
				perfM.IncConflict(clockVal)
			}
		}
	}
}

func RunClient(clientID int, configPath string, numOps int, indepRatio float64, commonRatio float64, batchMode string) {
	pipelined := os.Getenv("PIPELINE_MODE") == "true"
	maxInflight := 5  // default conservative
	if val := os.Getenv("MAX_INFLIGHT"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &maxInflight); err == nil && n == 1 && maxInflight > 0 {
			// Explicit override from environment
		} else {
			maxInflight = 5
		}
	} else {
		slowPathRatio := (float64(conflictrate) + commonRatio) / 100.0
		
		if slowPathRatio >= 0.80 {
			maxInflight = 3
			log.Infof("Client %d: SLOW workload (%.0f%% slow) → MAX_INFLIGHT=%d (conservative for serialized server)", 
				clientID, slowPathRatio*100, maxInflight)
		} else if slowPathRatio >= 0.30 {
			maxInflight = 8  // Reduced from 12 for cloud stability
			log.Infof("Client %d: MIXED workload (%.0f%% slow) → MAX_INFLIGHT=%d (moderate)", 
				clientID, slowPathRatio*100, maxInflight)
		} else {
			log.Infof("Client %d: FAST workload (%.0f%% slow) → MAX_INFLIGHT=%d (cloud-optimized, tunable via MAX_INFLIGHT env)", 
				clientID, slowPathRatio*100, maxInflight)
		}
	}

	var shuttingDown atomic.Bool
	var inflightOps atomic.Int64
	var metricsSaved atomic.Bool
	
	var limiter Limiter
	
	if pipelined {
		useAdaptive := os.Getenv("USE_ADAPTIVE_LIMITER") == "true"
		useSimple := os.Getenv("USE_SIMPLE_LIMITER") == "true"
		noLimiter := os.Getenv("NO_LIMITER") == "true" // Localhost bypass
		
		if noLimiter {
			limiter = NewNoOpLimiter(maxInflight, clientID)
			log.Infof("Client %d: PIPELINED mode with NO LIMITER (localhost only - zero overhead)", clientID)
		} else if useSimple {
			limiter = NewSimpleLimiter(maxInflight, clientID)
			log.Infof("Client %d: PIPELINED mode with SimpleLimiter (lock-free, max %d concurrent batches)", 
				clientID, maxInflight)
		} else if useAdaptive {
			limiter = NewAdaptiveLimiter(maxInflight, clientID)
			log.Infof("Client %d: PIPELINED mode with AdaptiveLimiter (adaptive, max %d concurrent batches)", 
				clientID, maxInflight)
		} else {
			limiter = NewChannelLimiter(maxInflight, clientID)
			log.Infof("Client %d: PIPELINED mode with ChannelLimiter (non-blocking, max %d concurrent batches)", 
				clientID, maxInflight)
		}
	} else {
		log.Infof("Client %d: SEQUENTIAL mode enabled (ordered batches)", clientID)
	}

	// Parse cluster configuration
	clusterConf := config.ParseClusterConfig(numOfServers, configPath)
	cluster := make(map[int]string)
	for sid, info := range clusterConf {
		cluster[sid] = info[config.ServerIP] + ":" + info[config.ServerRPCListenerPort]
	}
	initObjectIDPool(clientID)
	
	// Connect to servers
	conns := make(map[int]*rpc.Client)
	for sid, addr := range cluster {
		if sid >= numOfServers {
			continue
		}
		// Use optimized TCP connection for low latency
		c, err := dialClientRPC(addr, 5*time.Second)
		if err != nil {
			log.Warnf("Client %d: failed to connect to server %d (%s): %v", clientID, sid, addr, err)
			continue
		}
		conns[sid] = c
		log.Infof("Client %d: connected to server %d at %s [TCP_NODELAY enabled]", clientID, sid, addr)
		
		// LATENCY OPTIMIZATION: Pre-warm connection with ping (eliminates first-request TCP handshake cost)
		pingArgs := &PingArgs{Clock: 0}
		pingReply := &Reply{}
		pingStart := time.Now()
		if err := c.Call("WocService.Ping", pingArgs, pingReply); err != nil {
			log.Warnf("Client %d: Server %d ping failed: %v", clientID, sid, err)
		} else {
			rtt := time.Since(pingStart)
			log.Infof("Client %d: Server %d connection pre-warmed (RTT=%v)", clientID, sid, rtt)
		}
	}
	if len(conns) == 0 {
		log.Fatalf("Client %d: no server connections available", clientID)
	}

	// Shutdown handler - SETUP BEFORE STARTING WORK
	sigChan := make(chan os.Signal, 10)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// Dedicated goroutine for signal handling
	go func() {
		sig := <-sigChan
		log.Debugf("Client %d:  Received signal %v", clientID, sig)
		fmt.Printf("\n  Client %d: Received signal %v - starting graceful shutdown...\n", clientID, sig)
		
		shuttingDown.Store(true)
		
		// Wait for in-flight ops
		deadline := time.Now().Add(30 * time.Second)
		lastReport := time.Now()
		for {
			inflight := inflightOps.Load()
			if inflight == 0 {
				log.Debugf("Client %d:  All in-flight operations completed", clientID)
				fmt.Printf("Client %d:  All in-flight operations completed\n", clientID)
				break
			}
			
			if time.Now().After(deadline) {
				log.Warnf("Client %d:  Timeout waiting for %d in-flight operations", clientID, inflight)
				fmt.Printf("Client %d:  Timeout - %d operations still in flight\n", clientID, inflight)
				break
			}
			
			if time.Since(lastReport) >= 2*time.Second {
				log.Debugf("Client %d: Waiting for %d in-flight operations...", clientID, inflight)
				fmt.Printf("Client %d: Waiting for %d in-flight operations...\n", clientID, inflight)
				lastReport = time.Now()
			}
			time.Sleep(100 * time.Millisecond)
		}
		
		// Save metrics
		log.Infof("Client %d:  Saving metrics...", clientID)
		fmt.Printf("Client %d:  Saving metrics (Fast=%d, Slow=%d, Conflicts=%d)...\n", 
			clientID, perfM.FastCommits, perfM.SlowCommits, perfM.ConflictCommits)
		
		if err := perfM.SaveToFile(); err != nil {
			log.Errorf("Client %d:  Failed to save metrics: %v", clientID, err)
			fmt.Printf("Client %d:  ERROR saving metrics: %v\n", clientID, err)
		} else {
			metricsSaved.Store(true)
			log.Infof("Client %d:  Metrics saved successfully", clientID)
			fmt.Printf("Client %d:  Metrics saved to ./eval/client%d/\n", clientID, clientID)
		}
		
		// Flush
		time.Sleep(2 * time.Second)
		
		// Close connections
		for _, c := range conns {
			c.Close()
		}
		
		log.Infof("Client %d:  Shutdown complete", clientID)
		fmt.Printf("Client %d:  Shutdown complete\n", clientID)
		
		os.Exit(0)
	}()

	// Initialize performance meter
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
	go startMetricsServer(clientID)

	rand.Seed(time.Now().UnixNano() + int64(clientID))
	serverIDs := make([]int, 0, len(conns))
	for sid := range conns {
		serverIDs = append(serverIDs, sid)
	}

	if conflictrate > 0 {
		log.Infof("Client %d: Pre-creating %d hot objects for conflict experiments...", clientID, 10)
		
		type ObjectCreateArgs struct {
			ObjID   string
			ObjType int
		}
		type ObjectCreateReply struct {
			Success bool
		}
		
		var wg sync.WaitGroup
		for hotID := 0; hotID < 10; hotID++ {
			objID := fmt.Sprintf("obj-HOT-%d", hotID)
			
			// Send to all servers in parallel
			for _, conn := range conns {
				wg.Add(1)
				go func(c *rpc.Client, id string) {
					defer wg.Done()
					args := &ObjectCreateArgs{ObjID: id, ObjType: HotObject}
					reply := &ObjectCreateReply{}
					if err := c.Call("WocService.CreateObject", args, reply); err != nil {
						log.Debugf("Pre-create %s failed (will be created lazily): %v", id, err)
					}
				}(conn, objID)
			}
		}
		wg.Wait()
		log.Infof("Client %d: Hot object pre-creation complete", clientID)
	}

	opsLabel := fmt.Sprintf("%d", numOps)
	if numOps <= 0 {
		opsLabel = "infinite"
	}

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
		log.Debugf("Client %d: MIXED batch mode", clientID)
	} else if batchComposition == "single_obj" {
		log.Debugf("Client %d: SINGLE-OBJ batch mode", clientID)
	} else {
		log.Debugf("Client %d: OBJECT-SPECIFIC batch mode", clientID)
	}

	// Preload MongoDB queries if needed
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

	// Job queue for sequential mode
	jobQ := make(map[int]chan struct{})

	serverIdx := 0
	op := 0

	// Main operation loop
	infinite := numOps <= 0
	for infinite || op < numOps {
		if shuttingDown.Load() {
			log.Debugf("Client %d: Shutdown requested, stopping after %d operations", clientID, op)
			break
		}
		
		// Prevent busy-waiting when limiter is full (CPU optimization)
		if pipelined {
			limit, _ := limiter.GetStats()
			if inflightOps.Load() >= int64(limit) {
				time.Sleep(50 * time.Microsecond)  // Tiny yield to scheduler
				continue
			}
		}

		currentBatch := batchsize
		if !infinite && op+batchsize > numOps {
			currentBatch = numOps - op
		}

		CClock := incrementClock()
		// NOTE: RecordStarter moved AFTER limiter.Acquire (see pipelined section)

		cmd := &Args{
			ClientID:    clientID,
			ClientClock: CClock,
			CmdType:     WRITE,
			Type:        evalType,
		}

		// Batch composition logic
		if batchComposition == "mixed" {
			cmd.IsMixed = true
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				randVal := rand.Float64() * 100
				if randVal < float64(conflictrate) {
					cmd.ObjTypes[b] = HotObject
					cmd.ObjIDs[b] = hotObjIDs[(op+b)%10]  
				} else if randVal < float64(conflictrate)+indepRatio {
					cmd.ObjTypes[b] = IndependentObject
					cmd.ObjIDs[b] = indepObjIDs[(op+b)%100000]  
				} else {
					cmd.ObjTypes[b] = CommonObject
					// Note: Common objects are per-client in pre-gen pool
					cmd.ObjIDs[b] = commonObjIDs[(op+b)%10000]  
				}
			}
			cmd.ObjID = cmd.ObjIDs[0]
			cmd.ObjType = cmd.ObjTypes[0]
		} else if batchComposition == "single_obj" {
			cmd.IsMixed = false
			randVal := rand.Float64() * 100
			var objType int
			var objID string
			if randVal < float64(conflictrate) {
				objType = HotObject
				objID = hotObjIDs[op%10]  
			} else if randVal < float64(conflictrate)+indepRatio {
				objType = IndependentObject
				objID = indepObjIDs[op%100000]  
			} else {
				objType = CommonObject
				objID = commonObjIDs[(op/10)%10000]  
			}
			cmd.ObjType = objType
			cmd.ObjID = objID
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				cmd.ObjTypes[b] = objType
				cmd.ObjIDs[b] = objID
			}
		} else {
			cmd.IsMixed = false
			randVal := rand.Float64() * 100
			var objType int
			if randVal < float64(conflictrate) {
				objType = HotObject
			} else if randVal < float64(conflictrate)+indepRatio {
				objType = IndependentObject
			} else {
				objType = CommonObject
			}
			cmd.ObjType = objType
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				cmd.ObjTypes[b] = objType
				switch objType {
				case HotObject:
					cmd.ObjIDs[b] = hotObjIDs[(op+b)%10]  
				case IndependentObject:
					cmd.ObjIDs[b] = indepObjIDs[(op+b)%100000]  
				case CommonObject:
					cmd.ObjIDs[b] = commonObjIDs[((op+b)/10)%10000]  
				}
			}
			cmd.ObjID = cmd.ObjIDs[0]
		}

		switch evalType {
		case PlainMsg:
			batch := make([][]byte, currentBatch)
			for b := 0; b < currentBatch; b++ {
				batch[b] = genRandomBytes(msgsize)
			}
			cmd.CmdPlain = batch
		case MongoDB:
			batch := make([]mongodb.Query, currentBatch)
			for b := 0; b < currentBatch; b++ {
				batch[b] = mongoDBQueries[(op+b)%len(mongoDBQueries)]
			}
			cmd.CmdMongo = batch
		}

		// SELECT TARGET SERVER - ROUND-ROBIN TO ANY SERVER
		// Server will decide: fast path (coordinate) or slow path (forward to leader)
		targetSID := serverIDs[serverIdx%len(serverIDs)]
		targetConn := conns[targetSID]
		serverIdx++

		if pipelined {
			// Track limiter wait time SEPARATELY (not included in perfM timing)
			limiterWaitStart := time.Now()
			limiter.Acquire()
			limiterWaitMs := time.Since(limiterWaitStart).Milliseconds()
			
			// Log if limiter causes significant blocking (>1ms = bottleneck)
			if limiterWaitMs > 1 {
				log.Warnf("[Client %d] Limiter BLOCKED for %dms (bottleneck detected!)", clientID, limiterWaitMs)
			}
			
			go func(clockVal int, connection *rpc.Client, command *Args, serverID int, batchSize int) {
				inflightOps.Add(1)
				defer func() {
					inflightOps.Add(-1)
					limiter.Release()
					// Garbage collection: clear command payloads
					command.CmdPlain = nil
					command.CmdMongo = nil
					command.ObjIDs = nil
					command.ObjTypes = nil
				}()

				if shuttingDown.Load() {
					return
				}

			reply := &Reply{}
			perfM.RecordStarter(clockVal)
			err := connection.Call("WocService.ConsensusService", command, reply)
			perfM.RecordFinisher(clockVal)
			
			// Phase 2: Metrics recording
			if err != nil {
				atomic.AddInt64(&perfM.ConflictCommits, int64(batchSize))
				for b := 0; b < batchSize; b++ {
					perfM.IncConflict(clockVal)
				}
				if clientLatencyDebug {
					log.Debugf("[CLIENT-LATENCY] Batch %d | ERROR: %v", clockVal, err)
				}
				RecordBatch(batchSize, 0, "ERROR", true)
			} else {
				recordBatchMetrics(reply, clockVal, batchSize)
				RecordBatch(batchSize, reply.Latency, reply.PathUsed, false)
				
				// Adaptive limiter adjustment (based on RPC result)
				if reply.Latency > 0 {
					limiter.AdjustLimit(reply.PathUsed, int64(reply.Latency))
				}
				
				if clientLatencyDebug {
					log.Debugf("[CLIENT-LATENCY] Batch %d | Path=%s | ServerReported=%.2fms",
						clockVal, reply.PathUsed, reply.Latency)
				}
			}

			if clockVal%100 == 0 {
				limit, fastRatio := limiter.GetStats()
				log.Infof("[Client %d] Batch %d | limit=%d | fast=%.0f%% | size=%d",
					clientID, clockVal, limit, fastRatio*100, batchSize)
			}
			}(cmd.ClientClock, targetConn, cmd, targetSID, currentBatch)

		} else {
			// Sequential mode latency tracking
			batchStart := time.Now()
			
			if clientLatencyDebug {
				log.Debugf("[CLIENT-LATENCY] Sequential batch %d starting | ObjType=%d | BatchSize=%d | Target=Server%d",
					cmd.ClientClock, cmd.ObjType, currentBatch, targetSID)
			}
			
			stack := make(chan struct{}, 1)
			jobQ[cmd.ClientClock] = stack
			
			// Wait for previous batch (sequential ordering)
			orderingStart := time.Now()
			if prev, ok := jobQ[cmd.ClientClock-1]; ok && cmd.ClientClock > 1 {
				<-prev
				delete(jobQ, cmd.ClientClock-1)
			}
			orderingLatency := time.Since(orderingStart)

			//  START TIMER IMMEDIATELY BEFORE RPC (excludes all queue/ordering time)
			reply := &Reply{}
			perfM.RecordStarter(cmd.ClientClock)
			rpcStart := time.Now()
			err := targetConn.Call("WocService.ConsensusService", cmd, reply)
			rpcLatency := time.Since(rpcStart)
			perfM.RecordFinisher(cmd.ClientClock) // Stop timer immediately
			
			stack <- struct{}{} // Signal completion to next batch (not measured)
			
			// Garbage collection: clear command payloads
			defer func() {
				cmd.CmdPlain = nil
				cmd.CmdMongo = nil
				cmd.ObjIDs = nil
				cmd.ObjTypes = nil
			}()

			metricsStart := time.Now()
			if err != nil {
				atomic.AddInt64(&perfM.ConflictCommits, int64(currentBatch))
				for b := 0; b < currentBatch; b++ {
					perfM.IncConflict(cmd.ClientClock)
				}
				RecordBatch(currentBatch, 0, "ERROR", true)
			} else {
				recordBatchMetrics(reply, cmd.ClientClock, currentBatch)
				RecordBatch(currentBatch, reply.Latency, reply.PathUsed, false)
			}
			metricsLatency := time.Since(metricsStart)
			
			totalLatency := time.Since(batchStart)
			
			if clientLatencyDebug {
				log.Debugf("[CLIENT-LATENCY-BREAKDOWN] Sequential batch %d | Path=%s | Ordering=%vμs | RPC=%vms | Metrics=%vμs | Total=%vms | ServerReported=%.2fms",
					cmd.ClientClock, reply.PathUsed, orderingLatency.Microseconds(), rpcLatency.Milliseconds(), 
					metricsLatency.Microseconds(), totalLatency.Milliseconds(), reply.Latency)
			}
			
			log.Infof("[Client %d] Batch %d | size=%d | server=%d | %s | lat=%dms",
				clientID, cmd.ClientClock, currentBatch, targetSID, reply.PathUsed, totalLatency.Milliseconds())
		}

		op += currentBatch
	}

	// Cleanup for finite mode
	if !infinite {
		if pipelined {
			deadline := time.Now().Add(30 * time.Second)
			for inflightOps.Load() > 0 {
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		
		if err := perfM.SaveToFile(); err != nil {
			log.Errorf("Client %d: failed to save metrics: %v", clientID, err)
		} else {
			log.Infof("Client %d: saved performance metrics", clientID)
		}
	} else {
		// Infinite mode: wait for signal handler
		log.Infof("Client %d: Main loop exited, waiting for signal handler...", clientID)
		for !metricsSaved.Load() {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

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