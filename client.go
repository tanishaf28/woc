package main

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"woc/config"
	"woc/mongodb"
)

var clientLatencyDebug = os.Getenv("LATENCY_DEBUG") == "true"

var (
	tsOpsTotal       atomic.Int64
	tsLatSumMs       atomic.Int64
	tsLatCount       atomic.Int64
	tsServerLatSumMs atomic.Int64
)

var objIDsOnce sync.Once

func initObjectIDPool(clientID int) {
	objIDsOnce.Do(func() {
		start := time.Now()
		InitObjectRegistry()
		log.Infof("[Client %d] Built shared %d-object registry in %v (%d independent, %d dependent)",
			clientID, numObjects, time.Since(start), len(independentIdx), len(dependentIdx))
	})
}

type SimpleLimiter struct {
	semaphore   chan struct{}
	maxInflight int
	clientID    int
}

func NewSimpleLimiter(maxInflight int, clientID int) *SimpleLimiter {
	return &SimpleLimiter{
		semaphore:   make(chan struct{}, maxInflight),
		maxInflight: maxInflight,
		clientID:    clientID,
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

func (nl *NoOpLimiter) Acquire()                                     {}
func (nl *NoOpLimiter) Release()                                     {}
func (nl *NoOpLimiter) GetStats() (int, float64)                     { return 999999, 1.0 }
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
	<-cl.tokens // Non-blocking channel read
	cl.acquired.Add(1)
}

func (cl *ChannelLimiter) Release() {
	cl.released.Add(1)
	cl.tokens <- struct{}{} // Return token
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

// isConnectionError distinguishes a transport-level failure (server
// unreachable/down) from a normal application-level rejection. Go's
// net/rpc returns a non-nil error from Call in both cases, but a
// server-side handler returning a non-nil error (e.g. "fast path
// consensus failed" under contention — a perfectly healthy server) comes
// back wrapped as rpc.ServerError; anything else (broken pipe, connection
// reset, rpc.ErrShutdown, timeout) means the connection itself is bad.
// Only the latter should evict a server from the client's rotation.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(rpc.ServerError); ok {
		return false
	}
	return true
}

// ServerPool tracks which servers this client currently considers
// reachable and round-robins among them, evicting a server on a
// connection-level failure and re-admitting it once it responds again.
// Without this, a client that connected to a server which later dies mid-run
// keeps routing requests into it forever with no fallback to a live one.
type ServerPool struct {
	mu       sync.RWMutex
	conns    map[int]*rpc.Client // every server this client ever connected to
	healthy  []int
	idx      atomic.Uint64
	clientID int
}

func NewServerPool(conns map[int]*rpc.Client, clientID int) *ServerPool {
	p := &ServerPool{conns: conns, clientID: clientID}
	for sid := range conns {
		p.healthy = append(p.healthy, sid)
	}
	sort.Ints(p.healthy)
	return p
}

// Conn returns the (possibly stale) connection for a specific server,
// regardless of whether it's currently marked healthy.
func (p *ServerPool) Conn(sid int) (*rpc.Client, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, ok := p.conns[sid]
	return c, ok
}

// Pick round-robins among currently-healthy servers — used only as the
// failover target when a pinned server is down, never as a top-level
// dispatch mode.
func (p *ServerPool) Pick() (int, *rpc.Client, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.healthy) == 0 {
		return 0, nil, false
	}
	i := p.idx.Add(1)
	sid := p.healthy[i%uint64(len(p.healthy))]
	return sid, p.conns[sid], true
}

// PickPinned returns the connection to serverID if it is currently healthy,
// falling back to round-robin if that server is unreachable.
func (p *ServerPool) PickPinned(serverID int) (int, *rpc.Client, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, id := range p.healthy {
		if id == serverID {
			return serverID, p.conns[serverID], true
		}
	}
	// Pinned server not healthy — fall back to round-robin
	if len(p.healthy) == 0 {
		return 0, nil, false
	}
	i := p.idx.Add(1)
	sid := p.healthy[i%uint64(len(p.healthy))]
	return sid, p.conns[sid], true
}

func (p *ServerPool) MarkDown(sid int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, id := range p.healthy {
		if id == sid {
			p.healthy = append(p.healthy[:i:i], p.healthy[i+1:]...)
			log.Warnf("[Client %d] Server %d unreachable, removed from rotation (%d healthy remain)",
				p.clientID, sid, len(p.healthy))
			return
		}
	}
}

func (p *ServerPool) MarkUp(sid int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range p.healthy {
		if id == sid {
			return
		}
	}
	if _, ok := p.conns[sid]; ok {
		p.healthy = append(p.healthy, sid)
		log.Infof("[Client %d] Server %d reachable again, re-added to rotation", p.clientID, sid)
	}
}

// startReviver periodically pings every server not currently in rotation
// and re-admits any that respond, so a recovered server rejoins instead of
// staying evicted for the rest of the run.
func (p *ServerPool) startReviver(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			p.mu.RLock()
			down := make([]int, 0)
			for sid := range p.conns {
				found := false
				for _, h := range p.healthy {
					if h == sid {
						found = true
						break
					}
				}
				if !found {
					down = append(down, sid)
				}
			}
			p.mu.RUnlock()

			for _, sid := range down {
				conn := p.conns[sid]
				reply := &Reply{}
				if err := conn.Call("WocService.Ping", &PingArgs{}, reply); err == nil {
					p.MarkUp(sid)
				}
			}
		}
	}()
}

// callWithFailover issues the RPC against startSID, and on a
// connection-level failure (isConnectionError), evicts that server and
// retries on another currently-healthy one — bounded to one attempt per
// known server so a fully-dead cluster still returns instead of looping
// forever. Returns the server actually used, for logging/metrics.
func callWithFailover(pool *ServerPool, startSID int, cmd *Args, reply *Reply) (usedSID int, err error) {
	sid := startSID
	conn, ok := pool.Conn(sid)
	if !ok {
		sid, conn, ok = pool.Pick()
		if !ok {
			return startSID, fmt.Errorf("no servers available")
		}
	}

	tried := make(map[int]bool)
	for {
		tried[sid] = true
		*reply = Reply{}
		err = conn.Call("WocService.ConsensusService", cmd, reply)
		if !isConnectionError(err) {
			return sid, err
		}

		pool.MarkDown(sid)
		nextSID, nextConn, ok := pool.Pick()
		if !ok || tried[nextSID] {
			return sid, err
		}
		sid, conn = nextSID, nextConn
	}
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
	// baselineLatencyMs is a slow-moving EMA of this client's own observed
	// round latency, used to scale AdjustLimit's backoff/growth thresholds
	// to what's normal for THIS run instead of a hardcoded constant. A flat
	// constant can't distinguish "the protocol is struggling" from "this
	// run has injected network delay baked into every round's latency,
	// healthy or not" - under a netem delay eval, a fixed 300ms trigger
	// collapses concurrency down to minInflight on nearly every adjustment
	// simply because injected delay alone pushes normal round latency past
	// 300ms, independent of how well the consensus protocol is actually
	// coping with it.
	baselineLatencyMs float64
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
	al.cond.Signal() // Wake one waiting goroutine
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

	// Track this run's own typical round latency, slowly (EMA), so the
	// grow/backoff thresholds below can react to "worse than typical for
	// this run" instead of a fixed constant that can't tell a genuinely
	// struggling protocol apart from a run where every round simply costs
	// more because of injected network delay. Seeded from the first sample
	// rather than 0 so an early burst of delayed rounds doesn't get
	// compared against an artificially low baseline before it's had a
	// chance to adapt.
	if al.baselineLatencyMs <= 0 {
		al.baselineLatencyMs = float64(latencyMs)
	} else {
		al.baselineLatencyMs = 0.95*al.baselineLatencyMs + 0.05*float64(latencyMs)
	}

	if time.Since(al.lastAdjustTime) < 100*time.Millisecond {
		return
	}
	al.lastAdjustTime = time.Now()

	oldLimit := al.currentLimit

	// relThreshold scales an absolute floor by the run's own baseline
	// latency: in a low/no-delay run baseline*mult stays below floor, so
	// this returns floor unchanged (existing tuning for normal runs is
	// untouched); in a sustained-high-delay run it relaxes past floor so
	// delay alone doesn't get misread as protocol distress.
	relThreshold := func(floorMs float64, mult float64) float64 {
		scaled := al.baselineLatencyMs * mult
		if scaled > floorMs {
			return scaled
		}
		return floorMs
	}
	lat := float64(latencyMs)

	// Unified adjustment logic for both localhost and cloud
	if al.maxInflight <= 5 {
		if al.fastPathRatio > 0.6 && lat < relThreshold(200, 1.5) && al.currentLimit < al.maxInflight {
			al.currentLimit++ // Increment by 1 only
			al.cond.Signal()
		}
		// Aggressive backoff to protect serialized server
		if al.fastPathRatio < 0.4 || lat > relThreshold(300, 2.5) {
			if al.currentLimit > al.minInflight {
				al.currentLimit--
			}
		}

	} else if al.maxInflight <= 15 {
		if al.fastPathRatio > 0.75 && lat < relThreshold(100, 1.5) && al.currentLimit < al.maxInflight {
			increase := 2
			if al.currentLimit+increase > al.maxInflight {
				increase = al.maxInflight - al.currentLimit
			}
			al.currentLimit += increase
			for i := 0; i < increase; i++ {
				al.cond.Signal()
			}
		} else if al.fastPathRatio > 0.60 && lat < relThreshold(150, 1.5) && al.currentLimit < al.maxInflight {
			al.currentLimit++
			al.cond.Signal()
		}
		// Normal backoff
		if (al.fastPathRatio < 0.4 || lat > relThreshold(250, 2.5)) && al.currentLimit > al.minInflight {
			al.currentLimit--
		}

	} else {
		if al.fastPathRatio > 0.85 && lat < relThreshold(50, 1.5) && al.currentLimit < al.maxInflight {
			increase := 5 // Jump by 5
			if al.currentLimit+increase > al.maxInflight {
				increase = al.maxInflight - al.currentLimit
			}
			al.currentLimit += increase
			for i := 0; i < increase; i++ {
				al.cond.Signal()
			}
		} else if al.fastPathRatio > 0.70 && lat < relThreshold(100, 1.5) && al.currentLimit < al.maxInflight {
			increase := 2
			if al.currentLimit+increase > al.maxInflight {
				increase = al.maxInflight - al.currentLimit
			}
			al.currentLimit += increase
			for i := 0; i < increase; i++ {
				al.cond.Signal()
			}
		}
		// Gentle backoff (fast path issues may be temporary)
		if (al.fastPathRatio < 0.3 || lat > relThreshold(200, 2.5)) && al.currentLimit > al.minInflight {
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
		var fastOps, slowOps int
		fmt.Sscanf(reply.PathUsed, "MIXED(FAST:%d,SLOW:%d)", &fastOps, &slowOps)
		if fastOps > 0 {
			atomic.AddInt64(&perfM.FastCommits, int64(fastOps))
		}
		if slowOps > 0 {
			atomic.AddInt64(&perfM.SlowCommits, int64(slowOps))
		}
		for i := 0; i < fastOps && i < batchSize; i++ {
			perfM.IncFastPath(clockVal)
		}
		for i := 0; i < slowOps && i < batchSize; i++ {
			perfM.IncSlowPath(clockVal)
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
		}
	}
}

// RecordBatchTS feeds the tps_timeline_*.csv time-series recorder.
//   rttMs           — client-measured round-trip latency (issue-to-completion,
//                      includes network both ways); the primary metric.
//   serverLatencyMs — reply.Latency, the server's own processing-time-only
//                      measurement; recorded alongside for comparison.
func RecordBatchTS(rttMs float64, serverLatencyMs float64, isErr bool) {
	if isErr {
		return
	}
	tsOpsTotal.Add(1)
	tsLatSumMs.Add(int64(rttMs * 100))
	tsLatCount.Add(1)
	tsServerLatSumMs.Add(int64(serverLatencyMs * 100))
}

func startTimeSeriesRecorder(clientID int, intervalMs int) {
	dirPath := fmt.Sprintf("./eval/client%d", clientID)
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		log.Errorf("[TS] Client %d: failed to create eval dir: %v", clientID, err)
		return
	}

	ts := time.Now().Format("20060102_150405")
	filePath := fmt.Sprintf("%s/tps_timeline_%s.csv", dirPath, ts)
	f, err := os.Create(filePath)
	if err != nil {
		log.Errorf("[TS] Client %d: failed to create timeline file: %v", clientID, err)
		return
	}

	if _, err := fmt.Fprintln(f, "elapsed_ms,tps,lat_avg_ms,server_lat_avg_ms,event"); err != nil {
		_ = f.Close()
		log.Errorf("[TS] Client %d: failed to write header: %v", clientID, err)
		return
	}

	startTime := time.Now()
	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)

	go func() {
		defer f.Close()
		defer ticker.Stop()

		var prevOps int64
		for range ticker.C {
			elapsed := time.Since(startTime).Milliseconds()
			curOps := tsOpsTotal.Load()
			curLat := tsLatSumMs.Load()
			curServerLat := tsServerLatSumMs.Load()
			curCnt := tsLatCount.Load()

			dOps := curOps - prevOps
			prevOps = curOps

			tps := 0.0
			if intervalMs > 0 {
				tps = float64(dOps) * 1000.0 / float64(intervalMs)
			}

			latAvg := 0.0
			serverLatAvg := 0.0
			if curCnt > 0 {
				latAvg = float64(curLat) / float64(curCnt) / 100.0
				serverLatAvg = float64(curServerLat) / float64(curCnt) / 100.0
			}

			eventFile := fmt.Sprintf("./eval/client%d/.event", clientID)
			event := ""
			if data, err := os.ReadFile(eventFile); err == nil {
				event = strings.TrimSpace(string(data))
				_ = os.Remove(eventFile)
			}

			if _, err := fmt.Fprintf(f, "%d,%.1f,%.2f,%.2f,%s\n", elapsed, tps, latAvg, serverLatAvg, event); err != nil {
				log.Warnf("[TS] Client %d: failed to write timeline row: %v", clientID, err)
			}
		}
	}()

	log.Infof("[Client %d] Time-series recorder started -> %s (interval=%dms)", clientID, filePath, intervalMs)
}

func RunClient(clientID int, configPath string, numOps int, indepRatio float64, batchMode string) {
	if pinServer < 0 {
		log.Fatalf("Client %d: -pinserver is required (round-robin dispatch has been removed; pass -pinserver=<serverID>)", clientID)
	}

	// Client IDs continue on from server IDs (see start_mongodb_hetero_5n.sh),
	// so normalize to a 0-based index for keyOwnerClientIdx comparisons.
	clientIdx := clientID - numOfServers
	if clientIdx < 0 {
		clientIdx = 0
	}

	pipelined := os.Getenv("PIPELINE_MODE") == "true"
	maxInflight := 5 // default conservative
	if val := os.Getenv("MAX_INFLIGHT"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &maxInflight); err == nil && n == 1 && maxInflight > 0 {
			// Explicit override from environment
		} else {
			maxInflight = 5
		}
	} else {
		slowPathRatio := (100.0 - indepRatio) / 100.0

		if slowPathRatio >= 0.80 {
			maxInflight = 3
			log.Infof("Client %d: SLOW workload (%.0f%% slow) → MAX_INFLIGHT=%d (conservative for serialized server)",
				clientID, slowPathRatio*100, maxInflight)
		} else if slowPathRatio >= 0.30 {
			maxInflight = 8 // Reduced from 12 for cloud stability
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
	if os.Getenv("ENABLE_TIMESERIES") == "true" {
		tsInterval := 500
		if v := os.Getenv("TPS_TIMELINE_INTERVAL_MS"); v != "" {
			if n, err := fmt.Sscanf(v, "%d", &tsInterval); err != nil || n != 1 || tsInterval <= 0 {
				tsInterval = 500
			}
		}
		go startTimeSeriesRecorder(clientID, tsInterval)
	} else {
		log.Infof("[Client %d] Time-series recorder disabled (ENABLE_TIMESERIES=false)", clientID)
	}

	rand.Seed(time.Now().UnixNano() + int64(clientID))
	pool := NewServerPool(conns, clientID)
	pool.startReviver(5 * time.Second)

	// Objects are pre-warmed cluster-wide by the servers (see
	// preWarmAllObjects / InitObjectRegistry) - no client-side pre-creation
	// RPC is needed for any object type.

	opsLabel := fmt.Sprintf("%d", numOps)
	if numOps <= 0 {
		opsLabel = "infinite"
	}

	fmt.Printf("Client %d started | Total Ops: %s | Batch Mode: %s | Batch Size: %d | Composition: %s\n",
		clientID, opsLabel, batchMode, batchsize, batchComposition)
	fmt.Printf("  Object Distribution: Independent=%.0f%% | Dependent=%.0f%% (pool=%d objects)\n",
		indepRatio, 100.0-indepRatio, numObjects)

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

	// sendCommand picks a target server and dispatches cmd (pipelined or
	// sequential, per the client's mode), recording metrics exactly as the
	// single inline call site used to. Extracted into its own function so a
	// single loop iteration can send more than one command - needed for
	// MongoDB batches that split into a same-type independent request and a
	// same-type dependent request (see the MongoDB batch-composition branch
	// below). Returns false if no healthy server was available (caller
	// should retry without advancing op, matching the original behavior).
	sendCommand := func(cmd *Args, currentBatch int) bool {
		// SELECT TARGET SERVER
		// Server will decide: fast path (coordinate) or slow path (forward to
		// owner) regardless of which replica this lands on - but for an
		// independent-object request, we already know (deterministically,
		// via the same hash ring servers use - see ObjectOwner in
		// objectmap.go) which replica actually owns fast-path coordination
		// for cmd.ObjID, so prefer sending straight there instead of always
		// paying a guaranteed extra forward hop through the fixed
		// -pinserver for the ~(N-1)/N fraction of requests it doesn't
		// happen to own. Falls back to the pinned server (via PickPinned's
		// own unhealthy-target fallback) if the owner is currently down.
		// Dependent-object and multi-object requests keep routing to the
		// pinned server - there's no single "owner" to aim for (they always
		// need the global leader, or span several objects with different
		// owners), and the server-side forward-to-leader path already
		// handles that case.
		preferredServer := pinServer
		if cmd.ObjType == IndependentObject && cmd.ObjID != "" {
			preferredServer = ObjectOwner(cmd.ObjID)
		}
		targetSID, _, ok := pool.PickPinned(preferredServer)
		if !ok {
			log.Warnf("[Client %d] No healthy servers available, retrying shortly...", clientID)
			time.Sleep(200 * time.Millisecond)
			return false
		}

		if pipelined {
			// Track limiter wait time SEPARATELY (not included in perfM timing)
			limiterWaitStart := time.Now()
			limiter.Acquire()
			limiterWaitMs := time.Since(limiterWaitStart).Milliseconds()

			// Log if limiter causes significant blocking (>1ms = bottleneck)
			if limiterWaitMs > 1 {
				log.Warnf("[Client %d] Limiter BLOCKED for %dms (bottleneck detected!)", clientID, limiterWaitMs)
			}

			go func(clockVal int, command *Args, serverID int, batchSize int) {
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
				rpcStart := time.Now()
				_, err := callWithFailover(pool, serverID, command, reply)
				rpcLatencyMs := float64(time.Since(rpcStart).Microseconds()) / 1000.0
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
					RecordBatchTS(0, 0, true)
				} else {
					recordBatchMetrics(reply, clockVal, batchSize)
					RecordBatch(batchSize, reply.Latency, reply.PathUsed, false)
					RecordBatchTS(rpcLatencyMs, reply.Latency, false)

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
			}(cmd.ClientClock, cmd, targetSID, currentBatch)

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
			_, err := callWithFailover(pool, targetSID, cmd, reply)
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
				RecordBatchTS(0, 0, true)
			} else {
				recordBatchMetrics(reply, cmd.ClientClock, currentBatch)
				RecordBatch(currentBatch, reply.Latency, reply.PathUsed, false)
				RecordBatchTS(float64(rpcLatency.Microseconds())/1000.0, reply.Latency, false)
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
		return true
	}

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
				time.Sleep(50 * time.Microsecond) // Tiny yield to scheduler
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
		isRead := readRatio > 0 && rand.Float64()*100 < readRatio
		if isRead {
			cmd.CmdType = READ
			cmd.ReadMode = readMode
		}

		// Batch composition logic
		if targetObjID != "" {
			// Pin every operation to one exact object, for measuring that
			// object's own throughput/latency (see -hotobjthreshold eval) -
			// every element of the batch targets the same object, same
			// shape as "single_obj" but a fixed ID instead of a fresh
			// random pick per batch.
			meta, ok := objectByID[targetObjID]
			if !ok {
				log.Fatalf("Client %d: -targetobjid=%s not found in the object registry", clientID, targetObjID)
			}
			cmd.IsMixed = false
			cmd.ObjType = meta.Type
			cmd.ObjID = meta.ID
		} else if evalType == MongoDB {
			// MongoDB routing keys off the real YCSB record being touched,
			// not the synthetic obj-N pool: the consensus layer's
			// per-object lock/ownership has to apply to the document
			// actually written, or the fast/slow-path ordering guarantee
			// says nothing about what ends up in MongoDB. classifyRealKey
			// types each key deterministically off -indep (same mechanism
			// as the synthetic pool, so the I2D sweep still works here);
			// keyOwnerClientIdx is a separate concern, used only to pick
			// which client's traffic treats an independent-typed key as
			// "mine" (approximating real single-writer access), so this
			// client doesn't manufacture artificial contention on keys
			// nominally independent but actually driven by another client.
			keys := make([]string, currentBatch)
			types := make([]int, currentBatch)
			queries := make([]mongodb.Query, currentBatch)
			maxTries := len(mongoDBQueries)
			if maxTries > 2000 {
				maxTries = 2000
			}
			for b := 0; b < currentBatch; b++ {
				base := (op + b) % len(mongoDBQueries)
				q := mongoDBQueries[base]
				if q.Key != "" {
					for tries := 0; tries < maxTries; tries++ {
						cand := mongoDBQueries[(base+tries)%len(mongoDBQueries)]
						if cand.Key == "" {
							continue
						}
						if classifyRealKey(cand.Key) == DependentObject ||
							keyOwnerClientIdx(cand.Key, numClients) == clientIdx {
							q = cand
							break
						}
					}
				}
				queries[b] = q
				keys[b] = q.Key
				types[b] = classifyRealKey(q.Key)
			}

			if isRead {
				// Reads bypass conJobMongoDB's write classification entirely
				// (handled via handleRead instead), so there's no slow-path-
				// forcing behavior to split around here - keep the existing
				// single-command shape and fall through to sendCommand below.
				cmd.IsMixed = true
				cmd.ObjIDs = keys
				cmd.ObjTypes = types
				cmd.ObjID = keys[0]
				cmd.ObjType = types[0]
			} else {
				// Split into same-type batches so each actually gets routed
				// via its own fast/slow path. Before this split, one
				// dependent-typed key anywhere in the batch forced the
				// *entire* batch onto the slow path (conJobMongoDB's
				// needsSlowPath), while the client-reported
				// "MIXED(FAST:x,SLOW:y)" label kept describing the batch's
				// key-type composition, not which path actually ran -
				// making it look like partial fast-path execution was
				// happening when in practice almost none was, for any
				// batch spanning more than one key at a typical -indep
				// ratio.
				var indepKeys, depKeys []string
				var indepQueries, depQueries []mongodb.Query
				for b := 0; b < currentBatch; b++ {
					if types[b] == IndependentObject {
						indepKeys = append(indepKeys, keys[b])
						indepQueries = append(indepQueries, queries[b])
					} else {
						depKeys = append(depKeys, keys[b])
						depQueries = append(depQueries, queries[b])
					}
				}

				sameType := func(n, t int) []int {
					ts := make([]int, n)
					for i := range ts {
						ts[i] = t
					}
					return ts
				}
				buildMongoCmd := func(clock int, ks []string, qs []mongodb.Query, t int) *Args {
					return &Args{
						ClientID: clientID, ClientClock: clock, CmdType: WRITE, Type: evalType,
						IsMixed: true, ObjIDs: ks, ObjTypes: sameType(len(ks), t),
						ObjID: ks[0], ObjType: t, CmdMongo: qs,
					}
				}

				sent := true
				switch {
				case len(indepKeys) > 0 && len(depKeys) > 0:
					sent = sendCommand(buildMongoCmd(CClock, indepKeys, indepQueries, IndependentObject), len(indepKeys))
					if sent {
						sent = sendCommand(buildMongoCmd(incrementClock(), depKeys, depQueries, DependentObject), len(depKeys))
					}
				case len(indepKeys) > 0:
					sent = sendCommand(buildMongoCmd(CClock, indepKeys, indepQueries, IndependentObject), len(indepKeys))
				case len(depKeys) > 0:
					sent = sendCommand(buildMongoCmd(CClock, depKeys, depQueries, DependentObject), len(depKeys))
				}

				if !sent {
					continue
				}
				op += currentBatch
				continue
			}
		} else if batchComposition == "mixed" {
			// Each element of the batch is independently typed by indepRatio.
			cmd.IsMixed = true
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				meta := PickObjectOfType(PickObjectType())
				cmd.ObjTypes[b] = meta.Type
				cmd.ObjIDs[b] = meta.ID
			}
			cmd.ObjID = cmd.ObjIDs[0]
			cmd.ObjType = cmd.ObjTypes[0]
		} else if batchComposition == "single_obj" {
			// One object, reused for every element of the batch.
			cmd.IsMixed = false
			meta := PickObjectOfType(PickObjectType())
			cmd.ObjType = meta.Type
			cmd.ObjID = meta.ID
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				cmd.ObjTypes[b] = meta.Type
				cmd.ObjIDs[b] = meta.ID
			}
		} else if batchComposition == "multi_obj" {
			// One atomic transaction touching N distinct objects together -
			// always dependent/slow-path regardless of the individual
			// objects' types (paper classification: spanning >1 object
			// forces dependent). currentBatch is overridden to N so the
			// shared CmdPlain-building code below produces exactly one
			// payload element per object.
			const multiObjN = 2
			const maxPickAttempts = 50 // bounded: PickObjectOfType(type) returns nil if that type's pool is empty (e.g. -indep=100 or 0)
			cmd.MultiObject = true
			cmd.ObjIDs = make([]string, 0, multiObjN)
			cmd.ObjTypes = make([]int, 0, multiObjN)
			seen := map[string]bool{}
			for attempts := 0; len(cmd.ObjIDs) < multiObjN && attempts < maxPickAttempts; attempts++ {
				meta := PickObjectOfType(PickObjectType())
				if meta == nil || seen[meta.ID] {
					continue
				}
				seen[meta.ID] = true
				cmd.ObjIDs = append(cmd.ObjIDs, meta.ID)
				cmd.ObjTypes = append(cmd.ObjTypes, meta.Type)
			}
			if len(cmd.ObjIDs) < 2 {
				log.Errorf("[MULTI-OBJ] could not pick %d distinct objects (object pool too small/skewed for -indep=%.0f) - skipping op", multiObjN, indepRatio)
				op++
				continue
			}
			cmd.ObjID = cmd.ObjIDs[0]
			cmd.ObjType = DependentObject
			currentBatch = len(cmd.ObjIDs)
		} else {
			// "object-specific": one type for the whole batch, spread across
			// different objects of that type.
			cmd.IsMixed = false
			objType := PickObjectType()
			cmd.ObjType = objType
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				meta := PickObjectOfType(objType)
				cmd.ObjTypes[b] = meta.Type
				cmd.ObjIDs[b] = meta.ID
			}
			cmd.ObjID = cmd.ObjIDs[0]
		}

		if !isRead && evalType == PlainMsg {
			batch := make([][]byte, currentBatch)
			for b := 0; b < currentBatch; b++ {
				batch[b] = genRandomBytes(msgsize)
			}
			cmd.CmdPlain = batch
		}
		// MongoDB's CmdMongo is built above, alongside the real-key-driven
		// ObjID/ObjType, since both must agree on which query was picked.

		if !sendCommand(cmd, currentBatch) {
			continue
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
