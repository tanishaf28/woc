package eval

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type serverID = int
type prioClock = int
type priority = float64

type BatchMetrics struct {
	FastPathCount int
	SlowPathCount int
	ConflictCount int
}

type PerfMeter struct {
	numOfTotalTx   int
	batchSize      int
	sampleInterval prioClock
	lastPClock     prioClock
	fileName       string
	meters         sync.Map // Lock-free concurrent map: prioClock -> *RecordInstance
	FastCommits     int64
	SlowCommits     int64
	ConflictCommits int64
    FastPathFallbacks int64 // Track fast path failures to slow path
    fastPathLatencySum int64
    fastPathCount      int64
    slowPathLatencySum int64
    slowPathCount      int64
}

type RecordInstance struct {
	StartTime   time.Time
	TimeElapsed float64
	Metrics     BatchMetrics
}


// ---------------- Initialization ----------------
func (m *PerfMeter) Init(interval, batchSize int, fileName string) {
	m.sampleInterval = interval
	m.lastPClock = 0
	m.batchSize = batchSize
	m.numOfTotalTx = 0
	m.fileName = fileName
}

// ---------------- Record start/end ----------------
func (m *PerfMeter) RecordStarter(globalClock int) {
	m.meters.Store(globalClock, &RecordInstance{
		StartTime:   time.Now(),
		TimeElapsed: 0.0,
		Metrics:     BatchMetrics{},
	})
}

func (m *PerfMeter) RecordStarterAt(globalClock int, startTime time.Time) {
	m.meters.Store(globalClock, &RecordInstance{
		StartTime:   startTime,
		TimeElapsed: 0.0,
		Metrics:     BatchMetrics{},
	})
}

func (m *PerfMeter) RecordFinisher(globalClock int) error {
	val, exist := m.meters.Load(globalClock)
	if !exist {
		return errors.New("globalClock has not been recorded with starter")
	}

	rec := val.(*RecordInstance)
	start := rec.StartTime
	rec.TimeElapsed = float64(time.Now().Sub(start).Microseconds()) / 1000.0 // Record precise milliseconds

	return nil
}

// ---------------- Increment counters ----------------
func (m *PerfMeter) IncFastPath(globalClock int) {
	if val, ok := m.meters.Load(globalClock); ok {
		rec := val.(*RecordInstance)
		rec.Metrics.FastPathCount++
	}
}

func (m *PerfMeter) IncSlowPath(globalClock int) {
	if val, ok := m.meters.Load(globalClock); ok {
		rec := val.(*RecordInstance)
		rec.Metrics.SlowPathCount++
	}
}

func (m *PerfMeter) IncConflict(globalClock int) {
	if val, ok := m.meters.Load(globalClock); ok {
		rec := val.(*RecordInstance)
		rec.Metrics.ConflictCount++
	}
}

func (pm *PerfMeter) RecordFastCommit() {
	atomic.AddInt64(&pm.FastCommits, 1)
}

func (pm *PerfMeter) RecordSlowCommit() {
	atomic.AddInt64(&pm.SlowCommits, 1)
}

// Add n commits to the global counters
func (pm *PerfMeter) AddFastCommits(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&pm.FastCommits, int64(n))
}

func (pm *PerfMeter) AddSlowCommits(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&pm.SlowCommits, int64(n))
}

func (pm *PerfMeter) AddConflictCommits(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&pm.ConflictCommits, int64(n))
}

// Track when fast path fails and falls back to slow path
func (pm *PerfMeter) RecordFastPathFallback() {
    atomic.AddInt64(&pm.FastPathFallbacks, 1)
}

// ---------------- Save to file ----------------
func (m *PerfMeter) SaveToFile() error {
	var folderName string
	if len(m.fileName) >= 6 && m.fileName[:6] == "client" {
		var clientPart string
		for i := 6; i < len(m.fileName); i++ {
			if m.fileName[i] == '_' {
				clientPart = m.fileName[:i]
				break
			}
		}
		if clientPart == "" {
			clientPart = m.fileName
		}
		folderName = clientPart
	} else if len(m.fileName) >= 1 && m.fileName[0] == 's' {
		folderName = fmt.Sprintf("server%d", m.fileName[1]-'0')
	} else {
		folderName = "default"
	}

	dirPath := fmt.Sprintf("./eval/%s", folderName)
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return err
	}

	// Add timestamp to filename
	timestamp := time.Now().Format("20060102_150405")
	filePath := fmt.Sprintf("%s/%s_%s.csv", dirPath, m.fileName, timestamp)

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Collect all keys from sync.Map
	var keys []int
	m.meters.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(int))
		return true
	})
	sort.Ints(keys)

	fmt.Printf("[DEBUG-METRICS] Total keys in sync.Map: %d\n", len(keys))

	err = writer.Write([]string{"pclock", "latency (ms) per batch", "throughput (Tx/sec)", "fast path ops", "slow path ops", "conflict ops"})
	if err != nil {
		return err
	}

	counter := 0
	var latSum float64 = 0 
	var tptSum float64 = 0
	var latencies []float64  
	var fastSum, slowSum, conflictSum int = 0, 0, 0  

	for _, key := range keys {
		val, ok := m.meters.Load(key)
		if !ok {
			continue
		}
		value := val.(*RecordInstance)
		if value.TimeElapsed == 0 {
			fmt.Printf("[DEBUG-METRICS] Skipping batch %d: TimeElapsed=0\n", key)
			continue
		}

		latSum += value.TimeElapsed 
		counter++
		latencies = append(latencies, value.TimeElapsed)  
		fastSum += value.Metrics.FastPathCount
		slowSum += value.Metrics.SlowPathCount
		conflictSum += value.Metrics.ConflictCount

		lat := value.TimeElapsed
		tpt := (float64(m.batchSize) / float64(lat)) * 1000
		tptSum += tpt 

		err = writer.Write([]string{
			strconv.Itoa(key),
			strconv.FormatFloat(lat, 'f', 3, 64),
			strconv.FormatFloat(tpt, 'f', 3, 64),
			strconv.Itoa(value.Metrics.FastPathCount),
			strconv.Itoa(value.Metrics.SlowPathCount),
			strconv.Itoa(value.Metrics.ConflictCount),
		})
		if err != nil {
			return err
		}
	}

	if counter == 0 {
		// No completed batches recorded. Write a NO_DATA row and still emit global totals
		_ = writer.Write([]string{"NO_DATA", "", "", "", "", ""})
		// Still write global totals (likely zeros)
		err = writer.Write([]string{
			"GLOBAL_TOTALS",
			"",
			"",
			strconv.FormatInt(m.FastCommits, 10) + " ops",
			strconv.FormatInt(m.SlowCommits, 10) + " ops",
			strconv.FormatInt(m.ConflictCommits, 10) + " ops",
		})
		if err != nil {
			return err
		}
		return nil
	}

	
	avgLatency := latSum / float64(counter) 
	// Calculate actual wall-clock throughput
	firstVal, _ := m.meters.Load(keys[0])
	firstBatch := firstVal.(*RecordInstance)
	lastVal, _ := m.meters.Load(keys[len(keys)-1])
	lastBatch := lastVal.(*RecordInstance)
	lastEndTime := lastBatch.StartTime.Add(time.Duration(lastBatch.TimeElapsed) * time.Millisecond)
	totalWallClockSeconds := lastEndTime.Sub(firstBatch.StartTime).Seconds()

	// This is your TRUE throughput (Tx/sec)
	actualThroughput := float64(m.batchSize*counter) / totalWallClockSeconds

	avgFast := float64(fastSum) / float64(counter)
	avgSlow := float64(slowSum) / float64(counter)
	avgConflict := float64(conflictSum) / float64(counter)

	// Calculate tail latencies (p50, p95, p99)
	var fastPathLatencies []float64  
	var slowPathLatencies []float64   

	for _, key := range keys {
		val, ok := m.meters.Load(key)
		if !ok {
			continue
		}
		value := val.(*RecordInstance)
		if value.TimeElapsed == 0 {
			continue
		}

		// Track fast path vs slow path latencies
		if value.Metrics.FastPathCount > value.Metrics.SlowPathCount {
			fastPathLatencies = append(fastPathLatencies, value.TimeElapsed)  
		} else if value.Metrics.SlowPathCount > 0 {
			slowPathLatencies = append(slowPathLatencies, value.TimeElapsed)  
		}
	}

	sort.Float64s(latencies)   

	p50Latency := latencies[len(latencies)*50/100]
	p95Latency := latencies[len(latencies)*95/100]
	p99Latency := latencies[len(latencies)*99/100]

	// Calculate latency per transaction
	avgLatencyPerTx := avgLatency / float64(m.batchSize)
	var avgFastPathLatency float64
	var avgSlowPathLatency float64

	if len(fastPathLatencies) > 0 {
		var fastSum float64  
		for _, lat := range fastPathLatencies {
			fastSum += lat  
		}
		avgFastPathLatency = fastSum / float64(len(fastPathLatencies))
	}

	if len(slowPathLatencies) > 0 {
		var slowSum float64  
		for _, lat := range slowPathLatencies {
			slowSum += lat   
		}
		avgSlowPathLatency = slowSum / float64(len(slowPathLatencies))
	}

	// Write summary row with averages
	err = writer.Write([]string{
		"AVERAGE",
		strconv.FormatFloat(avgLatency, 'f', 3, 64) + " ms",
		strconv.FormatFloat(actualThroughput, 'f', 3, 64) + " Tx/sec",
		strconv.FormatFloat(avgFast, 'f', 3, 64),
		strconv.FormatFloat(avgSlow, 'f', 3, 64),
		strconv.FormatFloat(avgConflict, 'f', 3, 64),
	})
	if err != nil {
		return err
	}

	// Write overall throughput (total ops / total time)
	err = writer.Write([]string{
		"THROUGHPUT",
		"",
		strconv.FormatFloat(actualThroughput, 'f', 3, 64) + " Tx/sec",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}


	err = writer.Write([]string{
		"P50_LATENCY",
		strconv.FormatFloat(p50Latency, 'f', 0, 64) + " ms",  
		"",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}

	err = writer.Write([]string{
		"P95_LATENCY",
		strconv.FormatFloat(p95Latency, 'f', 0, 64) + " ms",  
		"",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}

	err = writer.Write([]string{
		"P99_LATENCY",
		strconv.FormatFloat(p99Latency, 'f', 0, 64) + " ms",  
		"",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}

	// Write latency per transaction
	err = writer.Write([]string{
		"AVG_LATENCY_PER_TX",
		strconv.FormatFloat(avgLatencyPerTx, 'f', 3, 64) + " ms/Tx",
		"",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}

	// Write fast path specific latency
	if len(fastPathLatencies) > 0 {
		err = writer.Write([]string{
			"AVG_FAST_PATH_LATENCY",
			strconv.FormatFloat(avgFastPathLatency, 'f', 3, 64) + " ms",
			"",
			strconv.Itoa(len(fastPathLatencies)) + " batches",
			"",
			"",
		})
		if err != nil {
			return err
		}
        // Write fast path latency per transaction
        err = writer.Write([]string{
            "AVG_FAST_PATH_LATENCY_PER_TX",
            strconv.FormatFloat(avgFastPathLatency/float64(m.batchSize), 'f', 3, 64) + " ms/Tx",
            "",
            "",
            "",
            "",
        })
        if err != nil {
            return err
        }
	}

	// Write slow path specific latency
	if len(slowPathLatencies) > 0 {
		err = writer.Write([]string{
			"AVG_SLOW_PATH_LATENCY",
			strconv.FormatFloat(avgSlowPathLatency, 'f', 3, 64) + " ms",
			"",
			"",
			strconv.Itoa(len(slowPathLatencies)) + " batches",
			"",
		})
		if err != nil {
			return err
		}
        // Write slow path latency per transaction
        err = writer.Write([]string{
            "AVG_SLOW_PATH_LATENCY_PER_TX",
            strconv.FormatFloat(avgSlowPathLatency/float64(m.batchSize), 'f', 3, 64) + " ms/Tx",
            "",
            "",
            "",
            "",
        })
        if err != nil {
            return err
        }
	}

	// Write global totals row (operation counts, not batch counts)
	err = writer.Write([]string{
		"GLOBAL_TOTALS",
		"",
		"",
		strconv.FormatInt(m.FastCommits, 10) + " ops",
		strconv.FormatInt(m.SlowCommits, 10) + " ops",
		strconv.FormatInt(m.ConflictCommits, 10) + " ops",
	})
	if err != nil {
		return err
	}


    // Write fast path fallback count
    err = writer.Write([]string{"TOTAL_FAST_PATH_FALLBACKS", strconv.FormatInt(m.FastPathFallbacks, 10), "ops"})
    if err != nil {
        return err
    }
	
	err = writer.Write([]string{"TOTAL_FAST_COMMITS", strconv.FormatInt(m.FastCommits, 10), "ops"})
	if err != nil {
		return err
	}
	err = writer.Write([]string{"TOTAL_SLOW_COMMITS", strconv.FormatInt(m.SlowCommits, 10), "ops"})
	if err != nil {
		return err
	}
	err = writer.Write([]string{"TOTAL_CONFLICT_COMMITS", strconv.FormatInt(m.ConflictCommits, 10), "ops"})
	if err != nil {
		return err
	}
	

	return nil
}


func (m *PerfMeter) RecordFastPathLatency(latencyMs float64) {
	latencyUs := int64(latencyMs * 1000)
	atomic.AddInt64(&m.fastPathLatencySum, latencyUs)
	atomic.AddInt64(&m.fastPathCount, 1)
}

func (m *PerfMeter) RecordSlowPathLatency(latencyMs float64) {
	latencyUs := int64(latencyMs * 1000)
	atomic.AddInt64(&m.slowPathLatencySum, latencyUs)
	atomic.AddInt64(&m.slowPathCount, 1)
}
