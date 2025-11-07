package eval

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
	"sync/atomic"

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
	sync.RWMutex
	numOfTotalTx   int
	batchSize      int
	sampleInterval prioClock
	lastPClock     prioClock
	fileName       string
	meters         map[prioClock]*RecordInstance
	// Global totals (optional)
	FastCommits   int64
	SlowCommits   int64
	ConflictCommits int64
}

type RecordInstance struct {
	StartTime   time.Time
	TimeElapsed int64
	Metrics     BatchMetrics
}

// ---------------- Initialization ----------------
func (m *PerfMeter) Init(interval, batchSize int, fileName string) {
	m.sampleInterval = interval
	m.lastPClock = 0
	m.batchSize = batchSize
	m.numOfTotalTx = 0
	m.fileName = fileName
	m.meters = make(map[prioClock]*RecordInstance)
}

// ---------------- Record start/end ----------------
func (m *PerfMeter) RecordStarter(pClock prioClock) {
	m.Lock()
	defer m.Unlock()

	m.meters[pClock] = &RecordInstance{
		StartTime:   time.Now(),
		TimeElapsed: 0,
		Metrics:     BatchMetrics{},
	}
}

func (m *PerfMeter) RecordFinisher(pClock prioClock) error {
	m.Lock()
	defer m.Unlock()

	_, exist := m.meters[pClock]
	if !exist {
		return errors.New("pClock has not been recorded with starter")
	}

	//m.meters[pClock].EndTime = time.Now()
	start := m.meters[pClock].StartTime
	m.meters[pClock].TimeElapsed = time.Now().Sub(start).Milliseconds()

	return nil
}

// ---------------- Increment counters ----------------
func (m *PerfMeter) IncFastPath(pClock prioClock) {
	m.Lock()
	defer m.Unlock()
	if rec, ok := m.meters[pClock]; ok {
		rec.Metrics.FastPathCount++
	}
}

func (m *PerfMeter) IncSlowPath(pClock prioClock) {
	m.Lock()
	defer m.Unlock()
	if rec, ok := m.meters[pClock]; ok {
		rec.Metrics.SlowPathCount++
	}
}

func (m *PerfMeter) IncConflict(pClock prioClock) {
	m.Lock()
	defer m.Unlock()
	if rec, ok := m.meters[pClock]; ok {
		rec.Metrics.ConflictCount++
	}
}

func (pm *PerfMeter) RecordFastCommit() {
    atomic.AddInt64(&pm.FastCommits, 1)
}

func (pm *PerfMeter) RecordSlowCommit() {
    atomic.AddInt64(&pm.SlowCommits, 1)
}
// ---------------- Save to file ----------------
func (m *PerfMeter) SaveToFile() error {
	// Determine folder name from fileName
	// Expected formats: "client<id>_eval" or "s<id>_n<num>_f<quorum>_b<batchsize>_<suffix>"
	var folderName string
	
	// Check if it's a client file
	if len(m.fileName) >= 6 && m.fileName[:6] == "client" {
		// Extract client ID - find where "_eval" starts or other delimiter
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
		// Server file - extract server ID
		var serverPart string
		for i := 1; i < len(m.fileName); i++ {
			if m.fileName[i] == '_' {
				serverPart = "server" + m.fileName[1:i]
				break
			}
		}
		if serverPart == "" {
			serverPart = "server" + m.fileName[1:]
		}
		folderName = serverPart
	} else {
		// Default fallback
		folderName = "default"
	}
	
	// Create directory structure
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

	m.RLock()
	defer m.RUnlock()

	var keys []int
	for key := range m.meters {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	err = writer.Write([]string{"pclock", "latency (ms) per batch", "throughput (Tx/sec)","fast path ops", "slow path ops", "conflict ops",})
	if err != nil {
		return err
	}

	counter := 0
	var latSum int64 = 0
	var tptSum float64 = 0  // Sum of individual batch throughputs
	var fastSum, slowSum, conflictSum int = 0, 0, 0

	for _, key := range keys {
		value := m.meters[key]
		if value.TimeElapsed == 0 {
			continue
		}

		latSum += value.TimeElapsed
		counter++
		fastSum += value.Metrics.FastPathCount
		slowSum += value.Metrics.SlowPathCount
		conflictSum += value.Metrics.ConflictCount

		lat := value.TimeElapsed
		// Throughput per batch: (operations / latency_ms) * 1000 = Tx/sec
		tpt := (float64(m.batchSize) / float64(lat)) * 1000
		tptSum += tpt  // Accumulate for averaging
		
		row := []string{
			strconv.Itoa(key),
			strconv.FormatInt(lat, 10),
			strconv.FormatFloat(tpt, 'f', 3, 64),
			strconv.Itoa(value.Metrics.FastPathCount),
			strconv.Itoa(value.Metrics.SlowPathCount),
			strconv.Itoa(value.Metrics.ConflictCount),
		}

		err := writer.Write(row)
		if err != nil {
			return err
		}
	}

	if counter == 0 {
		return errors.New("counter is 0")
	}

	// Calculate basic averages
	avgLatency := float64(latSum) / float64(counter)
	
	// Overall throughput calculation:
	// Method 1: Total ops / total elapsed wall-clock time (includes client delays)
	lastTime := m.meters[keys[len(keys)-1]]
	lastEndTime := lastTime.StartTime.Add(time.Duration(lastTime.TimeElapsed) * time.Millisecond)
	totalElapsedSeconds := lastEndTime.Sub(m.meters[keys[0]].StartTime).Seconds()
	overallThroughput := float64(m.batchSize*counter) / totalElapsedSeconds
	
	// Method 2: Total ops / sum of all batch processing times (active processing only)
	// This gives the TRUE system throughput without client-side delays
	totalProcessingSeconds := float64(latSum) / 1000.0 // Convert ms to seconds
	systemThroughput := float64(m.batchSize*counter) / totalProcessingSeconds
	
	// Average batch throughput: average of individual batch throughputs
	avgBatchThroughput := tptSum / float64(counter)
	
	avgFast := float64(fastSum) / float64(counter)
	avgSlow := float64(slowSum) / float64(counter)
	avgConflict := float64(conflictSum) / float64(counter)

	// Calculate tail latencies (p50, p95, p99)
	var latencies []int64
	var fastPathLatencies []int64
	var slowPathLatencies []int64
	
	for _, key := range keys {
		value := m.meters[key]
		if value.TimeElapsed == 0 {
			continue
		}
		latencies = append(latencies, value.TimeElapsed)
		
		// Track fast path vs slow path latencies
		if value.Metrics.FastPathCount > value.Metrics.SlowPathCount {
			fastPathLatencies = append(fastPathLatencies, value.TimeElapsed)
		} else if value.Metrics.SlowPathCount > 0 {
			slowPathLatencies = append(slowPathLatencies, value.TimeElapsed)
		}
	}
	
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	
	p50Latency := latencies[len(latencies)*50/100]
	p95Latency := latencies[len(latencies)*95/100]
	p99Latency := latencies[len(latencies)*99/100]
	
	// Calculate latency per transaction
	avgLatencyPerTx := avgLatency / float64(m.batchSize)
	
	// Calculate fast path and slow path specific latencies
	var avgFastPathLatency float64
	var avgSlowPathLatency float64
	
	if len(fastPathLatencies) > 0 {
		var fastSum int64
		for _, lat := range fastPathLatencies {
			fastSum += lat
		}
		avgFastPathLatency = float64(fastSum) / float64(len(fastPathLatencies))
	}
	
	if len(slowPathLatencies) > 0 {
		var slowSum int64
		for _, lat := range slowPathLatencies {
			slowSum += lat
		}
		avgSlowPathLatency = float64(slowSum) / float64(len(slowPathLatencies))
	}

	// Write summary row with averages
	err = writer.Write([]string{
		"AVERAGE",
		strconv.FormatFloat(avgLatency, 'f', 3, 64) + " ms",
		strconv.FormatFloat(avgBatchThroughput, 'f', 3, 64) + " Tx/sec",
		strconv.FormatFloat(avgFast, 'f', 3, 64),
		strconv.FormatFloat(avgSlow, 'f', 3, 64),
		strconv.FormatFloat(avgConflict, 'f', 3, 64),
	})
	if err != nil {
		return err
	}
	
	// Write overall throughput (total ops / total time)
	err = writer.Write([]string{
		"OVERALL_THROUGHPUT",
		"",
		strconv.FormatFloat(overallThroughput, 'f', 3, 64) + " Tx/sec",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}
	
	// Write system throughput (total ops / active processing time only)
	err = writer.Write([]string{
		"SYSTEM_THROUGHPUT",
		"",
		strconv.FormatFloat(systemThroughput, 'f', 3, 64) + " Tx/sec",
		"(active processing time only)",
		"",
		"",
	})
	if err != nil {
		return err
	}
	
	// Write tail latency metrics
	err = writer.Write([]string{
		"P50_LATENCY",
		strconv.FormatInt(p50Latency, 10) + " ms",
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
		strconv.FormatInt(p95Latency, 10) + " ms",
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
		strconv.FormatInt(p99Latency, 10) + " ms",
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

	return nil
}