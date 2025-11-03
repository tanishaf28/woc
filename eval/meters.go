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

	file, err := os.Create(fmt.Sprintf("./eval/%s.csv", m.fileName))
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

	err = writer.Write([]string{"pclock", "latency (ms) per batch", "throughput (Tx per Second)","fast path ops", "slow path ops", "conflict ops",})
	if err != nil {
		return err
	}

	counter := 0
	var latSum int64 = 0
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
		tpt := (float64(m.batchSize) / float64(lat)) * 1000
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

	avgLatency := float64(latSum) / float64(counter)
	lastTime := m.meters[keys[len(keys)-1]]
	lastEndTime := lastTime.StartTime.Add(time.Duration(lastTime.TimeElapsed) * time.Millisecond)
	avgThroughput := float64(m.batchSize*counter) / lastEndTime.Sub(m.meters[keys[0]].StartTime).Seconds()
	avgFast := float64(fastSum) / float64(counter)
	avgSlow := float64(slowSum) / float64(counter)
	avgConflict := float64(conflictSum) / float64(counter)


	err = writer.Write([]string{
		"-1",
		strconv.FormatFloat(avgLatency, 'f', 3, 64),
		strconv.FormatFloat(avgThroughput, 'f', 3, 64),
		strconv.FormatFloat(avgFast, 'f', 3, 64),
		strconv.FormatFloat(avgSlow, 'f', 3, 64),
		strconv.FormatFloat(avgConflict, 'f', 3, 64),
	})
	if err != nil {
		return err
	}
	// ---- NEW: write global totals row ----
	err = writer.Write([]string{
		"GLOBAL",
		"", // latency not applicable
		"", // throughput not applicable
		strconv.FormatInt(m.FastCommits, 10),
		strconv.FormatInt(m.SlowCommits, 10),
		strconv.FormatInt(m.ConflictCommits, 10),
	})
	if err != nil {
		return err
	}

	return nil
}


