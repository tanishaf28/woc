package eval

import (
	"testing"
	"time"
)

// countRecords mirrors SaveToFile's own iteration logic (Range over m.meters,
// skip entries whose TimeElapsed is still zero) so the test exercises the
// same counting path SaveToFile relies on, without touching the filesystem.
func countRecords(m *PerfMeter) int {
	count := 0
	m.meters.Range(func(_, value interface{}) bool {
		if value.(*RecordInstance).Finished {
			count++
		}
		return true
	})
	return count
}

func TestDiscardRecordRemovesEntry(t *testing.T) {
	m := &PerfMeter{}
	m.Init(1, 1, "test")

	m.RecordStarter(1)
	m.RecordFinisher(1)

	if _, ok := m.meters.Load(1); !ok {
		t.Fatalf("expected globalClock 1 to be present after RecordStarter/RecordFinisher")
	}

	m.DiscardRecord(1)

	if _, ok := m.meters.Load(1); ok {
		t.Fatalf("expected globalClock 1 to be removed after DiscardRecord")
	}
}

// TestFallbackDiscardPreventsDoubleCounting reproduces the exact shape of
// the bug this fix addresses: HandleCommand's IndependentObject branch opens
// an outer globalClock (RecordStarter), and if the fast-path attempt times
// out and falls back to the slow path, startSlowPathProcessor records its
// own separate, authoritative entry under a different globalClock. Before
// the fix, the outer entry was always finalized (RecordFinisher + a Fast/Slow
// Inc call) regardless of outcome, so one real operation produced two
// "completed round" entries - inflating SaveToFile's counter (and hence its
// throughput calculation). This test asserts the discard-on-fallback pattern
// leaves exactly one entry for one logical operation, not two.
func TestFallbackDiscardPreventsDoubleCounting(t *testing.T) {
	m := &PerfMeter{}
	m.Init(1, 1, "test")

	const outerClock = 10
	const innerClock = 11

	// Outer: HandleCommand opens a record for the fast-path attempt.
	m.RecordStarter(outerClock)

	// The attempt times out and falls back; startSlowPathProcessor records
	// its own complete entry for the same logical operation.
	m.RecordStarterAt(innerClock, time.Now())
	m.IncSlowPath(innerClock)
	m.RecordFinisher(innerClock)

	// Outer entry must be discarded, not finalized, once the caller learns
	// the round actually resolved via the inner (slow-path) entry.
	m.DiscardRecord(outerClock)

	got := countRecords(m)
	if got != 1 {
		t.Fatalf("expected exactly 1 completed record for 1 logical operation (fast-path fallback), got %d", got)
	}
}
