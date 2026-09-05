package smr

import (
	"sort"
	"sync"
	"testing"
)

// objTypeIndependent mirrors package main's IndependentObject constant (0);
// smr is a lower-level package and doesn't import main's object-type
// constants, so tests use the literal value directly.
const objTypeIndependent = 0

func newTestObject(numReplicas, quorumSize int) *ObjectState {
	s := NewServerState()
	s.AddObject("obj-test", objTypeIndependent, numReplicas, quorumSize, 0.001)
	return s.GetObject("obj-test")
}

// ---------------- Feature 2: Seq / ApplyFastProvisional / RevertIfSeqMatches ----------------

func TestNextSeqMonotonic(t *testing.T) {
	obj := newTestObject(5, 3)

	prev := int64(0)
	for i := 0; i < 5; i++ {
		seq := obj.NextSeq()
		if seq <= prev {
			t.Fatalf("NextSeq not monotonic: got %d after %d", seq, prev)
		}
		prev = seq
	}
}

func TestApplyFastProvisionalRejectsStale(t *testing.T) {
	obj := newTestObject(5, 3)

	seq1 := obj.NextSeq()
	if !obj.ApplyFastProvisional(1, "v1", seq1) {
		t.Fatalf("expected first provisional apply (seq=%d) to be accepted", seq1)
	}
	if obj.Value != "v1" || obj.Seq != seq1 {
		t.Fatalf("apply did not take effect: Value=%v Seq=%d", obj.Value, obj.Seq)
	}

	// A seq <= current Seq must be rejected (stale/regressive).
	if obj.ApplyFastProvisional(2, "stale", seq1) {
		t.Fatalf("expected apply at seq=%d (== current) to be rejected", seq1)
	}
	if obj.ApplyFastProvisional(2, "stale", seq1-1) {
		t.Fatalf("expected apply at seq=%d (< current) to be rejected", seq1-1)
	}
	if obj.Value != "v1" {
		t.Fatalf("rejected apply must not mutate Value, got %v", obj.Value)
	}
}

func TestApplyFastProvisionalStashesPrev(t *testing.T) {
	obj := newTestObject(5, 3)

	seq1 := obj.NextSeq()
	obj.ApplyFastProvisional(1, "v1", seq1)

	seq2 := obj.NextSeq()
	if !obj.ApplyFastProvisional(2, "v2", seq2) {
		t.Fatalf("expected second provisional apply (seq=%d) to be accepted", seq2)
	}
	if obj.PrevValue != "v1" || obj.PrevSeq != seq1 {
		t.Fatalf("expected Prev* to hold the pre-apply state (v1, seq=%d), got PrevValue=%v PrevSeq=%d",
			seq1, obj.PrevValue, obj.PrevSeq)
	}
	if obj.Value != "v2" || obj.Seq != seq2 {
		t.Fatalf("expected current state to be (v2, seq=%d), got Value=%v Seq=%d", seq2, obj.Value, obj.Seq)
	}
}

func TestRevertIfSeqMatches(t *testing.T) {
	obj := newTestObject(5, 3)

	seq1 := obj.NextSeq()
	obj.ApplyFastProvisional(1, "v1", seq1)

	seq2 := obj.NextSeq()
	obj.ApplyFastProvisional(2, "v2", seq2)

	// A revert for a superseded seq (seq1) must be a no-op.
	if obj.RevertIfSeqMatches(seq1) {
		t.Fatalf("revert for superseded seq=%d should have been a no-op", seq1)
	}
	if obj.Value != "v2" || obj.Seq != seq2 {
		t.Fatalf("no-op revert must not mutate state, got Value=%v Seq=%d", obj.Value, obj.Seq)
	}

	// A revert for the current seq (seq2) must restore the prior state (v1, seq1).
	if !obj.RevertIfSeqMatches(seq2) {
		t.Fatalf("expected revert for current seq=%d to succeed", seq2)
	}
	if obj.Value != "v1" || obj.Seq != seq1 {
		t.Fatalf("expected revert to restore (v1, seq=%d), got Value=%v Seq=%d", seq1, obj.Value, obj.Seq)
	}
}

// ---------------- Feature 1: ReassignWeights ----------------

func TestReassignWeightsCoordinatorGetsMaxWeight(t *testing.T) {
	const numReplicas = 5
	obj := newTestObject(numReplicas, 3)

	coordinatorID := 3
	arrivalOrder := []int{1, 0, 4} // some subset responded, in this order

	obj.ReassignWeights(arrivalOrder, coordinatorID, numReplicas)

	cache := obj.GetWeightCache()
	maxWeight := 0.0
	for _, w := range cache {
		if w > maxWeight {
			maxWeight = w
		}
	}
	if cache[coordinatorID] != maxWeight {
		t.Fatalf("expected coordinator %d to hold the max weight %.4f, got %.4f",
			coordinatorID, maxWeight, cache[coordinatorID])
	}
}

func TestReassignWeightsPreservesValueMultisetAndThreshold(t *testing.T) {
	const numReplicas = 5
	const quorumSize = 3
	obj := newTestObject(numReplicas, quorumSize)

	before := make([]float64, numReplicas)
	copy(before, obj.GetWeightCache())
	sort.Float64s(before)
	thresholdBefore := obj.ThresholdFast

	obj.ReassignWeights([]int{2, 4, 1}, 0, numReplicas)

	after := make([]float64, numReplicas)
	copy(after, obj.GetWeightCache())
	sort.Float64s(after)

	if len(before) != len(after) {
		t.Fatalf("weight count changed: before=%d after=%d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("weight value multiset changed at sorted index %d: before=%.6f after=%.6f",
				i, before[i], after[i])
		}
	}
	if obj.ThresholdFast != thresholdBefore {
		t.Fatalf("ReassignWeights must not change ThresholdFast: before=%.6f after=%.6f",
			thresholdBefore, obj.ThresholdFast)
	}
}

func TestReassignWeightsDeterministicForUnresponded(t *testing.T) {
	const numReplicas = 4
	obj := newTestObject(numReplicas, 2)

	// No one but the coordinator responded - remaining replicas must still
	// get the leftover weights in sorted-ID order, deterministically.
	obj.ReassignWeights(nil, 0, numReplicas)

	cache := obj.GetWeightCache()
	if cache[1] < cache[2] || cache[2] < cache[3] {
		t.Fatalf("expected unresponded replicas 1,2,3 to receive descending weights in sorted-ID order, got %v",
			cache)
	}
}

// TestWeightCacheConcurrentAccess reproduces the shape of consensus.go's
// fast-path broadcast (many goroutines calling GetWeightCache() without a
// lock, mirroring "lock-free access" on the hot path) racing against
// ReassignWeights (called from the async weight-reassign worker after every
// commit). Before weightCache became an atomic.Pointer, this exact pattern
// was a genuine data race: WeightCache was a plain slice field, written
// under o.Lock() by ReassignWeights but read with no synchronization at all
// in consensus.go - run with `go test -race` to confirm this is now clean.
func TestWeightCacheConcurrentAccess(t *testing.T) {
	const numReplicas = 7
	obj := newTestObject(numReplicas, 4)

	var readersWg, writerWg sync.WaitGroup
	stop := make(chan struct{})

	// Readers: hammer GetWeightCache() the same way the fast-path broadcast
	// loop does, once per (simulated) connection, every round. Run until
	// told to stop by the writer finishing its bounded loop below.
	for i := 0; i < 8; i++ {
		readersWg.Add(1)
		go func(replicaID int) {
			defer readersWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				cache := obj.GetWeightCache()
				if len(cache) != numReplicas {
					t.Errorf("torn/short read: got len=%d, want %d", len(cache), numReplicas)
					return
				}
				_ = cache[replicaID%numReplicas]
			}
		}(i)
	}

	// Writer: reassign weights repeatedly, as the async worker does after
	// every successful fast-path commit. Bounded, so it always terminates.
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for i := 0; i < 2000; i++ {
			obj.ReassignWeights([]int{i % numReplicas, (i + 1) % numReplicas}, i%numReplicas, numReplicas)
		}
	}()

	writerWg.Wait() // writer's loop is bounded, so this always returns
	close(stop)     // now tell the (unbounded) readers to stop
	readersWg.Wait()
}
