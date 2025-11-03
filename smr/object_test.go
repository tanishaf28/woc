package smr

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TestObjectWeightDistribution verifies per-object weight generation and thresholds.
func TestObjectWeightDistribution(t *testing.T) {
	numReplicas := 5
	numObjects := 8

	s := NewServerState()

	fmt.Println("=== Weighted Object Consensus Test ===")
	for i := 0; i < numObjects; i++ {
		objID := fmt.Sprintf("obj-%d", i)
		s.AddObject(objID, 0, numReplicas)
		obj := s.GetObject(objID)
		if obj == nil {
			t.Fatalf("object %s not registered", objID)
		}

		// Validate weight distribution
		if len(obj.Weights) != numReplicas {
			t.Fatalf("object %s has wrong number of weights: got %d, want %d",
				objID, len(obj.Weights), numReplicas)
		}

		total := 0.0
		for rid, w := range obj.Weights {
			if w <= 0 {
				t.Errorf("replica %d of %s has non-positive weight %f", rid, objID, w)
			}
			total += w
		}

		// Check threshold
		if obj.ThresholdFast <= 0 || obj.ThresholdFast > total {
			t.Errorf("object %s invalid fast threshold %.3f (total %.3f)",
				objID, obj.ThresholdFast, total)
		}

		fmt.Printf("\nObject: %s | R=%.3f | TotalW=%.3f | ThresholdFast=%.3f\n",
			obj.ID, obj.RValue, total, obj.ThresholdFast)

		for rid := 0; rid < numReplicas; rid++ {
			fmt.Printf("  Replica %d → w=%.4f\n", rid, obj.Weights[rid])
		}
	}
}

// TestFastQuorumValidation dynamically simulates random replies to check fast quorum.
func TestFastQuorumValidation(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	numReplicas := 5
	s := NewServerState()
	s.AddObject("testObj", 0, numReplicas)
	obj := s.GetObject("testObj")

	// Randomly select replies until threshold is reached
	ids := rand.Perm(numReplicas) // random order of replicas
	replies := make(map[int]float64)
	sum := 0.0
	reached := false

	for _, rid := range ids {
		replies[rid] = obj.Weights[rid]
		sum += obj.Weights[rid]
		if obj.HasFastQuorum(replies) {
			reached = true
			break
		}
	}

	if !reached {
		t.Errorf("fast quorum should have been reached with dynamic replies")
	} else {
		fmt.Printf("\n[Test] Fast quorum reached dynamically: total weight %.3f / threshold ≥ %.3f\n",
			sum, obj.ThresholdFast)
	}

	// Extra: test insufficient replies do not reach quorum
	replies = map[int]float64{ids[0]: obj.Weights[ids[0]]}
	if obj.HasFastQuorum(replies) {
		t.Errorf("fast quorum should NOT be reached with insufficient replies")
	}
}
