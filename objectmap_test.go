package main

import (
	"fmt"
	"testing"
)

var ringTestClusterSizes = []int{3, 5, 7, 11}

func testObjectIDs(n int) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("obj-%d", i)
	}
	return ids
}

// TestHashRingOwnerValidRange checks every object resolves to an in-range
// owner (0..n-1) for each target cluster size, over the user's 1000-object
// pool size.
func TestHashRingOwnerValidRange(t *testing.T) {
	ids := testObjectIDs(1000)
	for _, n := range ringTestClusterSizes {
		ring := NewHashRing(n)
		for _, id := range ids {
			owner := ring.Owner(id)
			if owner < 0 || owner >= n {
				t.Fatalf("n=%d: object %s got out-of-range owner %d", n, id, owner)
			}
		}
	}
}

// TestHashRingDeterministic checks the ring gives the same answer every time
// it's rebuilt for the same n - every replica must independently compute an
// identical ownership mapping without coordination (see ObjectOwner's real
// key fallback path, which relies on exactly this).
func TestHashRingDeterministic(t *testing.T) {
	ids := testObjectIDs(1000)
	for _, n := range ringTestClusterSizes {
		r1 := NewHashRing(n)
		r2 := NewHashRing(n)
		for _, id := range ids {
			if r1.Owner(id) != r2.Owner(id) {
				t.Fatalf("n=%d: ring not deterministic for object %s: %d vs %d", n, id, r1.Owner(id), r2.Owner(id))
			}
		}
	}
}

// TestHashRingNoCollisionLoss checks that virtual-node placement didn't lose
// entries to crc32 collisions (two different replica/vnode keys landing on
// the exact same ring position). At 100 vnodes/replica this is expected to
// be rare but not impossible - report it if it ever happens instead of
// letting it silently reduce a replica's effective share of the ring.
func TestHashRingNoCollisionLoss(t *testing.T) {
	for _, n := range ringTestClusterSizes {
		ring := NewHashRing(n)
		expected := n * virtualNodesPerReplica
		if len(ring.positions) != expected {
			t.Errorf("n=%d: expected %d ring positions, got %d", n, expected, len(ring.positions))
		}
		distinct := map[uint32]bool{}
		for _, p := range ring.positions {
			distinct[p] = true
		}
		if len(distinct) != expected {
			t.Logf("n=%d: %d/%d vnode positions are distinct (%d collisions) - owners map keeps only the last writer per collided position",
				n, len(distinct), expected, expected-len(distinct))
		}
	}
}

// TestHashRingDistribution reports (and flags if badly skewed) how evenly
// 1000 objects spread across replicas for each target cluster size.
func TestHashRingDistribution(t *testing.T) {
	const numObjs = 1000
	ids := testObjectIDs(numObjs)
	for _, n := range ringTestClusterSizes {
		ring := NewHashRing(n)
		counts := make([]int, n)
		for _, id := range ids {
			counts[ring.Owner(id)]++
		}
		ideal := float64(numObjs) / float64(n)
		minC, maxC := counts[0], counts[0]
		for _, c := range counts {
			if c < minC {
				minC = c
			}
			if c > maxC {
				maxC = c
			}
		}
		t.Logf("n=%d: counts=%v ideal=%.1f min=%d max=%d (min/ideal=%.2f max/ideal=%.2f)",
			n, counts, ideal, minC, maxC, float64(minC)/ideal, float64(maxC)/ideal)

		for id, c := range counts {
			if float64(c) < ideal*0.4 {
				t.Errorf("n=%d: replica %d got only %d/%d objects (ideal %.1f) - too skewed low", n, id, c, numObjs, ideal)
			}
			if float64(c) > ideal*2.5 {
				t.Errorf("n=%d: replica %d got %d/%d objects (ideal %.1f) - too skewed high", n, id, c, numObjs, ideal)
			}
		}
	}
}

// TestHashRingOwnerExcludingFailsOver checks that marking an object's
// primary owner dead always hands it to a different, live replica - never
// the dead one, never -1 (as long as at least one replica is alive).
func TestHashRingOwnerExcludingFailsOver(t *testing.T) {
	ids := testObjectIDs(1000)
	for _, n := range ringTestClusterSizes {
		ring := NewHashRing(n)
		for _, id := range ids {
			primary := ring.Owner(id)
			dead := map[int]bool{primary: true}
			backup := ring.OwnerExcluding(id, dead)
			if backup < 0 {
				t.Fatalf("n=%d: object %s has no owner when only primary %d is dead", n, id, primary)
			}
			if backup == primary {
				t.Fatalf("n=%d: object %s: OwnerExcluding returned the dead primary %d", n, id, primary)
			}
			if dead[backup] {
				t.Fatalf("n=%d: object %s: OwnerExcluding returned dead replica %d", n, id, backup)
			}
			if backup >= n {
				t.Fatalf("n=%d: object %s: OwnerExcluding returned out-of-range replica %d", n, id, backup)
			}
		}
	}
}

// TestHashRingOwnerExcludingAllDead checks the documented -1 sentinel when
// every replica is marked dead.
func TestHashRingOwnerExcludingAllDead(t *testing.T) {
	for _, n := range ringTestClusterSizes {
		ring := NewHashRing(n)
		dead := make(map[int]bool, n)
		for i := 0; i < n; i++ {
			dead[i] = true
		}
		if owner := ring.OwnerExcluding("obj-0", dead); owner != -1 {
			t.Errorf("n=%d: expected -1 when all replicas dead, got %d", n, owner)
		}
	}
}

// TestBuildOwnershipRingIntegration exercises the real pipeline the paper's
// §4.2 object mapping describes: InitObjectRegistry's static 1000-object
// pool, fed through BuildOwnershipRing for each target cluster size.
func TestBuildOwnershipRingIntegration(t *testing.T) {
	numObjects = 1000
	indepRatio = 90
	InitObjectRegistry()

	if len(objectByIndex) != 1000 {
		t.Fatalf("expected 1000 objects in registry, got %d", len(objectByIndex))
	}

	for _, n := range ringTestClusterSizes {
		owners := BuildOwnershipRing(n, nil)
		if len(owners) != numObjects {
			t.Fatalf("n=%d: expected %d owners, got %d", n, numObjects, len(owners))
		}
		for i, o := range owners {
			if o < 0 || o >= n {
				t.Fatalf("n=%d: object index %d got invalid owner %d", n, i, o)
			}
		}

		// A dead replica must never come back as an owner.
		dead := map[int]bool{0: true}
		ownersExcl := BuildOwnershipRing(n, dead)
		for i, o := range ownersExcl {
			if o == 0 {
				t.Fatalf("n=%d: object index %d assigned to dead replica 0 (dead-exclusion not applied)", n, i)
			}
		}
	}
}
