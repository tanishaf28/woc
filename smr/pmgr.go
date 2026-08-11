package smr

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

type serverID = int
type prioClock = int
type priority = float64

type PriorityManager struct {
	sync.RWMutex
	m        map[prioClock]map[serverID]priority
	scheme   []priority
	majority float64
	n        int
	q        int
}

func (pm *PriorityManager) Init(numOfServers, quorumSize, baseOfPriorities int, ratioTryStep float64, isCab bool) {
	pm.n = numOfServers
	pm.q = quorumSize // quorum size is t+1
	pm.m = make(map[prioClock]map[serverID]priority)

	ratio := 1.0
	if isCab {
		var err error
		ratio, err = calcInitPrioRatio(numOfServers, quorumSize, ratioTryStep)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("ratio: ", ratio)

	newPriorities := make(map[serverID]priority)

	for i := 0; i < numOfServers; i++ {
		p := float64(baseOfPriorities) * math.Pow(ratio, float64(i))
		newPriorities[numOfServers-1-i] = p
		pm.scheme = append(pm.scheme, p)
	}

	reverseSlice(pm.scheme)

	pm.majority = sum(pm.scheme) / 2

	pm.Lock()
	pm.m[0] = newPriorities
	pm.Unlock()
	return
}

func (pm *PriorityManager) SetNewPrioritiesUnderNewT(n, q, baseOfPriorities int, ratioTryStep float64, pClock prioClock) (newPriorities map[serverID]priority) {
	pm.n = n
	pm.q = q // quorum size is t+1
	pm.m = make(map[prioClock]map[serverID]priority)

	ratio := 1.0
	var err error
	ratio, err = calcInitPrioRatio(n, q, ratioTryStep)
	if err != nil {
		panic(err)
	}

	fmt.Println("ratio: ", ratio)

	newPriorities = make(map[serverID]priority)

	// reset pm scheme
	pm.scheme = []priority{}

	for i := 0; i < n; i++ {
		p := float64(baseOfPriorities) * math.Pow(ratio, float64(i))
		newPriorities[n-1-i] = p
		pm.scheme = append(pm.scheme, p)
	}

	fmt.Printf("pm.scheme length: %v\n", len(pm.scheme))
	reverseSlice(pm.scheme)

	pm.majority = sum(pm.scheme) / 2

	pm.Lock()
	pm.m[pClock] = newPriorities
	pm.Unlock()
	return
}

func reverseSlice(slice []priority) {
	length := len(slice)
	for i := 0; i < length/2; i++ {
		j := length - 1 - i
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func calcInitPrioRatio(n, f int, ratioTryStep float64) (float64, error) {
	r := 2.0 // initial guess
	for r > 1.0 { // Bounded loop
		if math.Pow(r, float64(n-f+1)) > 0.5*(math.Pow(r, float64(n))+1) && 0.5*(math.Pow(r, float64(n))+1) > math.Pow(r, float64(n-f)) {
			return r, nil
		}
		r -= ratioTryStep
	}
	return 0, fmt.Errorf("no valid ratio found for n=%d, f=%d", n, f)
}

func sum(arr []float64) float64 {
	total := 0.0
	for _, val := range arr {
		total += val
	}
	return total
}

func (pm *PriorityManager) UpdateFollowerPriorities(pClock prioClock, prioQueue chan serverID, leaderID serverID) error {

	newPriorities := make(map[serverID]priority)
	arranged := make(map[serverID]bool)

	for i := 0; i < pm.n; i++ {
		arranged[i] = false
	}

	nr := len(prioQueue)

	// Scheme index: leader always gets highest priority (index 0)
	schemeIdx := 0
	if leaderID >= 0 {
		if schemeIdx >= len(pm.scheme) {
			return fmt.Errorf("priority assignment of [%v] exceeds pm scheme length [%v]", schemeIdx, len(pm.scheme))
		}
		newPriorities[leaderID] = pm.scheme[schemeIdx]
		arranged[leaderID] = true
		schemeIdx++
	}

	// Collect responded servers from priority queue
	for j := 0; j < nr; j++ {
		s := <-prioQueue
		if schemeIdx >= len(pm.scheme) {
			return fmt.Errorf("priority assignment of [%v] exceeds pm scheme length [%v]", schemeIdx, len(pm.scheme))
		}
		newPriorities[s] = pm.scheme[schemeIdx]
		arranged[s] = true
		schemeIdx++
	}

	// Collect unresponded followers
	unresponded := make([]int, 0, pm.n)
	for id, done := range arranged {
		if !done {
			unresponded = append(unresponded, id)
		}
	}

	// Sort for deterministic assignment
	sort.Ints(unresponded)

	// Assign remaining followers in sorted order
	for _, id := range unresponded {
		if schemeIdx >= len(pm.scheme) {
			return fmt.Errorf("priority assignment of [%v] exceeds pm scheme length [%v]", schemeIdx, len(pm.scheme))
		}
		newPriorities[id] = pm.scheme[schemeIdx]
		schemeIdx++
	}

	pm.Lock()
	pm.m[pClock] = newPriorities
	// Prune old entries to prevent unbounded memory growth
	if pClock > 10 {
		delete(pm.m, pClock-10)
	}
	pm.Unlock()
	//fmt.Printf("newPriorities: %+v\n", newPriorities)
	return nil
}

func (pm *PriorityManager) GetFollowerPriorities(pClock int) (fpriorities map[serverID]priority) {
	fpriorities = make(map[serverID]priority)

	pm.RLock()
	defer pm.RUnlock()

	// Deep copy to prevent data races
	if src, ok := pm.m[pClock]; ok {
		for sid, prio := range src {
			fpriorities[sid] = prio
		}
	}
	return
}

func (pm *PriorityManager) GetMajority() (majority float64) {
	majority = pm.majority
	return
}

// GetMajorityExcluding is GetMajority, but with excludeIDs' priorities at
// pClock removed from the total voting weight first. Used during a leader
// election that already knows some replicas are dead (see
// ConsensusManager.StartElection): without this, the quorum threshold stays
// calibrated for the full N-replica weight total, and since the highest
// priority in this exponential scheme is normally held by the leader, a
// crashed leader can take enough of that weight with it that the survivors'
// combined priority can never exceed the un-adjusted threshold - election
// then fails forever instead of just excluding the dead replica's vote.
func (pm *PriorityManager) GetMajorityExcluding(pClock int, excludeIDs []int) float64 {
	pm.RLock()
	defer pm.RUnlock()

	total := sum(pm.scheme)
	if prios, ok := pm.m[pClock]; ok {
		for _, id := range excludeIDs {
			if p, exists := prios[id]; exists {
				total -= p
			}
		}
	}
	return total / 2
}

func (pm *PriorityManager) GetPriorityScheme() (scheme []priority) {
	scheme = pm.scheme
	return
}

func (pm *PriorityManager) GetQuorumSize() (q int) {
	q = pm.q
	return
}
