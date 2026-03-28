package smr

import (
	"errors"
	"sync"
)

type PriorityState struct {
	sync.RWMutex
	PrioClock int
	PrioVal   float64
	Majority  float64
}

func NewServerPriority(initPrioClock int, initPrioVal float64) PriorityState {
	return PriorityState{
		PrioClock: initPrioClock,
		PrioVal:   initPrioVal,
	}
}

func (p *PriorityState) UpdatePriority(newPClock int, newPriority float64) error {
	p.Lock()
	defer p.Unlock()

	if newPClock < 0 {
		return errors.New("newPClock must be non-negative")
	}

	if newPClock >= p.PrioClock {
		p.PrioClock = newPClock
		p.PrioVal = newPriority
	}

	return nil
}

func (p *PriorityState) GetPriority() (pClock int, pValue float64) {
	p.RLock()
	defer p.RUnlock()

	pClock = p.PrioClock
	pValue = p.PrioVal
	return
}

func (p *PriorityState) GetPrioVal() float64 {
	p.RLock()
	defer p.RUnlock()
	return p.PrioVal
}

func (p *PriorityState) SetMajority(m float64) {
	p.Lock()
	defer p.Unlock()
	p.Majority = m
}

func (p *PriorityState) GetMajority() float64 {
	p.RLock()
	defer p.RUnlock()
	return p.Majority
}
