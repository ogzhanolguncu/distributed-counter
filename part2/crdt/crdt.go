package crdt

import (
	"maps"
	"sync"
)

type (
	// PNMap stores node IDs and their corresponding counts.
	PNMap map[string]uint64

	// PNCounter (Positive-Negative Counter) using separate RWMutexes
	// for increments and decrements to potentially increase concurrency
	// between increment and decrement operations.
	PNCounter struct {
		incMu      sync.RWMutex
		decMu      sync.RWMutex
		increments PNMap
		decrements PNMap
	}
)

// New creates a new PNCounter with the provided node ID,
// initializing separate maps and mutexes.
func New(nodeId string) *PNCounter {
	// Initialize maps directly
	incMap := make(PNMap)
	incMap[nodeId] = 0

	decMap := make(PNMap)
	decMap[nodeId] = 0

	return &PNCounter{
		increments: incMap,
		decrements: decMap,
	}
}

// Value returns the current value of the counter (increments - decrements).
// It acquires read locks on both increment and decrement maps.
func (p *PNCounter) Value() int64 {
	// Acquire locks in a consistent order (inc then dec)
	p.incMu.RLock()
	defer p.incMu.RUnlock() // Ensure inc unlock happens

	p.decMu.RLock()
	defer p.decMu.RUnlock() // Ensure dec unlock happens

	var incSum, decSum uint64
	for _, v := range p.increments {
		incSum += v
	}
	for _, v := range p.decrements {
		decSum += v
	}

	return int64(incSum) - int64(decSum)
}

// LocalValue returns the net value for a specific node.
// It acquires read locks on both increment and decrement maps.
func (p *PNCounter) LocalValue(nodeId string) int64 {
	// Acquire locks in a consistent order
	p.incMu.RLock()
	defer p.incMu.RUnlock()

	p.decMu.RLock()
	defer p.decMu.RUnlock()

	var incVal, decVal uint64
	// Direct access within the locks
	if val, ok := p.increments[nodeId]; ok {
		incVal = val
	}
	if val, ok := p.decrements[nodeId]; ok {
		decVal = val
	}

	return int64(incVal) - int64(decVal)
}

// Increment increments the counter for the specified node.
// It acquires a write lock only on the increments map.
func (p *PNCounter) Increment(nodeId string) {
	p.incMu.Lock() // Acquire increments write lock
	defer p.incMu.Unlock()

	// Direct modification within the lock
	if _, exists := p.increments[nodeId]; !exists {
		p.increments[nodeId] = 0 // Initialize if node is new
	}
	p.increments[nodeId]++
}

// Decrement increments the decrement counter for the specified node.
// It acquires a write lock only on the decrements map.
func (p *PNCounter) Decrement(nodeId string) {
	p.decMu.Lock() // Acquire decrements write lock
	defer p.decMu.Unlock()

	// Direct modification within the lock
	if _, exists := p.decrements[nodeId]; !exists {
		p.decrements[nodeId] = 0 // Initialize if node is new
	}
	p.decrements[nodeId]++ // PNCounter increments the *decrement* map
}

// Counters returns copies of the increment and decrement counter maps.
// It acquires read locks on both maps.
func (p *PNCounter) Counters() (PNMap, PNMap) {
	// Acquire locks in a consistent order
	p.incMu.RLock()
	defer p.incMu.RUnlock()

	p.decMu.RLock()
	defer p.decMu.RUnlock()

	// Create independent copies while holding the locks
	incCopy := make(PNMap, len(p.increments))
	decCopy := make(PNMap, len(p.decrements))

	maps.Copy(incCopy, p.increments)
	maps.Copy(decCopy, p.decrements)

	return incCopy, decCopy
}

// MergeIncrements merges external increment values with the local counter.
// It acquires a write lock only on the increments map.
func (p *PNCounter) MergeIncrements(other PNMap) bool {
	p.incMu.Lock() // Acquire increments write lock
	defer p.incMu.Unlock()

	updated := false
	for nodeID, otherValue := range other {
		currentValue, exists := p.increments[nodeID]
		// Merge condition: take the max value
		if (!exists && otherValue > 0) || otherValue > currentValue {
			p.increments[nodeID] = otherValue
			updated = true
		}
	}
	return updated
}

// MergeDecrements merges external decrement values with the local counter.
// It acquires a write lock only on the decrements map.
func (p *PNCounter) MergeDecrements(other PNMap) bool {
	p.decMu.Lock() // Acquire decrements write lock
	defer p.decMu.Unlock()

	updated := false
	for nodeID, otherValue := range other {
		currentValue, exists := p.decrements[nodeID]
		// Merge condition: take the max value
		if (!exists && otherValue > 0) || otherValue > currentValue {
			p.decrements[nodeID] = otherValue
			updated = true
		}
	}
	return updated
}

// Merge combines this counter with another counter's state.
// It acquires write locks on the local counter via MergeIncrements/MergeDecrements
// and read locks on the other counter via its Counters() method.
func (p *PNCounter) Merge(other *PNCounter) bool {
	// Get copies of the other counter's state (acquires RLock on 'other' maps)
	otherIncrements, otherDecrements := other.Counters()

	// Merge into the local counter (acquires Lock on 'p' maps respectively)
	incUpdated := p.MergeIncrements(otherIncrements)
	decUpdated := p.MergeDecrements(otherDecrements)

	return incUpdated || decUpdated
}
