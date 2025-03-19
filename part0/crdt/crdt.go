package crdt

import (
	"maps"
	"sync/atomic"
)

// PNCounter is a Positive-Negative Counter CRDT that supports both increments and decrements
// This is a more appropriate CRDT for operations that need both increments and decrements
type PNCounter struct {
	// Increments tracks the positive counter per node
	increments atomic.Pointer[map[string]uint64]
	// Decrements tracks the negative counter per node
	decrements atomic.Pointer[map[string]uint64]
}

// NewPNCounter creates a new PN-Counter for the given node ID
func NewPNCounter(nodeID string) *PNCounter {
	c := &PNCounter{}

	// Initialize increment counters
	initialIncrements := make(map[string]uint64)
	initialIncrements[nodeID] = 0
	c.increments.Store(&initialIncrements)

	// Initialize decrement counters
	initialDecrements := make(map[string]uint64)
	initialDecrements[nodeID] = 0
	c.decrements.Store(&initialDecrements)

	return c
}

// Value returns the current value of the counter (increments - decrements)
func (p *PNCounter) Value() int64 {
	increments := p.increments.Load()
	decrements := p.decrements.Load()

	var incSum, decSum uint64
	for _, v := range *increments {
		incSum += v
	}
	for _, v := range *decrements {
		decSum += v
	}

	return int64(incSum) - int64(decSum)
}

// LocalValue returns the net value for a specific node
func (p *PNCounter) LocalValue(nodeID string) int64 {
	increments := p.increments.Load()
	decrements := p.decrements.Load()

	incValue := uint64(0)
	if val, ok := (*increments)[nodeID]; ok {
		incValue = val
	}

	decValue := uint64(0)
	if val, ok := (*decrements)[nodeID]; ok {
		decValue = val
	}

	return int64(incValue) - int64(decValue)
}

// Increment atomically increments the counter for the specified node
func (p *PNCounter) Increment(nodeID string) int64 {
	for {
		currentIncrements := p.increments.Load()
		newIncrements := make(map[string]uint64, len(*currentIncrements))

		// Copy current counters
		maps.Copy(newIncrements, *currentIncrements)

		// Increment the specified node's counter
		if _, exists := newIncrements[nodeID]; !exists {
			newIncrements[nodeID] = 0
		}
		newIncrements[nodeID]++

		// Try to swap
		if p.increments.CompareAndSwap(currentIncrements, &newIncrements) {
			return p.Value()
		}
		// If unsuccessful, try again
	}
}

// Decrement atomically increments the decrement counter for the specified node
func (p *PNCounter) Decrement(nodeID string) int64 {
	for {
		currentDecrements := p.decrements.Load()
		newDecrements := make(map[string]uint64, len(*currentDecrements))

		// Copy current counters
		maps.Copy(newDecrements, *currentDecrements)

		// Increment the specified node's decrement counter
		if _, exists := newDecrements[nodeID]; !exists {
			newDecrements[nodeID] = 0
		}
		newDecrements[nodeID]++

		// Try to swap
		if p.decrements.CompareAndSwap(currentDecrements, &newDecrements) {
			return p.Value()
		}
		// If unsuccessful, try again
	}
}

// Counters returns copies of both the increment and decrement counter maps
func (p *PNCounter) Counters() (map[string]uint64, map[string]uint64) {
	increments := p.increments.Load()
	decrements := p.decrements.Load()

	incCopy := make(map[string]uint64, len(*increments))
	decCopy := make(map[string]uint64, len(*decrements))

	maps.Copy(incCopy, *increments)
	maps.Copy(decCopy, *decrements)

	return incCopy, decCopy
}

// MergeIncrements merges the increment counters
func (p *PNCounter) MergeIncrements(other map[string]uint64) bool {
	for {
		currentIncrements := p.increments.Load()
		newIncrements := make(map[string]uint64, len(*currentIncrements))

		// Copy current counters
		maps.Copy(newIncrements, *currentIncrements)

		// Track if any values were updated
		updated := false

		// Merge by taking max value for each node
		for nodeID, otherValue := range other {
			currentValue, exists := newIncrements[nodeID]
			if !exists || otherValue > currentValue {
				newIncrements[nodeID] = otherValue
				updated = true
			}
		}

		// If no updates needed, return false
		if !updated {
			return false
		}

		// Try to swap
		if p.increments.CompareAndSwap(currentIncrements, &newIncrements) {
			return true
		}
		// If unsuccessful, try again
	}
}

// MergeDecrements merges the decrement counters
func (p *PNCounter) MergeDecrements(other map[string]uint64) bool {
	for {
		currentDecrements := p.decrements.Load()
		newDecrements := make(map[string]uint64, len(*currentDecrements))

		// Copy current counters
		maps.Copy(newDecrements, *currentDecrements)

		// Track if any values were updated
		updated := false

		// Merge by taking max value for each node
		for nodeID, otherValue := range other {
			currentValue, exists := newDecrements[nodeID]
			if !exists || otherValue > currentValue {
				newDecrements[nodeID] = otherValue
				updated = true
			}
		}

		// If no updates needed, return false
		if !updated {
			return false
		}

		// Try to swap
		if p.decrements.CompareAndSwap(currentDecrements, &newDecrements) {
			return true
		}
		// If unsuccessful, try again
	}
}

// Merge combines this counter with another
func (p *PNCounter) Merge(other *PNCounter) bool {
	otherIncrements, otherDecrements := other.Counters()
	incUpdated := p.MergeIncrements(otherIncrements)
	decUpdated := p.MergeDecrements(otherDecrements)
	return incUpdated || decUpdated
}
