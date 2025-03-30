package crdt

import (
	"maps"
	"sync/atomic"
	"time"
)

const maxRetryCount = 50

type (
	PNMap map[string]uint64

	// RefCountedMap wraps a PNMap with reference counting for Copy-on-Write operations
	RefCountedMap struct {
		data     PNMap
		refCount int32 // Using atomic operations for thread-safe reference counting
	}

	// PNCounter (Positive-Negative Counter) is a conflict-free replicated data type (CRDT)
	// that allows both incrementing and decrementing a counter in a distributed system.
	// This version uses Copy-on-Write with Reference Counting for improved performance.
	PNCounter struct {
		increments atomic.Pointer[RefCountedMap]
		decrements atomic.Pointer[RefCountedMap]
	}
)

// NewRefCountedMap creates a new map with an initial reference count of 1
func NewRefCountedMap() *RefCountedMap {
	return &RefCountedMap{
		data:     make(PNMap),
		refCount: 1, // Start with 1 reference
	}
}

// Acquire increments the reference count
func (r *RefCountedMap) Acquire() {
	atomic.AddInt32(&r.refCount, 1)
}

// Release decrements the reference count and returns true if the count reached zero
func (r *RefCountedMap) Release() bool {
	return atomic.AddInt32(&r.refCount, -1) == 0
}

// New creates a new PNCounter with the provided node ID
func New(nodeId string) *PNCounter {
	c := &PNCounter{}

	// Initialize increment map with node ID
	incMap := NewRefCountedMap()
	incMap.data[nodeId] = 0
	c.increments.Store(incMap)

	// Initialize decrement map with node ID
	decMap := NewRefCountedMap()
	decMap.data[nodeId] = 0
	c.decrements.Store(decMap)

	return c
}

// Value returns the current value of the counter (increments - decrements)
func (p *PNCounter) Value() int64 {
	increments := p.increments.Load()
	decrements := p.decrements.Load()

	var incSum, decSum uint64
	for _, v := range increments.data {
		incSum += v
	}
	for _, v := range decrements.data {
		decSum += v
	}

	return int64(incSum) - int64(decSum)
}

// LocalValue returns the net value for a specific node
func (p *PNCounter) LocalValue(nodeId string) int64 {
	increments := p.increments.Load()
	decrements := p.decrements.Load()

	var incSum, decSum uint64
	if val, ok := increments.data[nodeId]; ok {
		incSum = val
	}

	if val, ok := decrements.data[nodeId]; ok {
		decSum = val
	}

	return int64(incSum) - int64(decSum)
}

// Increment atomically increments the counter for the specified node
func (p *PNCounter) Increment(nodeId string) int64 {
	retry := &Retry[int64]{
		MaxAttempts: maxRetryCount,
		Delay:       1 * time.Millisecond,
	}

	return retry.Do(func() RetryResult[int64] {
		// Get the current ref-counted map
		currentMap := p.increments.Load()

		// Create a new map with the same data
		newMap := NewRefCountedMap()
		maps.Copy(newMap.data, currentMap.data)

		// Make our increment
		if _, exists := newMap.data[nodeId]; !exists {
			newMap.data[nodeId] = 0
		}
		newMap.data[nodeId]++

		// Try to swap atomically
		if p.increments.CompareAndSwap(currentMap, newMap) {
			// Successfully swapped, release old reference
			currentMap.Release()
			return RetryResult[int64]{Value: p.Value(), Done: true}
		}

		// Failed to swap, discard our new map
		return RetryResult[int64]{Value: p.Value(), Done: false}
	})
}

// Decrement atomically decrements the counter for the specified node
func (p *PNCounter) Decrement(nodeId string) int64 {
	retry := &Retry[int64]{
		MaxAttempts: maxRetryCount,
		Delay:       1 * time.Millisecond,
	}

	return retry.Do(func() RetryResult[int64] {
		// Get the current ref-counted map
		currentMap := p.decrements.Load()

		// Create a new map with the same data
		newMap := NewRefCountedMap()
		maps.Copy(newMap.data, currentMap.data)

		// Make our decrement
		if _, exists := newMap.data[nodeId]; !exists {
			newMap.data[nodeId] = 0
		}
		newMap.data[nodeId]++

		// Try to swap atomically
		if p.decrements.CompareAndSwap(currentMap, newMap) {
			// Successfully swapped, release old reference
			currentMap.Release()
			return RetryResult[int64]{Value: p.Value(), Done: true}
		}

		// Failed to swap, discard our new map
		return RetryResult[int64]{Value: p.Value(), Done: false}
	})
}

// Counters returns copies of the increment and decrement counter maps
func (p *PNCounter) Counters() (PNMap, PNMap) {
	increments := p.increments.Load().data
	decrements := p.decrements.Load().data

	// Create independent copies
	incCopy := make(PNMap, len(increments))
	decCopy := make(PNMap, len(decrements))

	maps.Copy(incCopy, increments)
	maps.Copy(decCopy, decrements)

	return incCopy, decCopy
}

// MergeIncrements merges external increment values with the local counter
func (p *PNCounter) MergeIncrements(other PNMap) bool {
	retry := &Retry[bool]{
		MaxAttempts: maxRetryCount,
		Delay:       1 * time.Millisecond,
	}

	return retry.Do(func() RetryResult[bool] {
		currentMap := p.increments.Load()

		// Check if there are any updates needed
		updated := false
		for nodeID, otherValue := range other {
			currentValue, exists := currentMap.data[nodeID]
			if (!exists && otherValue > 0) || otherValue > currentValue {
				updated = true
				break
			}
		}

		if !updated {
			return RetryResult[bool]{Value: false, Done: true}
		}

		// Create a new map with updates
		newMap := NewRefCountedMap()
		maps.Copy(newMap.data, currentMap.data)

		// Merge by taking max value for each node
		for nodeID, otherValue := range other {
			currentValue, exists := newMap.data[nodeID]
			if (!exists && otherValue > 0) || otherValue > currentValue {
				newMap.data[nodeID] = otherValue
			}
		}

		// Try to swap
		if p.increments.CompareAndSwap(currentMap, newMap) {
			currentMap.Release()
			return RetryResult[bool]{Value: true, Done: true}
		}

		// Failed to swap
		return RetryResult[bool]{Value: false, Done: false}
	})
}

// MergeDecrements merges external decrement values with the local counter
func (p *PNCounter) MergeDecrements(other PNMap) bool {
	retry := &Retry[bool]{
		MaxAttempts: maxRetryCount,
		Delay:       1 * time.Millisecond,
	}

	return retry.Do(func() RetryResult[bool] {
		currentMap := p.decrements.Load()

		// Check if there are any updates needed
		updated := false
		for nodeID, otherValue := range other {
			currentValue, exists := currentMap.data[nodeID]
			if (!exists && otherValue > 0) || otherValue > currentValue {
				updated = true
				break
			}
		}

		if !updated {
			return RetryResult[bool]{Value: false, Done: true}
		}

		// Create a new map with updates
		newMap := NewRefCountedMap()
		maps.Copy(newMap.data, currentMap.data)

		// Merge by taking max value for each node
		for nodeID, otherValue := range other {
			currentValue, exists := newMap.data[nodeID]
			if (!exists && otherValue > 0) || otherValue > currentValue {
				newMap.data[nodeID] = otherValue
			}
		}

		// Try to swap
		if p.decrements.CompareAndSwap(currentMap, newMap) {
			currentMap.Release()
			return RetryResult[bool]{Value: true, Done: true}
		}

		// Failed to swap
		return RetryResult[bool]{Value: false, Done: false}
	})
}

// Merge combines this counter with another
func (p *PNCounter) Merge(other *PNCounter) bool {
	otherIncrements, otherDecrements := other.Counters()
	incUpdated := p.MergeIncrements(otherIncrements)
	decUpdate := p.MergeDecrements(otherDecrements)
	return incUpdated || decUpdate
}
