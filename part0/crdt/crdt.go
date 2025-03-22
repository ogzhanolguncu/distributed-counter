package crdt

import (
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

const maxRetryCount = 10

type (
	PNMap map[string]uint64

	// PNCounter (Positive-Negative Counter) is a conflict-free replicated data type (CRDT)
	// that allows both incrementing and decrementing a counter in a distributed system.
	// Key CRDT properties:
	// - Convergence: All replicas will eventually reach the same state when they have
	//   processed the same updates, regardless of the order in which updates are received.
	// - Commutativity: The order of merge operations doesn't affect the final state.
	// - Associativity: Merging multiple counters can be done in any grouping order.
	//
	// Implementation details:
	// - Uses two G-Counters (grow-only counters): one for increments and one for decrements
	// - Each node maintains its own counter values in the respective maps
	// - The final value is computed as (sum of all increments) - (sum of all decrements)
	// - Concurrent operations are resolved by taking the maximum value for each node ID
	PNCounter struct {
		increments atomic.Pointer[PNMap]
		decrements atomic.Pointer[PNMap]

		incrementMapPool sync.Pool
		decrementMapPool sync.Pool
	}
)

func New(nodeId string) *PNCounter {
	c := &PNCounter{
		incrementMapPool: sync.Pool{New: func() any {
			m := make(map[string]uint64)
			return &m
		}},
		decrementMapPool: sync.Pool{New: func() any {
			m := make(map[string]uint64)
			return &m
		}},
	}

	initialIncrements := make(PNMap)
	initialIncrements[nodeId] = 0
	c.increments.Store(&initialIncrements)

	initialDecrements := make(PNMap)
	initialDecrements[nodeId] = 0
	c.decrements.Store(&initialDecrements)

	return c
}

// Returns the current of value of the counter (increments - decrements)
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

// Returns the net value for a specific node
func (p *PNCounter) LocalValue(nodeId string) int64 {
	increments := p.increments.Load()
	decrements := p.decrements.Load()

	var incSum, decSum uint64
	if val, ok := (*increments)[nodeId]; ok {
		incSum = val
	}

	if val, ok := (*decrements)[nodeId]; ok {
		decSum = val
	}

	return int64(incSum) - int64(decSum)
}

// Increments atomically increments the counter for the specified node
// The pattern creates a new map copy rather than modifying the existing one to prevent data corruption
// if the atomic swap fails, ensuring consistent state across concurrent operations.
// func (p *PNCounter) Increment(nodeId string) int64 {
// 	for {
// 		currentIncrements := p.increments.Load()
//
// 		// Create a new map with increment applied
// 		newIncrements := maps.Clone(*currentIncrements)
// 		newIncrements[nodeId]++
//
// 		// Try to swap
// 		if p.increments.CompareAndSwap(currentIncrements, &newIncrements) {
// 			return p.Value()
// 		}
// 		// Try until it's successfull
// 	}
// }

// Increments atomically increments the counter for the specified node
// The pattern creates a new map copy rather than modifying the existing one to prevent data corruption
// if the atomic swap fails, ensuring consistent state across concurrent operations.
func (p *PNCounter) Increment(nodeId string) int64 {
	retry := &Retry[int64]{
		MaxAttempts: maxRetryCount,
		Delay:       1 * time.Millisecond,
	}

	return retry.Do(func() RetryResult[int64] {
		currentIncrements, newIncrementsPtr, newIncrements := p.prepareMap(&p.increments, &p.incrementMapPool)
		if _, exists := newIncrements[nodeId]; !exists {
			newIncrements[nodeId] = 0
		}
		newIncrements[nodeId]++

		// Try to swap
		if p.increments.CompareAndSwap(currentIncrements, newIncrementsPtr) {
			// Put the old map back in the pool
			p.incrementMapPool.Put(currentIncrements)
			return RetryResult[int64]{Value: p.Value(), Done: true}
		}

		// If unsuccessful, put the unused map back in the pool
		p.incrementMapPool.Put(newIncrementsPtr)
		//  Return old Value if swap is unsuccessful
		return RetryResult[int64]{Value: p.Value(), Done: false}
	})
}

// Decrements atomically decrements the counter for the specified node
func (p *PNCounter) Decrement(nodeId string) int64 {
	retry := &Retry[int64]{
		MaxAttempts: maxRetryCount,
		Delay:       1 * time.Millisecond,
	}

	return retry.Do(func() RetryResult[int64] {
		currentDecrements, newDecrementsPtr, newDecrements := p.prepareMap(&p.decrements, &p.decrementMapPool)
		if _, exists := newDecrements[nodeId]; !exists {
			newDecrements[nodeId] = 0
		}
		newDecrements[nodeId]++

		// Try to swap
		if p.decrements.CompareAndSwap(currentDecrements, newDecrementsPtr) {
			// Put the old map back in the pool
			p.decrementMapPool.Put(currentDecrements)
			return RetryResult[int64]{Value: p.Value(), Done: true}
		}

		// If unsuccessful, put the unused map back in the pool
		p.decrementMapPool.Put(newDecrementsPtr)
		//  Return old Value if swap is unsuccessful
		return RetryResult[int64]{Value: p.Value(), Done: false}
	})
}

// Counters returns copies of the increment and decrement counter maps.
// The returned maps are independent copies of the internal state,
// so callers can safely modify them without affecting the CRDT.
// These maps contain the individual node contributions to the counter.
func (p *PNCounter) Counters() (PNMap, PNMap) {
	increments := *(p.increments.Load())
	decrements := *(p.decrements.Load())
	return increments, decrements
}

// MergeIncrements merges external increment values with the local counter.
// This follows the CRDT merge rule of taking the maximum value for each node.
// Returns true if any values were updated.
func (p *PNCounter) MergeIncrements(other PNMap) bool {
	retry := &Retry[bool]{
		MaxAttempts: maxRetryCount,
		Delay:       1 * time.Millisecond,
	}

	return retry.Do(func() RetryResult[bool] {
		currentIncrements, newIncrementsPtr, newIncrements := p.prepareMap(&p.increments, &p.incrementMapPool)
		updated := false
		// Merge by taking max value for each node
		for nodeID, otherValue := range other {
			currentValue, exists := newIncrements[nodeID]
			if !exists || otherValue > currentValue {
				newIncrements[nodeID] = otherValue
				updated = true
			}
		}

		if !updated {
			// If unsuccessful, put the unused map back in the pool
			p.incrementMapPool.Put(newIncrementsPtr)
			//  Return old Value if swap is unsuccessful
			return RetryResult[bool]{Value: false, Done: true}
		}

		// Try to swap
		if p.increments.CompareAndSwap(currentIncrements, newIncrementsPtr) {
			// Put the old map back in the pool
			p.incrementMapPool.Put(currentIncrements)
			return RetryResult[bool]{Value: true, Done: true}
		}

		// If unsuccessful, put the unused map back in the pool
		p.incrementMapPool.Put(newIncrementsPtr)
		//  Return old Value if swap is unsuccessful
		return RetryResult[bool]{Value: false, Done: false}
	})
}

// MergeDecrements merges external decrement values with the local counter.
// This follows the CRDT merge rule of taking the maximum value for each node.
// Returns true if any values were updated.
func (p *PNCounter) MergeDecrements(other PNMap) bool {
	retry := &Retry[bool]{
		MaxAttempts: maxRetryCount,
		Delay:       1 * time.Millisecond,
	}

	return retry.Do(func() RetryResult[bool] {
		currentDecrements, newDecrementsPtr, newDecrements := p.prepareMap(&p.decrements, &p.decrementMapPool)
		updated := false
		// Merge by taking max value for each node
		for nodeID, otherValue := range other {
			currentValue, exists := newDecrements[nodeID]
			if !exists || otherValue > currentValue {
				newDecrements[nodeID] = otherValue
				updated = true
			}
		}

		if !updated {
			// If unsuccessful, put the unused map back in the pool
			p.decrementMapPool.Put(newDecrementsPtr)
			//  Return old Value if swap is unsuccessful
			return RetryResult[bool]{Value: false, Done: true}
		}

		// Try to swap
		if p.decrements.CompareAndSwap(currentDecrements, newDecrementsPtr) {
			// Put the old map back in the pool
			p.decrementMapPool.Put(currentDecrements)
			return RetryResult[bool]{Value: true, Done: true}
		}

		// If unsuccessful, put the unused map back in the pool
		p.decrementMapPool.Put(newDecrementsPtr)
		//  Return old Value if swap is unsuccessful
		return RetryResult[bool]{Value: false, Done: false}
	})
}

// prepareMap is a generic helper that prepares a new map from a pool
// for safe atomic operations on a PNMap
func (p *PNCounter) prepareMap(current *atomic.Pointer[PNMap], pool *sync.Pool) (*PNMap, *PNMap, PNMap) {
	currentMap := current.Load()
	// Get a map from the pool
	newMapPtr := pool.Get().(*PNMap)
	newMap := *newMapPtr
	// Clear the map in case it was used before
	for k := range newMap {
		delete(newMap, k)
	}
	maps.Copy(newMap, *currentMap)
	return currentMap, newMapPtr, newMap
}

// Merge combines this counter with another
func (p *PNCounter) Merge(other *PNCounter) bool {
	otherIncrements, otherDecrements := other.Counters()
	incUpdated := p.MergeIncrements(otherIncrements)
	decUpdate := p.MergeDecrements(otherDecrements)
	return incUpdated || decUpdate
}
