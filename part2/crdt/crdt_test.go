package crdt

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPNCounter_MultiNode(t *testing.T) {
	counter := New("node1")

	counter.Increment("node1")
	counter.Increment("node2")
	counter.Increment("node3")

	require.Equal(t, int64(3), counter.Value(), "Value should be 3 after increments from 3 nodes")

	counter.Decrement("node1")
	counter.Decrement("node2")

	require.Equal(t, int64(1), counter.Value(), "Value should be 1 after 2 decrements")

	require.Equal(t, int64(0), counter.LocalValue("node1"), "Node1 local value should be 0")
	require.Equal(t, int64(0), counter.LocalValue("node2"), "Node2 local value should be 0")
	require.Equal(t, int64(1), counter.LocalValue("node3"), "Node3 local value should be 1")
}

func TestPNCounter_Merge(t *testing.T) {
	counter1 := New("node1")
	counter2 := New("node2")

	counter1.Increment("node1")
	counter1.Increment("node1")
	counter2.Increment("node2")
	counter2.Decrement("node3")

	require.Equal(t, int64(2), counter1.Value(), "Counter1 should be 2")
	require.Equal(t, int64(0), counter2.Value(), "Counter2 should be 0")

	updated := counter1.Merge(counter2)
	require.True(t, updated, "Merge should return true when values are updated")

	require.Equal(t, int64(2), counter1.Value(), "Counter1 value should be 2 after merge")

	require.Equal(t, int64(2), counter1.LocalValue("node1"), "node1 local value should be 2")
	require.Equal(t, int64(1), counter1.LocalValue("node2"), "node2 local value should be 1")
	require.Equal(t, int64(-1), counter1.LocalValue("node3"), "node3 local value should be -1")
}

func TestPNCounter_Commutativity(t *testing.T) {
	counter1 := New("node1")
	counter2 := New("node2")

	counter1.Increment("node1")
	counter1.Increment("shared")
	counter2.Increment("node2")
	counter2.Increment("shared")
	counter2.Increment("shared")

	counterA := New("nodeA")
	counterB := New("nodeB")

	counterA.Merge(counter1)
	counterA.Merge(counter2)

	counterB.Merge(counter2)
	counterB.Merge(counter1)

	require.Equal(t, counterA.Value(), counterB.Value(), "Merge should be commutative")

	incA, decA := counterA.Counters()
	incB, decB := counterB.Counters()

	relevantKeys := []string{"node1", "node2", "shared"}

	for _, key := range relevantKeys {
		require.Equal(t, incA[key], incB[key], "Increment for %s should be identical", key)
		require.Equal(t, decA[key], decB[key], "Decrement for %s should be identical", key)
	}
}

func TestPNCounter_ConcurrentOperations(t *testing.T) {
	counter := New("shared")

	// Number of concurrent operations
	concurrency := 100

	var wg sync.WaitGroup
	wg.Add(concurrency * 2) // For both increments and decrements

	// Concurrent increments
	for i := range concurrency {
		go func(id int) {
			defer wg.Done()
			counter.Increment("node-inc")
		}(i)
	}

	// Concurrent decrements
	for i := range concurrency {
		go func(id int) {
			defer wg.Done()
			counter.Decrement("node-dec")
		}(i)
	}

	wg.Wait()

	// Check the final state
	require.Equal(t, int64(0), counter.Value(), "Value should be 0 after equal increments and decrements")
	require.Equal(t, int64(concurrency), counter.LocalValue("node-inc"), "node-inc should have value equal to concurrency")
	require.Equal(t, int64(-concurrency), counter.LocalValue("node-dec"), "node-dec should have value equal to -concurrency")
}

func TestPNCounter_ConcurrentMerges(t *testing.T) {
	// Test concurrent merges
	main := New("main")

	// Create several counters with different operations
	sources := make([]*PNCounter, 10)
	for i := 0; i < 10; i++ {
		sources[i] = New(fmt.Sprintf("source-%d", i))
		nodeID := fmt.Sprintf("node-%d", i)

		for j := 0; j < i+1; j++ {
			sources[i].Increment(nodeID)
		}

		if i%2 == 0 {
			for j := 0; j < i/2; j++ {
				sources[i].Decrement(fmt.Sprintf("dec-node-%d", i))
			}
		}
	}

	edgeCase := New("edge-case")
	largeIncMap := make(PNMap)
	largeIncMap["large-inc"] = uint64(1 << 32) // A large but not overflow-causing value
	edgeCase.MergeIncrements(largeIncMap)

	largeDecMap := make(PNMap)
	largeDecMap["large-dec"] = uint64(1 << 31) // Half the increment value
	edgeCase.MergeDecrements(largeDecMap)

	// Add the edge case to our sources
	sources = append(sources, edgeCase)

	// Merge concurrently
	var wg sync.WaitGroup
	wg.Add(len(sources))
	for _, src := range sources {
		go func(source *PNCounter) {
			defer wg.Done()
			// Try multiple merges to increase contention
			for i := 0; i < 3; i++ {
				main.Merge(source)
			}
		}(src)
	}
	wg.Wait()

	// Calculate expected value
	expectedInc := int64(0)
	for i := 0; i < 10; i++ {
		expectedInc += int64(i + 1) // Each source has i+1 increments
	}
	expectedInc += int64(1 << 32) // Add large edge case

	expectedDec := int64(0)
	for i := 0; i < 10; i += 2 { // Even numbered sources
		expectedDec += int64(i / 2) // Each even source has i/2 decrements
	}
	expectedDec += int64(1 << 31) // Add large edge case decrements

	expectedValue := expectedInc - expectedDec
	require.Equal(t, expectedValue, main.Value(),
		"Value should reflect all increments and decrements after concurrent merges")
}
