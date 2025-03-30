package crdt

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

func BenchmarkIncrement(b *testing.B) {
	counter := New("node-1")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		counter.Increment("node-1")
	}
}

func BenchmarkDecrement(b *testing.B) {
	counter := New("node-1")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		counter.Decrement("node-1")
	}
}

func BenchmarkValue(b *testing.B) {
	counter := New("node-1")
	for range 100 {
		counter.Increment("node-1")
		counter.Decrement("node-2")
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = counter.Value()
	}
}

func BenchmarkMerge(b *testing.B) {
	c1 := New("node-1")
	c2 := New("node-2")

	for range 100 {
		c1.Increment("node-1")
		c2.Decrement("node-2")
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c1.Merge(c2)
	}
}

// Benchmark concurrent increments
func BenchmarkConcurrentIncrements(b *testing.B) {
	for _, numGoroutines := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("goroutines-%d", numGoroutines), func(b *testing.B) {
			counter := New("shared")

			// Reset the timer before the parallel section
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				nodeID := fmt.Sprintf("node-%d", numGoroutines)
				for pb.Next() {
					counter.Increment(nodeID)
				}
			})
		})
	}
}

// Benchmark memory usage with increasing number of nodes
func BenchmarkMemoryUsage(b *testing.B) {
	for _, numNodes := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes-%d", numNodes), func(b *testing.B) {
			// Force GC before measuring
			runtime.GC()

			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)

			// Create counters for each node
			counters := make([]*PNCounter, numNodes)
			for i := range numNodes {
				nodeID := fmt.Sprintf("node-%d", i)
				counters[i] = New(nodeID)

				// Add some operations to make it realistic
				counters[i].Increment(nodeID)
				counters[i].Decrement(nodeID)
			}

			// Force GC after creating counters
			runtime.GC()

			var m2 runtime.MemStats
			runtime.ReadMemStats(&m2)

			// Report memory usage as an alloc metric
			b.ReportMetric(float64(m2.TotalAlloc-m1.TotalAlloc)/float64(numNodes), "bytes/node")
		})
	}
}

// Benchmark for memory leak detection
func BenchmarkMemoryLeak(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping memory leak test in short mode")
	}

	counter := New("leak-test")

	// Force GC before starting
	runtime.GC()

	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Perform many operations
	for i := 0; i < b.N; i++ {
		nodeID := fmt.Sprintf("node-%d", i%100)
		if i%2 == 0 {
			counter.Increment(nodeID)
		} else {
			counter.Decrement(nodeID)
		}
	}

	// Force GC after operations
	runtime.GC()

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Report memory growth
	memGrowth := float64(m2.Alloc-m1.Alloc) / float64(b.N)
	b.ReportMetric(memGrowth, "bytes/op")
}

// Benchmark for concurrent operations with many nodes
func BenchmarkConcurrentManyNodes(b *testing.B) {
	for _, numNodes := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("nodes-%d", numNodes), func(b *testing.B) {
			var wg sync.WaitGroup
			counter := New("shared")

			// Reset timer before starting goroutines
			b.ResetTimer()

			for i := range numNodes {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					nodeID := strconv.Itoa(id)
					localCounter := New(nodeID)

					for j := range b.N / numNodes {
						if j%2 == 0 {
							localCounter.Increment(nodeID)
						} else {
							localCounter.Decrement(nodeID)
						}

						// Periodically merge
						if j%10 == 0 {
							localCounter.Merge(counter)
							counter.Merge(localCounter)
						}
					}
				}(i)
			}

			wg.Wait()
		})
	}
}

// Benchmark Copy-on-Write efficiency
func BenchmarkCopyOnWrite(b *testing.B) {
	counter := New("node-1")

	// Prepare with some initial data
	for i := range 1000 {
		nodeID := fmt.Sprintf("node-%d", i%10)
		counter.Increment(nodeID)
	}

	b.ResetTimer()

	// We'll measure how efficiently the copy-on-write works
	// by doing concurrent reads and writes
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine alternates between reads and writes
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				// Reading operation
				_ = counter.Value()
			} else {
				// Writing operation
				nodeID := fmt.Sprintf("node-%d", i%10)
				counter.Increment(nodeID)
			}
			i++
		}
	})
}

// Benchmark with larger state
func BenchmarkLargeState(b *testing.B) {
	// Create a counter with a large state (many nodes)
	counter := New("node-0")

	// Add data for many nodes
	for i := range 10000 {
		nodeID := fmt.Sprintf("node-%d", i)
		counter.Increment(nodeID)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Alternate between read and write operations
		if i%2 == 0 {
			_ = counter.Value()
		} else {
			nodeID := fmt.Sprintf("node-%d", i%100)
			counter.Increment(nodeID)
		}
	}
}
