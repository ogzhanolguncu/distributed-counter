package node

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type MemoryTransport struct {
	addr    string
	handler func(addr string, data []byte) error
	mu      sync.RWMutex
}

func NewMemoryTransport(addr string) *MemoryTransport {
	return &MemoryTransport{
		addr: addr,
	}
}

func (t *MemoryTransport) Send(addr string, data []byte) error {
	time.Sleep(10 * time.Millisecond) // Prevent message flood
	// Get the transport while holding the global lock
	tmu.RLock()
	transport, exists := transports[addr]
	tmu.RUnlock()
	if !exists {
		return fmt.Errorf("transport not found for address: %s", addr)
	}
	// Get the handler while holding the transport's lock
	transport.mu.RLock()
	handler := transport.handler
	transport.mu.RUnlock()
	if handler == nil {
		return fmt.Errorf("no handler registered for address: %s", addr)
	}
	// Call the handler outside of any locks
	return handler(t.addr, data)
}

func (t *MemoryTransport) Listen(handler func(addr string, data []byte) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = handler
	tmu.Lock()
	transports[t.addr] = t
	tmu.Unlock()
	return nil
}

func (t *MemoryTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	tmu.Lock()
	delete(transports, t.addr)
	tmu.Unlock()
	return nil
}

var (
	transports = make(map[string]*MemoryTransport)
	tmu        sync.RWMutex
)

// Updated to work with G-Counter CRDT implementation
func waitForConvergence(t *testing.T, nodes []*Node, expectedTotalCounter int64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allConverged := true
		for _, n := range nodes {
			// Get the total counter value
			totalCounter := n.GetCounter()
			if totalCounter != expectedTotalCounter {
				allConverged = false
				increments, decrements := n.counter.Counters()
				t.Logf("Node %s: total counter = %d (expected %d), inc=%v, dec=%v",
					n.GetAddr(), totalCounter, expectedTotalCounter, increments, decrements)
				break
			}
		}
		if allConverged {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Log detailed state information for debugging
	t.Log("Detailed node states at convergence failure:")
	for _, n := range nodes {
		increments, decrements := n.counter.Counters()
		t.Logf("Node %s: total counter = %d, inc=%v, dec=%v",
			n.GetAddr(), n.GetCounter(), increments, decrements)
	}

	t.Fatalf("nodes did not converge within timeout. Expected total counter = %d",
		expectedTotalCounter)
}

func createTestNode(t *testing.T, addr string, syncInterval time.Duration) *Node {
	transport := NewMemoryTransport(addr)
	config := Config{
		Addr:         addr,
		SyncInterval: syncInterval,
		MaxSyncPeers: 2,
	}
	node, err := NewNode(config, transport)
	require.NoError(t, err)
	return node
}

func TestConcurrentIncrement(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)
	node1.SetPeers([]string{"node2"})
	node2.SetPeers([]string{"node1"})

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	wg1.Add(1)
	go func() {
		defer wg1.Done()
		for i := 0; i < 100; i++ {
			node1.Increment()
			// Small sleep to prevent overloading
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		for i := 0; i < 100; i++ {
			node2.Increment()
			// Small sleep to prevent overloading
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg1.Wait()
	wg2.Wait()

	// Give nodes some time to sync before checking convergence
	time.Sleep(200 * time.Millisecond)

	// Check for convergence to total counter of 200 (100 from each node)
	waitForConvergence(t, []*Node{node1, node2}, 200, 5*time.Second)

	// Additional validation
	increments1, _ := node1.counter.Counters()
	increments2, _ := node2.counter.Counters()

	// Verify that each node has the correct counter values
	require.Equal(t, uint64(100), increments1["node1"], "Node1 should have 100 increments for itself")
	require.Equal(t, uint64(100), increments1["node2"], "Node1 should have 100 increments for node2")
	require.Equal(t, uint64(100), increments2["node1"], "Node2 should have 100 increments for node1")
	require.Equal(t, uint64(100), increments2["node2"], "Node2 should have 100 increments for itself")

	node1.Close()
	node2.Close()

	time.Sleep(500 * time.Millisecond)
}

// Test with three nodes to verify proper CRDT behavior
// func TestThreeNodeConcurrentIncrement(t *testing.T) {
// 	node1 := createTestNode(t, "node1", 100*time.Millisecond)
// 	node2 := createTestNode(t, "node2", 100*time.Millisecond)
// 	node3 := createTestNode(t, "node3", 100*time.Millisecond)
//
// 	// Set up the peer relationships (fully connected)
// 	node1.SetPeers([]string{"node2", "node3"})
// 	node2.SetPeers([]string{"node1", "node3"})
// 	node3.SetPeers([]string{"node1", "node2"})
//
// 	var wg sync.WaitGroup
// 	wg.Add(3)
//
// 	// Each node increments 50 times
// 	go func() {
// 		defer wg.Done()
// 		for i := 0; i < 50; i++ {
// 			node1.Increment()
// 			time.Sleep(1 * time.Millisecond)
// 		}
// 	}()
//
// 	go func() {
// 		defer wg.Done()
// 		for i := 0; i < 50; i++ {
// 			node2.Increment()
// 			time.Sleep(1 * time.Millisecond)
// 		}
// 	}()
//
// 	go func() {
// 		defer wg.Done()
// 		for i := 0; i < 50; i++ {
// 			node3.Increment()
// 			time.Sleep(1 * time.Millisecond)
// 		}
// 	}()
//
// 	wg.Wait()
//
// 	// Give nodes some time to sync before checking convergence
// 	time.Sleep(500 * time.Millisecond)
//
// 	// Check for convergence to total counter of 150 (50 from each node)
// 	waitForConvergence(t, []*Node{node1, node2, node3}, 150, 5*time.Second)
//
// 	// Additional validation
// 	for _, node := range []*Node{node1, node2, node3} {
// 		counters := node.counter.Counters()
// 		// Each node should have the correct view of all counters
// 		require.Equal(t, uint64(50), counters["node1"],
// 			"Node %s should see 50 increments for node1", node.GetAddr())
// 		require.Equal(t, uint64(50), counters["node2"],
// 			"Node %s should see 50 increments for node2", node.GetAddr())
// 		require.Equal(t, uint64(50), counters["node3"],
// 			"Node %s should see 50 increments for node3", node.GetAddr())
// 	}
//
// 	node1.Close()
// 	node2.Close()
// 	node3.Close()
//
// 	time.Sleep(500 * time.Millisecond)
// }

// Test decrement functionality
// Test negative counter values
// func TestNegativeCounter(t *testing.T) {
// 	node1 := createTestNode(t, "node1", 100*time.Millisecond)
// 	node2 := createTestNode(t, "node2", 100*time.Millisecond)
//
// 	node1.SetPeers([]string{"node2"})
// 	node2.SetPeers([]string{"node1"})
//
// 	// Decrement without incrementing first - should go negative
//
// 	// Now increment a few times
// 	for i := 0; i < 6; i++ {
// 		node2.Increment()
// 	}
//
// 	// Wait for sync
// 	time.Sleep(300 * time.Millisecond)
//
// 	// Verify both nodes see -5
// 	waitForConvergence(t, []*Node{node1, node2}, 6, 2*time.Second)
//
// 	// Wait for sync
// 	time.Sleep(300 * time.Millisecond)
//
// 	for i := 0; i < 5; i++ {
// 		node1.Decrement()
// 	}
// 	// Verify both nodes see -2 (-5 + 3)
// 	waitForConvergence(t, []*Node{node1, node2}, 1, 2*time.Second)
//
// 	// // Verify increments and decrements on both nodes
// 	// require.Equal(t, uint64(0), inc1["node1"], "Node1 should have 0 increments for itself")
// 	// require.Equal(t, uint64(5), dec1["node1"], "Node1 should have 5 decrements for itself")
// 	// require.Equal(t, uint64(3), inc1["node2"], "Node1 should have 3 increments for node2")
// 	// require.Equal(t, uint64(0), dec1["node2"], "Node1 should have 0 decrements for node2")
// 	//
// 	// require.Equal(t, uint64(0), inc2["node1"], "Node2 should have 0 increments for node1")
// 	// require.Equal(t, uint64(5), dec2["node1"], "Node2 should have 5 decrements for node1")
// 	// require.Equal(t, uint64(3), inc2["node2"], "Node2 should have 3 increments for itself")
// 	// require.Equal(t, uint64(0), dec2["node2"], "Node2 should have 0 decrements for itself")
// }

//
// // Test late joining node
// func TestLateJoiningNode(t *testing.T) {
// 	node1 := createTestNode(t, "node1", 100*time.Millisecond)
// 	node2 := createTestNode(t, "node2", 100*time.Millisecond)
//
// 	// Only node1 knows about node2 initially
// 	node1.SetPeers([]string{"node2"})
//
// 	// Increment node1 50 times
// 	for i := 0; i < 50; i++ {
// 		node1.Increment()
// 		time.Sleep(1 * time.Millisecond)
// 	}
//
// 	// Now let node2 know about node1 (late joining)
// 	node2.SetPeers([]string{"node1"})
//
// 	// Wait for sync
// 	time.Sleep(500 * time.Millisecond)
//
// 	// Verify both nodes converge to 50
// 	waitForConvergence(t, []*Node{node1, node2}, 50, 2*time.Second)
//
// 	// Now increment node2 30 times
// 	for i := 0; i < 30; i++ {
// 		node2.Increment()
// 		time.Sleep(1 * time.Millisecond)
// 	}
//
// 	// Wait for sync
// 	time.Sleep(300 * time.Millisecond)
//
// 	// Verify both nodes converge to 80 total
// 	waitForConvergence(t, []*Node{node1, node2}, 80, 2*time.Second)
//
// 	// Additional validation
// 	counters1 := node1.counter.Counters()
// 	counters2 := node2.counter.Counters()
//
// 	require.Equal(t, uint64(50), counters1["node1"], "Node1 should have 50 increments for itself")
// 	require.Equal(t, uint64(30), counters1["node2"], "Node1 should have 30 increments for node2")
// 	require.Equal(t, uint64(50), counters2["node1"], "Node2 should have 50 increments for node1")
// 	require.Equal(t, uint64(30), counters2["node2"], "Node2 should have 30 increments for itself")
// }
//
// // Test network partition and recovery
// func TestNetworkPartition(t *testing.T) {
// 	node1 := createTestNode(t, "node1", 100*time.Millisecond)
// 	node2 := createTestNode(t, "node2", 100*time.Millisecond)
// 	node3 := createTestNode(t, "node3", 100*time.Millisecond)
//
// 	// Set up the peer relationships
// 	node1.SetPeers([]string{"node2", "node3"})
// 	node2.SetPeers([]string{"node1", "node3"})
// 	node3.SetPeers([]string{"node1", "node2"})
//
// 	// Initial increments to verify connectivity
// 	node1.Increment()
// 	node2.Increment()
// 	node3.Increment()
//
// 	// Wait for sync
// 	time.Sleep(300 * time.Millisecond)
//
// 	// Verify all nodes converge to 3
// 	waitForConvergence(t, []*Node{node1, node2, node3}, 3, 2*time.Second)
//
// 	// Simulate network partition: node3 isolated
// 	node1.SetPeers([]string{"node2"})
// 	node2.SetPeers([]string{"node1"})
// 	// node3 still thinks it's connected but no one contacts it
//
// 	// Increments during partition
// 	for i := 0; i < 10; i++ {
// 		node1.Increment()
// 		node2.Increment()
// 		time.Sleep(1 * time.Millisecond)
// 	}
// 	for i := 0; i < 5; i++ {
// 		node3.Increment() // Isolated node
// 		time.Sleep(1 * time.Millisecond)
// 	}
//
// 	// Wait for node1 and node2 to sync with each other
// 	time.Sleep(300 * time.Millisecond)
//
// 	// Verify node1 and node2 converge to 23 (3 initial + 10 from node1 + 10 from node2)
// 	waitForConvergence(t, []*Node{node1, node2}, 23, 2*time.Second)
//
// 	// Node3 should be at 8 (3 initial + 5 during partition)
// 	require.Equal(t, uint64(8), node3.GetCounter(), "Node3 should have 8 total increments during partition")
//
// 	// Heal the partition
// 	node1.SetPeers([]string{"node2", "node3"})
// 	node2.SetPeers([]string{"node1", "node3"})
// 	node3.SetPeers([]string{"node1", "node2"})
//
// 	// Wait for sync
// 	time.Sleep(500 * time.Millisecond)
//
// 	// Verify all nodes converge to 28 (3 initial + 10 from node1 + 10 from node2 + 5 from node3)
// 	waitForConvergence(t, []*Node{node1, node2, node3}, 28, 2*time.Second)
// }
