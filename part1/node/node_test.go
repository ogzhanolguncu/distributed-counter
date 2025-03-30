package node

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type MemoryTransport struct {
	addr            string
	handler         func(addr string, data []byte) error
	mu              sync.RWMutex
	partitionedFrom map[string]bool // Track which nodes this node is partitioned from
}

func NewMemoryTransport(addr string) *MemoryTransport {
	return &MemoryTransport{
		addr:            addr,
		partitionedFrom: make(map[string]bool),
	}
}

func (t *MemoryTransport) Send(addr string, data []byte) error {
	time.Sleep(10 * time.Millisecond) // Prevent message flood

	// Check if recipient is in our partition list
	t.mu.RLock()
	partitioned := t.partitionedFrom[addr]
	t.mu.RUnlock()

	if partitioned {
		return fmt.Errorf("network partition: cannot send to %s from %s", addr, t.addr)
	}

	// Get the transport while holding the global lock
	tmu.RLock()
	transport, exists := transports[addr]
	tmu.RUnlock()
	if !exists {
		return fmt.Errorf("transport not found for address: %s", addr)
	}

	// Check if sender is in recipient's partition list
	transport.mu.RLock()
	senderPartitioned := transport.partitionedFrom[t.addr]
	transport.mu.RUnlock()

	if senderPartitioned {
		return fmt.Errorf("network partition: cannot receive from %s at %s", t.addr, addr)
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

// AddPartition simulates a network partition between this node and another
func (t *MemoryTransport) AddPartition(addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.partitionedFrom[addr] = true
}

// RemovePartition removes a simulated network partition
func (t *MemoryTransport) RemovePartition(addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.partitionedFrom, addr)
}

// CreateBidirectionalPartition creates a partition between two nodes
func CreateBidirectionalPartition(t *testing.T, addr1, addr2 string) {
	transport1, exists1 := GetTransport(addr1)
	transport2, exists2 := GetTransport(addr2)

	require.True(t, exists1, "Transport for %s should exist", addr1)
	require.True(t, exists2, "Transport for %s should exist", addr2)

	transport1.AddPartition(addr2)
	transport2.AddPartition(addr1)
}

// HealBidirectionalPartition heals a partition between two nodes
func HealBidirectionalPartition(t *testing.T, addr1, addr2 string) {
	transport1, exists1 := GetTransport(addr1)
	transport2, exists2 := GetTransport(addr2)

	require.True(t, exists1, "Transport for %s should exist", addr1)
	require.True(t, exists2, "Transport for %s should exist", addr2)

	transport1.RemovePartition(addr2)
	transport2.RemovePartition(addr1)
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

// waitForConvergence verifies that all nodes converge to the expected counter value
// within the given timeout
func waitForConvergence(t *testing.T, nodes []*Node, expectedTotalCounter int64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	logInterval := 200 * time.Millisecond

	for time.Now().Before(deadline) {
		allConverged := true
		allValues := make(map[string]int64)

		for _, n := range nodes {
			// Get the total counter value
			totalCounter := n.GetCounter()
			allValues[n.GetAddr()] = totalCounter

			if totalCounter != expectedTotalCounter {
				allConverged = false
			}
		}

		// Only log at intervals to reduce spam
		if !allConverged && time.Since(lastLog) > logInterval {
			t.Logf("Waiting for convergence: %v (expected %d)", allValues, expectedTotalCounter)
			lastLog = time.Now()
		}

		if allConverged {
			t.Logf("All nodes converged to %d", expectedTotalCounter)
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

// GetTransport retrieves a transport for testing purposes
func GetTransport(addr string) (*MemoryTransport, bool) {
	tmu.RLock()
	defer tmu.RUnlock()
	transport, exists := transports[addr]
	return transport, exists
}

// createTestNode creates a new node with a memory transport for testing
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
	node3 := createTestNode(t, "node3", 100*time.Millisecond)

	node1.SetPeers([]string{"node2", "node3"})
	node2.SetPeers([]string{"node1", "node3"})
	node3.SetPeers([]string{"node2", "node1"})

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	var wg3 sync.WaitGroup

	wg1.Add(1)
	go func() {
		defer wg1.Done()
		for range 100 {
			node1.Increment()
			// Small sleep to prevent overloading
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		for range 100 {
			node2.Increment()
			// Small sleep to prevent overloading
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg3.Add(1)
	go func() {
		defer wg3.Done()
		for range 100 {
			node3.Decrement()
			// Small sleep to prevent overloading
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg1.Wait()
	wg2.Wait()
	wg3.Wait()

	waitForConvergence(t, []*Node{node1, node2, node3}, 100, 5*time.Second)

	node1.Close()
	node2.Close()
	node3.Close()

	time.Sleep(500 * time.Millisecond)
}

func TestLateJoiningNode(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)

	node1.SetPeers([]string{"node2"})

	for range 50 {
		node1.Increment()
		time.Sleep(1 * time.Millisecond)
	}

	node2.SetPeers([]string{"node1"})

	waitForConvergence(t, []*Node{node1, node2}, 50, 2*time.Second)

	for range 30 {
		node2.Increment()
		time.Sleep(1 * time.Millisecond)
	}

	waitForConvergence(t, []*Node{node1, node2}, 80, 2*time.Second)

	counters1, _ := node1.counter.Counters()
	counters2, _ := node2.counter.Counters()

	require.Equal(t, uint64(50), counters1["node1"], "Node1 should have 50 increments for itself")
	require.Equal(t, uint64(30), counters1["node2"], "Node1 should have 30 increments for node2")
	require.Equal(t, uint64(50), counters2["node1"], "Node2 should have 50 increments for node1")
	require.Equal(t, uint64(30), counters2["node2"], "Node2 should have 30 increments for itself")

	node1.Close()
	node2.Close()

	time.Sleep(500 * time.Millisecond)
}

func TestNetworkPartition(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)
	node3 := createTestNode(t, "node3", 100*time.Millisecond)

	node1.SetPeers([]string{"node2", "node3"})
	node2.SetPeers([]string{"node1", "node3"})
	node3.SetPeers([]string{"node1", "node2"})

	node1.Increment()
	node2.Increment()
	node3.Increment()

	waitForConvergence(t, []*Node{node1, node2, node3}, 3, 2*time.Second)

	CreateBidirectionalPartition(t, "node1", "node3")
	CreateBidirectionalPartition(t, "node2", "node3")

	node1.SetPeers([]string{"node2"})
	node2.SetPeers([]string{"node1"})

	t.Log("Incrementing node1 and node2 during partition")
	for range 10 {
		node1.Increment()
		node2.Increment()
		time.Sleep(1 * time.Millisecond)
	}

	t.Log("Incrementing isolated node3 during partition")
	for range 5 {
		node3.Increment()
		time.Sleep(1 * time.Millisecond)
	}

	waitForConvergence(t, []*Node{node1, node2}, 23, 2*time.Second)

	require.Equal(t, int64(8), node3.GetCounter(),
		"Node3 should have 8 total increments during partition (3 initial + 5 new)")

	t.Log("State before healing partition:")
	logDetailedState(t, []*Node{node1, node2, node3})

	t.Log("Healing network partition")
	HealBidirectionalPartition(t, "node1", "node3")
	HealBidirectionalPartition(t, "node2", "node3")

	node1.SetPeers([]string{"node2", "node3"})
	node2.SetPeers([]string{"node1", "node3"})
	node3.SetPeers([]string{"node1", "node2"})

	t.Log("State during convergence after healing:")
	logDetailedState(t, []*Node{node1, node2, node3})

	waitForConvergence(t, []*Node{node1, node2, node3}, 28, 5*time.Second)

	t.Log("Final state after convergence:")
	logDetailedState(t, []*Node{node1, node2, node3})

	node1.Close()
	node2.Close()
	node3.Close()

	time.Sleep(500 * time.Millisecond)
}

func logDetailedState(t *testing.T, nodes []*Node) {
	for _, n := range nodes {
		increments, decrements := n.counter.Counters()
		t.Logf("Node %s: counter=%d, inc=%v, dec=%v",
			n.GetAddr(), n.GetCounter(), increments, decrements)
	}
}
