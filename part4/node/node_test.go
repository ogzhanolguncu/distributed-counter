package node

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part4/crdt"
	"github.com/ogzhanolguncu/distributed-counter/part4/peer"
	"github.com/ogzhanolguncu/distributed-counter/part4/wal"
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

func createTestNode(t *testing.T, addr string, syncInterval time.Duration, maxPeerFail int, failureDuration time.Duration) *Node {
	transport := NewMemoryTransport(addr)
	config := Config{
		Addr:                addr,
		SyncInterval:        syncInterval,
		MaxSyncPeers:        2,
		MaxConsecutiveFails: maxPeerFail,
		FailureTimeout:      failureDuration,
	}

	peerManager := peer.NewPeerManager(maxPeerFail, failureDuration)
	node, err := NewNode(config, transport, peerManager)
	require.NoError(t, err)
	return node
}

func TestConcurrentIncrement(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond, 1, 2*time.Second)
	node2 := createTestNode(t, "node2", 100*time.Millisecond, 1, 2*time.Second)
	node3 := createTestNode(t, "node3", 100*time.Millisecond, 1, 2*time.Second)

	node1.peers.AddPeer("node2")
	node1.peers.AddPeer("node3")

	node2.peers.AddPeer("node1")
	node2.peers.AddPeer("node3")

	node3.peers.AddPeer("node2")
	node3.peers.AddPeer("node1")

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
	node1 := createTestNode(t, "node1", 100*time.Millisecond, 1, 2*time.Second)
	node2 := createTestNode(t, "node2", 100*time.Millisecond, 1, 2*time.Second)

	node1.peers.AddPeer("node2")

	for range 50 {
		node1.Increment()
		time.Sleep(1 * time.Millisecond)
	}

	node2.peers.AddPeer("node1")

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
	// Create three nodes with short sync intervals for faster testing
	node1 := createTestNode(t, "node1", 100*time.Millisecond, 1, 2*time.Second)
	node2 := createTestNode(t, "node2", 100*time.Millisecond, 1, 2*time.Second)
	node3 := createTestNode(t, "node3", 100*time.Millisecond, 1, 2*time.Second)

	// Set up the peer connections
	node1.GetPeerManager().AddPeer("node2")
	node1.GetPeerManager().AddPeer("node3")
	node2.GetPeerManager().AddPeer("node1")
	node2.GetPeerManager().AddPeer("node3")
	node3.GetPeerManager().AddPeer("node1")
	node3.GetPeerManager().AddPeer("node2")

	// Initial increments to ensure the network is working
	node1.Increment()
	node2.Increment()
	node3.Increment()

	// Wait for initial convergence
	waitForConvergence(t, []*Node{node1, node2, node3}, 3, 2*time.Second)

	// Create a network partition - isolate node3 from node1 and node2
	CreateBidirectionalPartition(t, "node1", "node3")
	CreateBidirectionalPartition(t, "node2", "node3")

	// Update the peer managers to reflect the network partition
	node1.GetPeerManager().RemovePeer("node3")
	node2.GetPeerManager().RemovePeer("node3")
	node3.GetPeerManager().RemovePeer("node1")
	node3.GetPeerManager().RemovePeer("node2")

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

	// Wait for node1 and node2 to converge (they're still connected to each other)
	waitForConvergence(t, []*Node{node1, node2}, 23, 2*time.Second)

	// Check node3's state during isolation
	require.Equal(t, int64(8), node3.GetCounter(),
		"Node3 should have 8 total increments during partition (3 initial + 5 new)")

	t.Log("State before healing partition:")
	logDetailedState(t, []*Node{node1, node2, node3})

	t.Log("Healing network partition")
	// Restore network connectivity
	HealBidirectionalPartition(t, "node1", "node3")
	HealBidirectionalPartition(t, "node2", "node3")

	// Restore peer configurations
	node1.GetPeerManager().AddPeer("node3")
	node2.GetPeerManager().AddPeer("node3")
	node3.GetPeerManager().AddPeer("node1")
	node3.GetPeerManager().AddPeer("node2")

	t.Log("State during convergence after healing:")
	logDetailedState(t, []*Node{node1, node2, node3})

	// Wait for all nodes to converge after the partition is healed
	// Total should be 28: 3 initial + 10 from node1 + 10 from node2 + 5 from node3
	waitForConvergence(t, []*Node{node1, node2, node3}, 28, 5*time.Second)

	t.Log("Final state after convergence:")
	logDetailedState(t, []*Node{node1, node2, node3})

	// Clean up
	node1.Close()
	node2.Close()
	node3.Close()
	time.Sleep(500 * time.Millisecond) // Allow time for graceful shutdown
}

func TestMessageInactiveNode(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond, 1, 10*time.Second)
	node2 := createTestNode(t, "node2", 100*time.Millisecond, 1, 10*time.Second)
	node3 := createTestNode(t, "node3", 100*time.Millisecond, 1, 10*time.Second)

	node1.peers.AddPeer("node2")
	node1.peers.AddPeer("node3")
	node2.peers.AddPeer("node1")
	node2.peers.AddPeer("node3")
	node3.peers.AddPeer("node1")
	node3.peers.AddPeer("node2")

	node1.Increment()

	waitForConvergence(t, []*Node{node1, node2, node3}, 1, 5*time.Second)

	node3.Close()

	node1.Increment()

	waitForConvergence(t, []*Node{node1, node2}, 2, 5*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		peers := node1.peers.GetPeers()
		if len(peers) == 1 && peers[0] == "node2" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	peers := node1.peers.GetPeers()
	require.Equal(t, 1, len(peers), "node1 should have exactly one active peer")
	require.Equal(t, "node2", peers[0], "node2 should be the only active peer")

	node1.Close()
	node2.Close()
	time.Sleep(200 * time.Millisecond)
}

func logDetailedState(t *testing.T, nodes []*Node) {
	for _, n := range nodes {
		increments, decrements := n.counter.Counters()
		t.Logf("Node %s: counter=%d, inc=%v, dec=%v",
			n.GetAddr(), n.GetCounter(), increments, decrements)
	}
}

func TestWALRecovery(t *testing.T) {
	// Create a unique data directory for this test
	dataDir, err := os.MkdirTemp("", "wal-recovery-test")
	require.NoError(t, err)

	// Create a separate data directory for node2 to ensure isolation
	dataDir2, err := os.MkdirTemp("", "wal-recovery-test-node2")
	require.NoError(t, err)

	// Defer cleanup for both directories
	defer func() {
		// Make sure all files are closed before removal
		time.Sleep(500 * time.Millisecond)

		// Cleanup node1's directory
		if err := os.RemoveAll(dataDir); err != nil {
			t.Logf("Warning: Failed to clean up node1 directory: %v", err)
		} else {
			t.Logf("Successfully cleaned up node1 directory: %s", dataDir)
		}

		// Cleanup node2's directory
		if err := os.RemoveAll(dataDir2); err != nil {
			t.Logf("Warning: Failed to clean up node2 directory: %v", err)
		} else {
			t.Logf("Successfully cleaned up node2 directory: %s", dataDir2)
		}

		// Also check for and remove any WAL directory in the current directory
		if _, err := os.Stat(walDirName); err == nil {
			if err := os.RemoveAll(walDirName); err != nil {
				t.Logf("Warning: Failed to clean up stray WAL directory: %v", err)
			} else {
				t.Logf("Successfully cleaned up stray WAL directory: %s", walDirName)
			}
		}
	}()

	// First node configuration - with a custom data directory
	transport1 := NewMemoryTransport("node1")
	config1 := Config{
		Addr:                "node1",
		SyncInterval:        100 * time.Millisecond,
		MaxSyncPeers:        2,
		MaxConsecutiveFails: 1,
		FailureTimeout:      2 * time.Second,
		DataDir:             dataDir,
		// Add specific WAL configuration for this test
		WALMaxSegments: 5,
		WALMinSegments: 2,
		WALMaxAge:      1 * time.Hour,
	}
	peerManager1 := peer.NewPeerManager(1, 2*time.Second)

	// Create and start the first node
	node1, err := NewNode(config1, transport1, peerManager1)
	require.NoError(t, err)

	// Create and connect a second node with its own data directory
	transport2 := NewMemoryTransport("node2")
	config2 := Config{
		Addr:                "node2",
		SyncInterval:        100 * time.Millisecond,
		MaxSyncPeers:        2,
		MaxConsecutiveFails: 1,
		FailureTimeout:      2 * time.Second,
		DataDir:             dataDir2, // Use separate directory for node2
		WALMaxSegments:      5,
		WALMinSegments:      2,
		WALMaxAge:           1 * time.Hour,
	}
	peerManager2 := peer.NewPeerManager(1, 2*time.Second)
	node2, err := NewNode(config2, transport2, peerManager2)
	require.NoError(t, err)

	// Connect the nodes
	node1.peers.AddPeer("node2")
	node2.peers.AddPeer("node1")

	// Perform some operations
	for i := 0; i < 20; i++ {
		node1.Increment()
		time.Sleep(1 * time.Millisecond)
	}

	for i := 0; i < 10; i++ {
		node2.Increment()
		time.Sleep(1 * time.Millisecond)
	}

	// Wait for convergence
	waitForConvergence(t, []*Node{node1, node2}, 30, 2*time.Second)

	// Capture the state before shutdown
	incs1, decs1 := node1.counter.Counters()
	t.Logf("Before shutdown - Node1: counter=%d, inc=%v, dec=%v",
		node1.GetCounter(), incs1, decs1)

	// Gracefully close node1
	err = node1.Close()
	require.NoError(t, err)

	// Give it time to properly close and sync
	time.Sleep(500 * time.Millisecond)

	// Create a new node with the same config (simulating a restart)
	transport1New := NewMemoryTransport("node1")
	peerManager1New := peer.NewPeerManager(1, 2*time.Second)

	// Create a new node instance that should recover state from WAL
	node1Recovered, err := NewNode(config1, transport1New, peerManager1New)
	require.NoError(t, err)

	// Verify that the recovered node has the correct counter value
	incsRecovered, decsRecovered := node1Recovered.counter.Counters()
	t.Logf("After recovery - Node1: counter=%d, inc=%v, dec=%v",
		node1Recovered.GetCounter(), incsRecovered, decsRecovered)

	// Check that the recovered state matches the original state
	require.Equal(t, node1.GetCounter(), node1Recovered.GetCounter(),
		"Recovered node should have the same counter value as before shutdown")

	// Connect the recovered node to node2
	node1Recovered.peers.AddPeer("node2")
	node2.peers.AddPeer("node1")

	// Perform additional operations to test the recovered node
	for i := 0; i < 5; i++ {
		node1Recovered.Increment()
		time.Sleep(1 * time.Millisecond)
	}

	// Wait for convergence with the new increments
	waitForConvergence(t, []*Node{node1Recovered, node2}, 35, 2*time.Second)

	// Cleanup - make sure nodes are properly closed
	// and we wait long enough for file handles to be released
	err = node1Recovered.Close()
	require.NoError(t, err)

	err = node2.Close()
	require.NoError(t, err)

	// Wait for file handles to be properly released before cleanup
	time.Sleep(500 * time.Millisecond)
}

func TestWALSegmentCleanup(t *testing.T) {
	// Create a unique data directory for this test
	dataDir, err := os.MkdirTemp("", "wal-segment-test")
	require.NoError(t, err)

	// Use defer with a function to ensure proper cleanup happens
	defer func() {
		// Make sure all files are closed before removal
		time.Sleep(500 * time.Millisecond)

		// Log what we're about to clean up
		walFiles, err := filepath.Glob(filepath.Join(dataDir, "wal", "*", "segment-*"))
		if err == nil && len(walFiles) > 0 {
			t.Logf("Found %d WAL segment files that will be cleaned up:", len(walFiles))
			for _, file := range walFiles {
				t.Logf("  - %s", file)
			}
		}

		err = os.RemoveAll(dataDir)
		if err != nil {
			t.Logf("Warning: Failed to clean up test directory: %v", err)
		} else {
			t.Logf("Successfully cleaned up test directory: %s", dataDir)
		}

		// Also check for and remove any WAL directory in the current directory
		if _, err := os.Stat(walDirName); err == nil {
			if err := os.RemoveAll(walDirName); err != nil {
				t.Logf("Warning: Failed to clean up stray WAL directory: %v", err)
			} else {
				t.Logf("Successfully cleaned up stray WAL directory: %s", walDirName)
			}
		}
	}()

	// Enable debug logging for WAL
	os.Setenv("WAL_DEBUG", "1")

	// Create a node with VERY small max file size to generate many segments quickly
	config1 := Config{
		Addr:                "node1",
		SyncInterval:        100 * time.Millisecond,
		MaxSyncPeers:        2,
		MaxConsecutiveFails: 1,
		FailureTimeout:      2 * time.Second,
		DataDir:             dataDir,
		// Configure WAL with small maximums to test cleanup
		WALMaxSegments: 3, // Keep only 3 segments max
		WALMinSegments: 2, // Always keep at least 2 segments
		WALMaxAge:      1 * time.Hour,
		// Set a very short cleanup interval to ensure it runs during test
		WALCleanupInterval: 200 * time.Millisecond,
	}

	// Create a tiny max file size WAL to force frequent segment rotation
	walPath := filepath.Join(dataDir, walDirName, "node1")
	if err := os.MkdirAll(walPath, 0755); err != nil {
		t.Fatalf("Failed to create WAL directory: %v", err)
	}

	// Open WAL with a very small max file size (1KB) to force segment rotation
	nodeWAL, err := wal.OpenWAL(walPath, "node1", true, 1024)
	require.NoError(t, err)

	// Initialize counter
	counter := crdt.New("node1")

	// Manually generate more segments by doing many operations
	t.Log("Generating multiple WAL segments manually...")

	// Generate enough operations to create several segments
	for i := 0; i < 200; i++ {
		// Add operations that will increase segment size
		if i%2 == 0 {
			counter.Increment("node1")
			err = nodeWAL.WriteCounterIncrement("node1")
		} else {
			counter.Decrement("node1")
			err = nodeWAL.WriteCounterDecrement("node1")
		}
		require.NoError(t, err)

		// Add larger metadata to force segment rotation
		largeData := make(map[string]interface{})
		largeData["timestamp"] = time.Now().UnixNano()
		largeData["operation_id"] = i
		largeData["padding"] = strings.Repeat("padding-data", 20) // Add bulk

		err = nodeWAL.WriteMetadata(largeData)
		require.NoError(t, err)

		// Force sync to ensure data is written
		if i%10 == 0 {
			nodeWAL.Sync()
		}

		// Take a snapshot occasionally
		if i%30 == 0 {
			err = nodeWAL.WriteCounterState(counter)
			require.NoError(t, err)
			nodeWAL.Sync()
		}
	}

	// Force final sync
	nodeWAL.Sync()

	// Check how many segments we have before cleanup
	files, err := filepath.Glob(filepath.Join(walPath, "segment-*"))
	require.NoError(t, err)
	t.Logf("Created %d segment files before cleanup", len(files))
	require.Greater(t, len(files), 3, "Should have created multiple segments")

	// Run cleanup directly
	cleanupConfig := wal.CleanupConfig{
		MaxSegments: config1.WALMaxSegments,
		MinSegments: config1.WALMinSegments,
		MaxAge:      config1.WALMaxAge,
	}
	err = nodeWAL.CleanupSegments(cleanupConfig)
	require.NoError(t, err)

	// Wait for cleanup to complete
	time.Sleep(200 * time.Millisecond)

	// Check the number of segment files after cleanup
	files, err = filepath.Glob(filepath.Join(walPath, "segment-*"))
	require.NoError(t, err)
	t.Logf("Found %d segment files after cleanup", len(files))
	for _, f := range files {
		t.Logf("  Segment file: %s", filepath.Base(f))
	}

	// The number of segments should be within the configured limits
	require.LessOrEqual(t, len(files), config1.WALMaxSegments,
		"Should have no more than WALMaxSegments files")
	require.GreaterOrEqual(t, len(files), config1.WALMinSegments,
		"Should have at least WALMinSegments files")

	// Remember the counter value
	counterValue := counter.Value()

	// Close WAL before recovery test
	err = nodeWAL.Close()
	require.NoError(t, err)

	// Wait for file handles to be properly released
	time.Sleep(500 * time.Millisecond)

	// Recover the counter from the remaining WAL segments
	recoveredCounter, err := wal.RecoverCounter(walPath, "node1")
	require.NoError(t, err)

	// Verify that the recovered counter has the correct value
	recoveredValue := recoveredCounter.Value()
	t.Logf("Counter value after recovery: %d", recoveredValue)

	// Despite cleanup, the counter value should be maintained
	require.Equal(t, counterValue, recoveredValue,
		"Counter value should be preserved after WAL segment cleanup")
}
