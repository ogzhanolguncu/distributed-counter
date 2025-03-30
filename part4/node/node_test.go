package node

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part3/peer"
	"github.com/ogzhanolguncu/distributed-counter/part3/protocol"
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
	tmu.RLock()
	transport, exists := transports[addr]
	tmu.RUnlock()
	if !exists {
		return fmt.Errorf("transport not found for address: %s", addr)
	}
	return transport.handler(t.addr, data)
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

func waitForConvergence(t *testing.T, nodes []*Node, expectedCounter uint64, expectedVersion uint32, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allConverged := true
		for _, n := range nodes {
			if n.state.counter.Load() != expectedCounter || n.state.version.Load() != expectedVersion {
				allConverged = false
				break
			}
		}
		if allConverged {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge within timeout. Expected counter=%d, version=%d",
		expectedCounter, expectedVersion)
}

func createTestNodeWithWAL(t *testing.T, addr string, syncInterval time.Duration, maxPeerFail int, failureDuration time.Duration, walDir string) *Node {
	transport := NewMemoryTransport(addr)

	err := os.MkdirAll(walDir, 0755)
	require.NoError(t, err)

	config := Config{
		Addr:                addr,
		SyncInterval:        syncInterval,
		MaxSyncPeers:        2,
		MaxConsecutiveFails: maxPeerFail,
		FailureTimeout:      failureDuration,
		// WAL configuration
		WalDir:         walDir,
		EnableWal:      true,
		EnableWalFsync: true,
		MaxWalFileSize: 1 * 1024 * 1024,
	}

	peerManager := peer.NewPeerManager(maxPeerFail, failureDuration)
	node, err := NewNode(config, transport, peerManager)
	require.NoError(t, err)
	return node
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

func TestNodeBasicOperation(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond, 1, 2*time.Second)
	node2 := createTestNode(t, "node2", 100*time.Millisecond, 1, 2*time.Second)
	node3 := createTestNode(t, "node3", 100*time.Millisecond, 1, 2*time.Second)

	node1.peers.AddPeer("node2")
	node1.peers.AddPeer("node3")

	node2.peers.AddPeer("node1")
	node2.peers.AddPeer("node3")

	node2.peers.AddPeer("node1")
	node2.peers.AddPeer("node2")

	node1.state.counter.Store(42)
	node1.state.version.Store(1)

	waitForConvergence(t, []*Node{node1, node2, node3}, 42, 1, 2*time.Second)

	node1.Close()
	node2.Close()
	node3.Close()
	time.Sleep(200 * time.Millisecond)
}

func TestNodeStateConvergence(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond, 1, 2*time.Second)
	node2 := createTestNode(t, "node2", 100*time.Millisecond, 1, 2*time.Second)

	node1.peers.AddPeer("node2")
	node2.peers.AddPeer("node1")

	node1.state.counter.Store(100)
	node1.state.version.Store(1)
	node2.state.counter.Store(50)
	node2.state.version.Store(2)

	waitForConvergence(t, []*Node{node1, node2}, 50, 2, 2*time.Second)

	node1.Close()
	node2.Close()
	time.Sleep(200 * time.Millisecond)
}

func TestNodeLateJoiner(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond, 1, 2*time.Second)
	node2 := createTestNode(t, "node2", 100*time.Millisecond, 1, 2*time.Second)

	node1.state.counter.Store(100)
	node1.state.version.Store(5)

	node1.peers.AddPeer("node2")
	node2.peers.AddPeer("node1")

	waitForConvergence(t, []*Node{node1, node2}, 100, 5, 2*time.Second)

	node1.Close()
	node2.Close()
	time.Sleep(200 * time.Millisecond)
}

func TestConcurrentUpdates(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond, 1, 2*time.Second)
	node2 := createTestNode(t, "node2", 100*time.Millisecond, 1, 2*time.Second)
	node3 := createTestNode(t, "node3", 100*time.Millisecond, 1, 2*time.Second)

	node1.peers.AddPeer("node2")
	node1.peers.AddPeer("node3")

	node2.peers.AddPeer("node1")
	node2.peers.AddPeer("node3")

	node2.peers.AddPeer("node1")
	node2.peers.AddPeer("node2")

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		node1.state.counter.Store(100)
		node1.state.version.Store(1)
	}()

	go func() {
		defer wg.Done()
		node2.state.counter.Store(200)
		node2.state.version.Store(2)
	}()

	go func() {
		defer wg.Done()
		node3.state.counter.Store(300)
		node3.state.version.Store(3)
	}()

	wg.Wait()
	waitForConvergence(t, []*Node{node1, node2, node3}, 300, 3, 2*time.Second)

	node1.Close()
	node2.Close()
	node3.Close()
	time.Sleep(200 * time.Millisecond)
}

func TestMessageDropping(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond, 1, 2*time.Second)
	node2 := createTestNode(t, "node2", 100*time.Millisecond, 1, 2*time.Second)

	node1.peers.AddPeer("node2")
	node2.peers.AddPeer("node1")

	// Fill up the message buffer to force drops
	for range defaultChannelBuffer + 10 {
		node1.incomingMsg <- MessageInfo{
			message: protocol.Message{
				Type:    protocol.MessageTypePush,
				Version: 1,
				Counter: 100,
			},
			addr: "node2",
		}
	}

	node1.state.counter.Store(500)
	node1.state.version.Store(5)

	waitForConvergence(t, []*Node{node1, node2}, 500, 5, 2*time.Second)

	node1.Close()
	node2.Close()
	time.Sleep(200 * time.Millisecond)
}

func TestRingTopology(t *testing.T) {
	numNodes := 10
	nodes := make([]*Node, numNodes)

	for i := range numNodes {
		addr := fmt.Sprintf("node%d", i)
		nodes[i] = createTestNode(t, addr, 100*time.Millisecond, 1, 10*time.Second)
		defer nodes[i].Close()
	}

	for i := range numNodes {
		prevIdx := (i - 1 + numNodes) % numNodes
		nextIdx := (i + 1) % numNodes

		nodes[i].peers.AddPeer(fmt.Sprintf("node%d", prevIdx))
		nodes[i].peers.AddPeer(fmt.Sprintf("node%d", nextIdx))
	}

	nodes[0].state.counter.Store(42)
	nodes[0].state.version.Store(1)

	waitForConvergence(t, nodes, 42, 1, 5*time.Second)

	for i := range numNodes {
		nodes[i].Close()
	}
	time.Sleep(200 * time.Millisecond)
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

	waitForConvergence(t, []*Node{node1, node2, node3}, 1, 1, 2*time.Second)

	node3.Close()

	node1.Increment()

	waitForConvergence(t, []*Node{node1, node2}, 2, 2, 2*time.Second)

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

func TestMultiNodeWALConsistency(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal-multi-test-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	numNodes := 5
	nodes := make([]*Node, numNodes)

	for i := range numNodes {
		addr := fmt.Sprintf("node%d", i)
		walDir := filepath.Join(tempDir, addr)
		nodes[i] = createTestNodeWithWAL(t, addr, 50*time.Millisecond, 1, 2*time.Second, walDir)
	}

	for i := range numNodes {
		for j := range numNodes {
			if i != j {
				nodes[i].peers.AddPeer(fmt.Sprintf("node%d", j))
			}
		}
	}

	t.Log("Performing increments")
	nodes[0].Increment()

	time.Sleep(100 * time.Millisecond)

	nodes[2].Increment()

	time.Sleep(100 * time.Millisecond)

	nodes[4].Increment()

	t.Log("Waiting for first convergence")
	waitForConvergence(t, nodes, 3, 3, 10*time.Second)

	t.Log("Node states after first convergence:")
	for i, node := range nodes {
		t.Logf("Node %d: counter=%d, version=%d", i, node.GetCounter(), node.GetVersion())
	}

	t.Log("Closing nodes 1 and 3")
	require.NoError(t, nodes[1].Close())
	require.NoError(t, nodes[3].Close())

	t.Log("Restarting nodes 1 and 3")
	walDir1 := filepath.Join(tempDir, "node1")
	walDir3 := filepath.Join(tempDir, "node3")

	nodes[1] = createTestNodeWithWAL(t, "node1", 50*time.Millisecond, 1, 2*time.Second, walDir1)
	nodes[3] = createTestNodeWithWAL(t, "node3", 50*time.Millisecond, 1, 2*time.Second, walDir3)

	for j := range numNodes {
		if j != 1 {
			nodes[1].peers.AddPeer(fmt.Sprintf("node%d", j))
		}
		if j != 3 {
			nodes[3].peers.AddPeer(fmt.Sprintf("node%d", j))
		}
	}

	require.Equal(t, uint64(3), nodes[1].GetCounter(), "Restarted node should recover counter value")
	require.Equal(t, uint32(3), nodes[1].GetVersion(), "Restarted node should recover version")
	require.Equal(t, uint64(3), nodes[3].GetCounter(), "Restarted node should recover counter value")
	require.Equal(t, uint32(3), nodes[3].GetVersion(), "Restarted node should recover version")

	t.Log("Performing additional increment")
	nodes[2].Increment()

	t.Log("Waiting for final convergence")
	waitForConvergence(t, nodes, 4, 4, 10*time.Second)

	t.Log("Final node states:")
	for i, node := range nodes {
		t.Logf("Node %d: counter=%d, version=%d", i, node.GetCounter(), node.GetVersion())
	}

	t.Log("Closing all nodes")
	for i := range numNodes {
		nodes[i].Close()
	}
}
