package node

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part1/peer"
	"github.com/ogzhanolguncu/distributed-counter/part1/protocol"
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

func createTestNode(t *testing.T, addr string, syncInterval time.Duration) *Node {
	transport := NewMemoryTransport(addr)
	config := Config{
		Addr:         addr,
		SyncInterval: syncInterval,
		MaxSyncPeers: 2,
	}

	peerManager := peer.NewPeerManager()
	node, err := NewNode(config, transport, peerManager)
	require.NoError(t, err)
	return node
}

func TestNodeBasicOperation(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)
	node3 := createTestNode(t, "node3", 100*time.Millisecond)

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
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)

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
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)

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
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)
	node3 := createTestNode(t, "node3", 100*time.Millisecond)

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
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)

	node1.peers.AddPeer("node2")
	node2.peers.AddPeer("node1")

	// Fill up the message buffer to force drops
	for i := 0; i < defaultChannelBuffer+10; i++ {
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

	for i := 0; i < numNodes; i++ {
		addr := fmt.Sprintf("node%d", i)
		nodes[i] = createTestNode(t, addr, 100*time.Millisecond)
		defer nodes[i].Close()
	}

	for i := 0; i < numNodes; i++ {
		prevIdx := (i - 1 + numNodes) % numNodes
		nextIdx := (i + 1) % numNodes

		nodes[i].peers.AddPeer(fmt.Sprintf("node%d", prevIdx))
		nodes[i].peers.AddPeer(fmt.Sprintf("node%d", nextIdx))
	}

	nodes[0].state.counter.Store(42)
	nodes[0].state.version.Store(1)

	waitForConvergence(t, nodes, 42, 1, 5*time.Second)

	for i := 0; i < numNodes; i++ {
		nodes[i].Close()
	}
}
