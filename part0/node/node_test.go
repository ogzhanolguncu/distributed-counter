package node

import (
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
	return transports[addr].handler(t.addr, data)
}

func (t *MemoryTransport) Listen(handler func(addr string, data []byte) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = handler
	transports[t.addr] = t
	return nil
}

func (t *MemoryTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(transports, t.addr)
	return nil
}

// Global registry of transports - simulates the network
var (
	transports = make(map[string]*MemoryTransport)
	tmu        sync.RWMutex
)

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

func TestNodeBasicOperation(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)
	node3 := createTestNode(t, "node3", 100*time.Millisecond)
	defer node1.cancel()
	defer node2.cancel()
	defer node3.cancel()

	node1.peers = []string{"node2", "node3"}
	node2.peers = []string{"node1", "node3"}
	node3.peers = []string{"node1", "node2"}

	node1.state.mu.Lock()
	node1.state.counter = 42
	node1.state.version = 1
	node1.state.mu.Unlock()

	time.Sleep(500 * time.Millisecond)

	assertNodeState := func(n *Node, expectedCounter uint64, expectedVersion uint32) {
		n.state.mu.RLock()
		defer n.state.mu.RUnlock()
		require.Equal(t, expectedCounter, n.state.counter)
		require.Equal(t, expectedVersion, n.state.version)
	}

	assertNodeState(node2, 42, 1)
	assertNodeState(node3, 42, 1)
}

func TestNodeStateConvergence(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)
	defer node1.cancel()
	defer node2.cancel()

	node1.peers = []string{"node2"}
	node2.peers = []string{"node1"}

	node1.state.mu.Lock()
	node1.state.counter = 100
	node1.state.version = 1
	node1.state.mu.Unlock()

	node2.state.mu.Lock()
	node2.state.counter = 50
	node2.state.version = 2
	node2.state.mu.Unlock()

	time.Sleep(500 * time.Millisecond)

	node1.state.mu.RLock()
	require.Equal(t, uint64(50), node1.state.counter)
	require.Equal(t, uint32(2), node1.state.version)
	node1.state.mu.RUnlock()

	node2.state.mu.RLock()
	require.Equal(t, uint64(50), node2.state.counter)
	require.Equal(t, uint32(2), node2.state.version)
	node2.state.mu.RUnlock()
}
