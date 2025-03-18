package node

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part0/protocol"
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

// Updated to work with new State implementation
func waitForConvergence(t *testing.T, nodes []*Node, expectedCounter uint64, expectedVersion uint64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allConverged := true
		for _, n := range nodes {
			state := n.state.GetState()
			if state.Counter != expectedCounter || state.Version != expectedVersion {
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

	node, err := NewNode(config, transport)
	require.NoError(t, err)
	return node
}

func setNodeState(node *Node, counter uint64, version uint64) {
	state := &CounterVersionState{
		Counter: counter,
		Version: version,
	}
	node.state.state.Store(state)
}

func TestNodeBasicOperation(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)
	node3 := createTestNode(t, "node3", 100*time.Millisecond)

	node1.SetPeers([]string{"node2", "node3"})
	node2.SetPeers([]string{"node1", "node3"})
	node3.SetPeers([]string{"node1", "node2"})

	setNodeState(node1, 42, 1)

	waitForConvergence(t, []*Node{node1, node2, node3}, 42, 1, 2*time.Second)

	node1.Close()
	node2.Close()
	node3.Close()
	time.Sleep(200 * time.Millisecond)
}

func TestNodeStateConvergence(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)

	node1.SetPeers([]string{"node2"})
	node2.SetPeers([]string{"node1"})

	setNodeState(node1, 100, 1)
	setNodeState(node2, 50, 2)

	waitForConvergence(t, []*Node{node1, node2}, 50, 2, 2*time.Second)

	node1.Close()
	node2.Close()
	time.Sleep(200 * time.Millisecond)
}

func TestNodeLateJoiner(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)

	setNodeState(node1, 100, 5)

	node1.SetPeers([]string{"node2"})
	node2.SetPeers([]string{"node1"})

	waitForConvergence(t, []*Node{node1, node2}, 100, 5, 2*time.Second)

	node1.Close()
	node2.Close()
	time.Sleep(200 * time.Millisecond)
}

func TestConcurrentUpdates(t *testing.T) {
	node1 := createTestNode(t, "node1", 100*time.Millisecond)
	node2 := createTestNode(t, "node2", 100*time.Millisecond)
	node3 := createTestNode(t, "node3", 100*time.Millisecond)

	node1.SetPeers([]string{"node2", "node3"})
	node2.SetPeers([]string{"node1", "node3"})
	node3.SetPeers([]string{"node1", "node2"})

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		setNodeState(node1, 100, 1)
	}()

	go func() {
		defer wg.Done()
		setNodeState(node2, 200, 2)
	}()

	go func() {
		defer wg.Done()
		setNodeState(node3, 300, 3)
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

	node1.SetPeers([]string{"node2"})
	node2.SetPeers([]string{"node1"})

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

	// Set initial state using our helper
	setNodeState(node1, 500, 5)

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
		transport := NewMemoryTransport(addr)
		config := Config{
			Addr:         addr,
			SyncInterval: 100 * time.Millisecond,
			MaxSyncPeers: numNodes / 2, // To make it converge faster
		}

		node, err := NewNode(config, transport)
		require.NoError(t, err)
		nodes[i] = node
	}

	for i := range numNodes {
		prev := (i - 1 + numNodes) % numNodes
		next := (i + 1) % numNodes
		nodes[i].SetPeers([]string{
			fmt.Sprintf("node%d", prev),
			fmt.Sprintf("node%d", next),
		})
	}

	setNodeState(nodes[0], 42, 1)

	waitForConvergence(t, nodes, 42, 1, 3*time.Second)

	for i := range numNodes {
		nodes[i].Close()
	}
}
