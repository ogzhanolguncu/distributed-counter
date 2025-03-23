package main

import (
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part0/node"
	"github.com/ogzhanolguncu/distributed-counter/part0/protocol"
)

const (
	numNodes       = 20
	basePort       = 9000
	testDuration   = 20 * time.Second
	operationsRate = 100 * time.Millisecond // Perform operations every X ms
	syncInterval   = 2 * time.Second
	maxSyncPeers   = 5
	cooldownPeriod = 10 * time.Second // Time after operations to allow full convergence
)

// Simple TCP transport implementation
type TCPTransport struct {
	addr     string
	listener net.Listener
	handler  func(addr string, data []byte) error
	closed   bool
	mu       sync.RWMutex
}

func NewTCPTransport(addr string) (*TCPTransport, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	t := &TCPTransport{
		addr:     addr,
		listener: listener,
	}

	return t, nil
}

func (t *TCPTransport) Listen(handler func(addr string, data []byte) error) error {
	t.handler = handler

	go func() {
		for {
			conn, err := t.listener.Accept()
			if err != nil {
				t.mu.RLock()
				closed := t.closed
				t.mu.RUnlock()
				if closed {
					return
				}
				fmt.Printf("Error accepting connection: %v\n", err)
				continue
			}

			go t.handleConnection(conn)
		}
	}()

	return nil
}

func (t *TCPTransport) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read message size first (assuming fixed buffer size for simplicity)
	buf := make([]byte, protocol.DefaultBufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Printf("Error reading from connection: %v\n", err)
		return
	}

	if t.handler != nil {
		if err := t.handler(conn.RemoteAddr().String(), buf[:n]); err != nil {
			fmt.Printf("Error handling message: %v\n", err)
		}
	}
}

func (t *TCPTransport) Send(addr string, data []byte) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Write(data)
	return err
}

func (t *TCPTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return t.listener.Close()
}

func generateAddress(id int) string {
	return fmt.Sprintf("127.0.0.1:%d", basePort+id)
}

// Improved transport reliability with retries
func sendWithRetry(transport protocol.Transport, addr string, data []byte, maxRetries int) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := transport.Send(addr, data)
		if err == nil {
			return nil
		}
		lastErr = err
		// Exponential backoff
		time.Sleep(time.Duration(1<<i) * time.Millisecond)
	}
	return lastErr
}

func main() {
	fmt.Println("Starting distributed counter test with", numNodes, "nodes")
	fmt.Println("Test will run for", testDuration)

	// Create nodes
	nodes := make([]*node.Node, numNodes)

	// Start nodes
	for i := 0; i < numNodes; i++ {
		addr := generateAddress(i)
		transport, err := NewTCPTransport(addr)
		if err != nil {
			fmt.Printf("Failed to create transport for node %d: %v\n", i, err)
			return
		}

		config := node.Config{
			Addr:         addr,
			SyncInterval: syncInterval,
			MaxSyncPeers: maxSyncPeers,
		}

		n, err := node.NewNode(config, transport)
		if err != nil {
			fmt.Printf("Failed to create node %d: %v\n", i, err)
			return
		}

		nodes[i] = n
	}

	// Configure all peers for each node
	allAddresses := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		allAddresses[i] = generateAddress(i)
	}

	for i := 0; i < numNodes; i++ {
		// Filter out own address
		peers := make([]string, 0, numNodes-1)
		for j := 0; j < numNodes; j++ {
			if i != j {
				peers = append(peers, allAddresses[j])
			}
		}
		nodes[i].SetPeers(peers)
	}

	fmt.Println("All nodes created and peers configured")

	// Track expected counter values
	expectedIncrements := int64(0)
	expectedDecrements := int64(0)
	var counterMutex sync.Mutex

	// Start operations
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Function to perform random operations on nodes
	performOperations := func(nodeIndex int) {
		defer wg.Done()
		ticker := time.NewTicker(operationsRate)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Randomly decide whether to increment or decrement
				if rand.IntN(2) == 0 {
					nodes[nodeIndex].Increment()
					counterMutex.Lock()
					expectedIncrements++
					counterMutex.Unlock()
				} else {
					nodes[nodeIndex].Decrement()
					counterMutex.Lock()
					expectedDecrements++
					counterMutex.Unlock()
				}
			case <-stopChan:
				return
			}
		}
	}

	// Launch operation goroutines
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go performOperations(i)
	}

	// Run for specified duration
	fmt.Println("Test running, performing operations...")
	time.Sleep(testDuration)
	close(stopChan)
	wg.Wait()

	// Since we can't access the transport directly from the Node struct,
	// we'll use a different approach to ensure synchronization
	fmt.Println("Operations completed, forcing final synchronization...")

	// Instead of trying to directly access the transport layer,
	// we'll use the existing node mechanics to trigger more frequent syncs

	// First, create a mesh pattern where each node pulls state multiple times
	for syncRound := 0; syncRound < 3; syncRound++ {
		fmt.Printf("Sync round %d...\n", syncRound+1)

		// We'll trigger multiple sync cycles manually by waiting a bit longer than the sync interval
		time.Sleep(syncInterval * 2)

		// Check if we're making progress
		fmt.Println("Current state snapshot:")
		allSame := true
		firstVal := nodes[0].GetCounter()
		for i, n := range nodes {
			val := n.GetCounter()
			if i < 5 { // Just show first 5 to avoid cluttering output
				fmt.Printf("Node %d: %d, ", i, val)
			}
			if val != firstVal {
				allSame = false
			}
		}
		fmt.Println()

		if allSame {
			fmt.Println("All nodes have already converged!")
			break
		}
	}

	// Give time for final synchronization
	fmt.Println("Waiting for final convergence...", cooldownPeriod)
	time.Sleep(cooldownPeriod)

	// Verify results
	fmt.Println("\n--- Test Results ---")

	// Compute expected value
	expectedValue := expectedIncrements - expectedDecrements
	fmt.Printf("Total operations: %d increments, %d decrements\n", expectedIncrements, expectedDecrements)
	fmt.Printf("Expected final counter value: %d\n", expectedValue)

	// Check if all nodes converged
	allEqual := true
	firstValue := nodes[0].GetCounter()

	fmt.Println("\nFinal counter values:")
	for i, n := range nodes {
		value := n.GetCounter()
		fmt.Printf("Node %d (%s): %d\n", i, n.GetAddr(), value)
		if value != firstValue {
			allEqual = false
		}
	}

	fmt.Println("\nConvergence check:")
	if allEqual {
		fmt.Printf("SUCCESS: All nodes converged to value %d\n", firstValue)
		if firstValue == expectedValue {
			fmt.Println("SUCCESS: Final value matches expected value")
		} else {
			fmt.Printf("FAILURE: Final value %d doesn't match expected value %d\n", firstValue, expectedValue)
		}
	} else {
		fmt.Println("FAILURE: Nodes did not converge to the same value")

		// Additional diagnostics
		fmt.Println("\nDiagnostics:")
		fmt.Println("Individual node increment/decrement counters:")

		for i, n := range nodes {
			fmt.Printf("Node %d (%s):\n", i, n.GetAddr())
			fmt.Printf("  Total value: %d\n", n.GetCounter())
			fmt.Printf("  Local value: %d\n", n.GetLocalCounter())
		}
	}

	// Clean up
	for _, n := range nodes {
		n.Close()
	}
}
