package main

import (
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part2/node"
	"github.com/ogzhanolguncu/distributed-counter/part2/peer"
	"github.com/ogzhanolguncu/distributed-counter/part2/protocol"
)

const (
	numNodes      = 10
	basePort      = 9000
	testDuration  = 10 * time.Second
	operationRate = 100 * time.Millisecond
)

func main() {
	var (
		expectedValue int64
		increments    int64
		decrements    int64
		metricsLock   sync.Mutex
	)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	fmt.Println("=== DISTRIBUTED COUNTER ===")

	// Create nodes
	nodes := make([]*node.Node, numNodes)
	for i := range numNodes {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		transport, err := protocol.NewTCPTransport(addr, logger)
		if err != nil {
			log.Fatalf("Failed to create transport: %v", err)
		}

		n, err := node.NewNode(node.Config{
			Addr:         addr,
			SyncInterval: 500 * time.Millisecond,
			MaxSyncPeers: 5,
			LogLevel:     slog.LevelError,
		}, transport, peer.NewPeerManager(), logger)
		if err != nil {
			log.Fatalf("Failed to create node: %v", err)
		}
		nodes[i] = n
	}
	fmt.Printf("Created %d nodes\n", numNodes)

	// Connect all nodes (full mesh topology)
	for i, node := range nodes {
		pm := node.GetPeerManager()
		for j, peer := range nodes {
			if i != j {
				pm.AddPeer(peer.GetAddr())
			}
		}
	}
	fmt.Println("Connected all nodes in a full mesh")

	// Run concurrent operations
	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Start concurrent increment/decrement operations
	fmt.Println("Starting operations...")
	for i := range numNodes {
		wg.Add(1)
		go func(nodeIndex int) {
			defer wg.Done()

			ticker := time.NewTicker(operationRate)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					// Randomly increment or decrement
					if rand.Intn(2) == 0 {
						nodes[nodeIndex].Increment()
						metricsLock.Lock()
						expectedValue++
						increments++
						metricsLock.Unlock()
					} else {
						nodes[nodeIndex].Decrement()
						metricsLock.Lock()
						expectedValue--
						decrements++
						metricsLock.Unlock()
					}
				case <-stopChan:
					return
				}
			}
		}(i)
	}

	// Run the test for specified duration
	fmt.Printf("Running test for %v...\n", testDuration)
	time.Sleep(testDuration)

	// Stop operations
	close(stopChan)
	fmt.Println("Test complete. Waiting for final synchronization...")

	// Wait for nodes to finish syncing
	time.Sleep(3 * time.Second)
	wg.Wait()

	// Final results
	fmt.Println("\n=== FINAL RESULTS ===")

	metricsLock.Lock()
	finalExpected := expectedValue
	finalIncs := increments
	finalDecs := decrements
	metricsLock.Unlock()

	fmt.Printf("Operations: %d increments, %d decrements\n", finalIncs, finalDecs)
	fmt.Printf("Expected value: %d\n", finalExpected)

	// Check if nodes converged
	fmt.Println("Node values:")
	allSame := true
	firstValue := nodes[0].GetCounter()

	for i, node := range nodes {
		value := node.GetCounter()
		fmt.Printf("Node %d: %d\n", i, value)

		if value != firstValue {
			allSame = false
		}
	}

	// Print convergence status
	if allSame {
		fmt.Printf("\nSUCCESS: All nodes converged to %d\n", firstValue)
		if firstValue == finalExpected {
			fmt.Println("PERFECT: Value matches expected count!")
		} else {
			fmt.Printf("PARTIAL: Nodes converged but to unexpected value (expected %d, got %d)\n",
				finalExpected, firstValue)
		}
	} else {
		fmt.Println("\nFAILURE: Nodes did not converge to the same value")
	}

	// Shutdown
	fmt.Println("\nShutting down...")
	for i, n := range nodes {
		if err := n.Close(); err != nil {
			log.Printf("Error closing node %d: %v", i, err)
		}
	}
	fmt.Println("All components shut down successfully")
}
