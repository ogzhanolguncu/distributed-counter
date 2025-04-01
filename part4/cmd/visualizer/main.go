package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part4/discovery"
	"github.com/ogzhanolguncu/distributed-counter/part4/node"
	"github.com/ogzhanolguncu/distributed-counter/part4/peer"
	"github.com/ogzhanolguncu/distributed-counter/part4/protocol"
	"github.com/ogzhanolguncu/distributed-counter/part4/visualizer"
)

const (
	numNodes          = 5
	basePort          = 9000
	discoveryPort     = 8000
	syncInterval      = 500 * time.Millisecond
	heartbeatInterval = 1 * time.Second
	testDuration      = 30 * time.Second
)

func main() {
	// Create visualizer
	vis, err := visualizer.NewVisualizer("gossip_log.txt")
	if err != nil {
		log.Fatalf("Failed to create visualizer: %v", err)
	}
	defer vis.Stop()

	fmt.Println("Starting discovery server...")
	// Start discovery server
	discoveryAddr := fmt.Sprintf("127.0.0.1:%d", discoveryPort)
	discoveryServer := discovery.NewDiscoveryServer(discoveryAddr, 5*time.Second)

	go func() {
		if err := discoveryServer.Start(); err != nil {
			log.Printf("Discovery server stopped: %v", err)
		}
	}()

	// Create nodes
	nodes := make([]*node.Node, numNodes)
	clients := make([]*discovery.DiscoveryClient, numNodes)

	fmt.Println("Creating nodes...")
	for i := range numNodes {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)

		// Create transport with visualization
		transport, err := protocol.NewTCPTransport(addr)
		if err != nil {
			log.Fatalf("Failed to create transport: %v", err)
		}

		// Wrap with instrumented transport
		instrTransport := visualizer.NewInstrumentedTransport(
			transport,
			fmt.Sprintf("node%d", i),
			vis,
		)

		// Create node
		peerManager := peer.NewPeerManager(3, 10*time.Second)
		config := node.Config{
			Addr:                addr,
			SyncInterval:        syncInterval,
			MaxSyncPeers:        2,
			MaxConsecutiveFails: 3,
			FailureTimeout:      10 * time.Second,
		}

		n, err := node.NewNode(config, instrTransport, peerManager)
		if err != nil {
			log.Fatalf("Failed to create node: %v", err)
		}
		nodes[i] = n

		// Set visualizer
		n.SetVisualizer(vis)

		// Create discovery client
		clients[i] = discovery.NewDiscoveryClient(discoveryAddr, n)
		if err := clients[i].Start(heartbeatInterval); err != nil {
			log.Fatalf("Failed to start discovery client: %v", err)
		}
	}

	// Give nodes time to discover each other
	fmt.Println("Waiting for discovery...")
	time.Sleep(2 * time.Second)

	// Perform operations
	fmt.Println("Performing operations...")
	for i := 0; i < 10; i++ {
		nodeIdx := i % numNodes
		if i%3 == 0 {
			nodes[nodeIdx].Decrement()
			fmt.Printf("Node %d decremented\n", nodeIdx)
		} else {
			nodes[nodeIdx].Increment()
			fmt.Printf("Node %d incremented\n", nodeIdx)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Create a partition between first 2 and last 3 nodes
	fmt.Println("Creating network partition...")
	for i := 0; i < 2; i++ {
		for j := 2; j < numNodes; j++ {
			nodes[i].GetPeerManager().RemovePeer(fmt.Sprintf("127.0.0.1:%d", basePort+j))
			nodes[j].GetPeerManager().RemovePeer(fmt.Sprintf("127.0.0.1:%d", basePort+i))
		}
	}

	// Perform operations during partition
	for i := 0; i < 6; i++ {
		if i < 3 {
			nodes[0].Increment()
			fmt.Println("Node 0 incremented (partition 1)")
		} else {
			nodes[3].Increment()
			fmt.Println("Node 3 incremented (partition 2)")
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Heal partition
	fmt.Println("Healing partition...")
	for i := 0; i < 2; i++ {
		for j := 2; j < numNodes; j++ {
			nodes[i].GetPeerManager().AddPeer(fmt.Sprintf("127.0.0.1:%d", basePort+j))
			nodes[j].GetPeerManager().AddPeer(fmt.Sprintf("127.0.0.1:%d", basePort+i))
		}
	}

	// Wait for convergence
	fmt.Println("Waiting for convergence...")
	time.Sleep(5 * time.Second)

	// Print final values
	fmt.Println("\nFinal counter values:")
	for i, n := range nodes {
		fmt.Printf("Node %d: %d\n", i, n.GetCounter())
	}

	// Generate timeline report
	report := vis.GenerateTimelineReport()
	fmt.Printf("\nGossip Timeline Report:\n%s\n", report)

	// Clean up
	fmt.Println("Shutting down...")
	for i := range numNodes {
		clients[i].Stop()
		nodes[i].Close()
	}
	discoveryServer.Stop()
}
