package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part2/discovery"
	"github.com/ogzhanolguncu/distributed-counter/part2/node"
	"github.com/ogzhanolguncu/distributed-counter/part2/peer"
	"github.com/ogzhanolguncu/distributed-counter/part2/protocol"
)

const (
	numNodes            = 3
	syncInterval        = 2 * time.Second
	maxSyncPeers        = 2
	basePort            = 9000
	discoveryServerPort = 8000
	heartbeatInterval   = 1 * time.Second
	cleanupInterval     = 5 * time.Second
	maxConsecutiveFails = 5
	failureTimeout      = 30 * time.Second
)

func main() {
	discoveryServerAddr := fmt.Sprintf("127.0.0.1:%d", discoveryServerPort)
	discoveryServer := discovery.NewDiscoveryServer(discoveryServerAddr, cleanupInterval)

	fmt.Printf("Starting discovery server at %s\n", discoveryServerAddr)
	go func() {
		if err := discoveryServer.Start(); err != nil {
			log.Fatalf("Discovery server failed: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	nodes := make([]*node.Node, numNodes)
	discoveryClients := make([]*discovery.DiscoveryClient, numNodes)

	fmt.Println("Starting distributed counter with", numNodes, "nodes")
	for i := range numNodes {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		transport, err := protocol.NewTCPTransport(addr)
		if err != nil {
			log.Fatalf("Failed to create transport for node %d: %v", i, err)
		}

		peerManager := peer.NewPeerManager(maxConsecutiveFails, failureTimeout)
		config := node.Config{
			Addr:                addr,
			SyncInterval:        syncInterval,
			MaxSyncPeers:        maxSyncPeers,
			MaxConsecutiveFails: maxConsecutiveFails,
			FailureTimeout:      failureTimeout,
		}

		n, err := node.NewNode(config, transport, peerManager)
		if err != nil {
			log.Fatalf("Failed to create node %d: %v", i, err)
		}

		nodes[i] = n
		discoveryClients[i] = discovery.NewDiscoveryClient(discoveryServerAddr, n)

		if err := discoveryClients[i].Start(heartbeatInterval); err != nil {
			log.Fatalf("Failed to start discovery client for node %d: %v", i, err)
		}

		fmt.Printf("Node %d started at %s and registered with discovery server\n", i, addr)
	}

	fmt.Println("\nInitial peer connections:")
	for i, n := range nodes {
		peers := n.GetPeerManager().GetPeers()
		fmt.Printf("Node %d peers: %v\n", i, peers)
	}

	fmt.Println("\nIncrementing counter on node 0...")
	for range 5 {
		nodes[0].Increment()
		fmt.Printf("Node 0 incremented counter to %d (version %d)\n",
			nodes[0].GetCounter(), nodes[0].GetVersion())
		time.Sleep(4 * time.Second)
	}

	fmt.Println("\nWaiting for gossip propagation...")
	time.Sleep(syncInterval * 5)

	fmt.Println("\nFinal counter values:")
	for i, n := range nodes {
		fmt.Printf("Node %d counter: %d (version %d)\n", i, n.GetCounter(), n.GetVersion())
	}

	gracefulShutdown(nodes, discoveryClients, discoveryServer)
}

func gracefulShutdown(nodes []*node.Node, clients []*discovery.DiscoveryClient, server *discovery.DiscoveryServer) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived signal %v, initiating graceful shutdown...\n", sig)
	case <-time.After(30 * time.Second):
		fmt.Println("\nTimeout reached, initiating graceful shutdown...")
	}

	fmt.Println("Stopping discovery clients...")
	for i, client := range clients {
		client.Stop()
		fmt.Printf("Discovery client %d stopped\n", i)
	}

	fmt.Println("Closing all node connections...")
	for i, n := range nodes {
		if err := n.Close(); err != nil {
			log.Printf("Error closing node %d: %v", i, err)
		} else {
			fmt.Printf("Node %d shut down successfully\n", i)
		}
	}

	fmt.Println("Stopping discovery server...")
	if err := server.Stop(); err != nil {
		log.Printf("Error stopping discovery server: %v", err)
	} else {
		fmt.Println("Discovery server stopped successfully")
	}

	fmt.Println("All components shut down successfully")
}
