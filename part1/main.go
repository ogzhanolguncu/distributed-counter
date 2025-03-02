package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part1/node"
	"github.com/ogzhanolguncu/distributed-counter/part1/peer"
	"github.com/ogzhanolguncu/distributed-counter/part1/protocol"
)

const (
	numNodes     = 3
	syncInterval = 2 * time.Second
	maxSyncPeers = 2
	basePort     = 9000
)

func main() {
	nodes := make([]*node.Node, numNodes)

	fmt.Println("Starting distributed counter with", numNodes, "nodes")

	for i := range numNodes {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)

		transport, err := protocol.NewTCPTransport(addr)
		if err != nil {
			log.Fatalf("Failed to create transport for node %d: %v", i, err)
		}
		peerManager := peer.NewPeerManager()
		config := node.Config{
			Addr:         addr,
			SyncInterval: syncInterval,
			MaxSyncPeers: maxSyncPeers,
		}

		n, err := node.NewNode(config, transport, peerManager)
		if err != nil {
			log.Fatalf("Failed to create node %d: %v", i, err)
		}

		nodes[i] = n
		fmt.Printf("Node %d started at %s\n", i, addr)
	}

	fmt.Println("Building network topology...")

	for i, node := range nodes {
		for j := range numNodes {
			if i != j { // Don't add itself as a peer
				peerAddr := fmt.Sprintf("127.0.0.1:%d", basePort+j)
				node.GetPeerManager().AddPeer(peerAddr)
				fmt.Printf("Node %d connected to peer at %s\n", i, peerAddr)
			}
		}
	}

	// Increment counter on first node a few times
	fmt.Println("\nIncrementing counter on node 0...")
	for range 5 {
		nodes[0].Increment()
		fmt.Printf("Node 0 incremented counter to %d\n", nodes[0].GetCounter())
		time.Sleep(4 * time.Second)
	}

	fmt.Println("\nWaiting for gossip propagation...")
	time.Sleep(syncInterval * 5)

	fmt.Println("\nFinal counter values:")
	for i, n := range nodes {
		fmt.Printf("Node %d counter: %d\n", i, n.GetCounter())
	}

	gracefulShutdown(nodes)
}

func gracefulShutdown(nodes []*node.Node) {
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived signal %v, initiating graceful shutdown...\n", sig)
	case <-time.After(30 * time.Second):
		fmt.Println("\nTimeout reached, initiating graceful shutdown...")
	}

	fmt.Println("Closing all node connections...")

	for i, n := range nodes {
		if err := n.Close(); err != nil {
			log.Printf("Error closing node %d: %v", i, err)
		} else {
			fmt.Printf("Node %d shut down successfully\n", i)
		}
	}

	fmt.Println("All nodes shut down successfully")
}
