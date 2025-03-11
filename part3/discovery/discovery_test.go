package discovery

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part3/node"
	"github.com/ogzhanolguncu/distributed-counter/part3/peer"
	"github.com/ogzhanolguncu/distributed-counter/part3/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestNode(t *testing.T, addr string, syncInterval time.Duration) *node.Node {
	transport, err := protocol.NewTCPTransport(addr)
	require.NoError(t, err, "Failed to start TCP Transport")
	config := node.Config{
		Addr:                addr,
		SyncInterval:        syncInterval,
		MaxSyncPeers:        2,
		MaxConsecutiveFails: 1,
		FailureTimeout:      10 * time.Second,
	}
	peerManager := peer.NewPeerManager(1, 10*time.Second)
	node, err := node.NewNode(config, transport, peerManager)
	require.NoError(t, err)
	return node
}

func getFreePort() string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func waitForServer(addr string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
			conn, err := net.Dial("tcp", addr)
			if err == nil {
				conn.Close()
				return true
			}
		}
	}
}

func TestDiscoveryClientIntegration(t *testing.T) {
	serverAddr := getFreePort()
	cleanupInterval := 5 * time.Second
	server := NewDiscoveryServer(serverAddr, cleanupInterval)

	go func() {
		err := server.Start()
		if err != nil && err != http.ErrServerClosed {
			t.Errorf("Server failed: %v", err)
		}
	}()
	defer server.Stop()

	if !waitForServer(serverAddr, 5*time.Second) {
		t.Fatal("Server didn't start in time")
	}

	t.Run("Client registration and discovery", func(t *testing.T) {
		node1 := createTestNode(t, getFreePort(), 1*time.Second)
		node2 := createTestNode(t, getFreePort(), 1*time.Second)

		client1 := NewDiscoveryClient(serverAddr, node1)
		client2 := NewDiscoveryClient(serverAddr, node2)

		err := client1.Register()
		require.NoError(t, err)

		server.mu.Lock()
		_, exists := server.knownPeers[node1.GetAddr()]
		server.mu.Unlock()
		assert.True(t, exists, "Node1 should be registered")

		err = client2.Register()
		require.NoError(t, err)

		client2.discoverPeers()

		peers := node2.GetPeerManager().GetPeers()
		assert.Contains(t, peers, node1.GetAddr(), "Node2 should discover Node1")

		client1.Stop()
		client2.Stop()
	})

	t.Run("Heartbeat keeps peer alive", func(t *testing.T) {
		node1 := createTestNode(t, getFreePort(), 1*time.Second)

		client1 := NewDiscoveryClient(serverAddr, node1)

		err := client1.Register()
		require.NoError(t, err)

		heartbeatInterval := 100 * time.Millisecond
		client1.StartHeartbeat(heartbeatInterval)

		time.Sleep(heartbeatInterval * 5)

		server.mu.Lock()
		_, exists := server.knownPeers[node1.GetAddr()]
		server.mu.Unlock()
		assert.True(t, exists, "Node should still be registered after heartbeats")

		client1.Stop()

		shortServerAddr := getFreePort()
		shortCleanupInterval := 200 * time.Millisecond
		shortServer := NewDiscoveryServer(shortServerAddr, shortCleanupInterval)

		go func() {
			err := shortServer.Start()
			if err != nil && err != http.ErrServerClosed {
				t.Errorf("Short server failed: %v", err)
			}
		}()
		defer shortServer.Stop()

		if !waitForServer(shortServerAddr, 5*time.Second) {
			t.Fatal("Short server didn't start in time")
		}

		shortClient := NewDiscoveryClient(shortServerAddr, node1)
		err = shortClient.Register()
		require.NoError(t, err)

		shortServer.mu.Lock()
		_, exists = shortServer.knownPeers[node1.GetAddr()]
		shortServer.mu.Unlock()
		assert.True(t, exists, "Node should be registered with short server")

		time.Sleep(shortCleanupInterval * 3)

		shortServer.mu.Lock()
		_, exists = shortServer.knownPeers[node1.GetAddr()]
		shortServer.mu.Unlock()
		assert.False(t, exists, "Node should be removed after inactivity")
	})
}

func TestConcurrentRegistrations(t *testing.T) {
	serverAddr := getFreePort()
	cleanupInterval := 5 * time.Second
	server := NewDiscoveryServer(serverAddr, cleanupInterval)

	go func() {
		err := server.Start()
		if err != nil && err != http.ErrServerClosed {
			t.Errorf("Server failed: %v", err)
		}
	}()
	defer server.Stop()

	if !waitForServer(serverAddr, 5*time.Second) {
		t.Fatal("Server didn't start in time")
	}

	t.Run("Concurrent registrations", func(t *testing.T) {
		numPeers := 10
		nodes := make([]*node.Node, numPeers)
		clients := make([]*DiscoveryClient, numPeers)

		for i := range numPeers {
			nodes[i] = createTestNode(t, fmt.Sprintf("127.0.0.1:80%02d", i), 1*time.Second)
			clients[i] = NewDiscoveryClient(serverAddr, nodes[i])
		}

		var wg sync.WaitGroup
		wg.Add(numPeers)

		for i := range numPeers {
			go func(idx int) {
				defer wg.Done()
				err := clients[idx].Register()
				if err != nil {
					t.Errorf("Failed to register node %d: %v", idx, err)
				}
			}(i)
		}

		wg.Wait()

		server.mu.Lock()
		registeredCount := len(server.knownPeers)
		server.mu.Unlock()

		assert.Equal(t, numPeers, registeredCount, "All nodes should be registered")

		clients[0].discoverPeers()

		peerAddrs := nodes[0].GetPeerManager().GetPeers()

		log.Printf("Nodes own address %s\n", nodes[0].GetAddr())
		log.Printf("Nodes %+v", peerAddrs)

		assert.Equal(t, numPeers-1, len(peerAddrs), "Should discover all other peers")

		for i := range numPeers {
			clients[i].Stop()
		}
	})
}

func TestLateJoinerDiscovery(t *testing.T) {
	serverAddr := getFreePort()
	cleanupInterval := 5 * time.Second
	server := NewDiscoveryServer(serverAddr, cleanupInterval)

	go func() {
		err := server.Start()
		if err != nil && err != http.ErrServerClosed {
			t.Errorf("Server failed: %v", err)
		}
	}()
	defer server.Stop()

	if !waitForServer(serverAddr, 5*time.Second) {
		t.Fatal("Server didn't start in time")
	}

	numInitialNodes := 3
	initialNodes := make([]*node.Node, numInitialNodes)
	initialClients := make([]*DiscoveryClient, numInitialNodes)

	for i := range numInitialNodes {
		initialNodes[i] = createTestNode(t, getFreePort(), 1*time.Second)
		initialClients[i] = NewDiscoveryClient(serverAddr, initialNodes[i])

		err := initialClients[i].Register()
		require.NoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)

	for i := range numInitialNodes {
		initialClients[i].discoverPeers()
	}

	time.Sleep(1 * time.Second)

	for i := range numInitialNodes {
		peers := initialNodes[i].GetPeerManager().GetPeers()
		t.Logf("Node %d peers: %v", i, peers)
		assert.Equal(t, numInitialNodes-1, len(peers),
			fmt.Sprintf("Initial node %d should know about all other initial nodes", i))
	}

	lateNode := createTestNode(t, getFreePort(), 1*time.Second)
	lateNodeClient := NewDiscoveryClient(serverAddr, lateNode)

	err := lateNodeClient.Register()
	require.NoError(t, err)

	lateNodeClient.discoverPeers()

	lateNode.Increment()
	time.Sleep(2 * time.Second)

	lateNodeAddr := lateNode.GetAddr()
	lateJoinerFound := false

	for i := range numInitialNodes {
		peers := initialNodes[i].GetPeerManager().GetPeers()
		t.Logf("Node %d peers after late joiner: %v", i, peers)

		if slices.Contains(peers, lateNodeAddr) {
			lateJoinerFound = true
			break
		}
	}

	assert.True(t, lateJoinerFound, "At least one initial node should have added the late joiner")
}
