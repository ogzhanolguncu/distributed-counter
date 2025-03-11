package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part3/discovery"
	"github.com/ogzhanolguncu/distributed-counter/part3/node"
	"github.com/ogzhanolguncu/distributed-counter/part3/peer"
	"github.com/ogzhanolguncu/distributed-counter/part3/protocol"
)

const (
	defaultSyncInterval        = 2 * time.Second
	defaultMaxSyncPeers        = 2
	defaultHeartbeatInterval   = 1 * time.Second
	defaultMaxConsecutiveFails = 5
	defaultFailureTimeout      = 30 * time.Second
	defaultDiscoveryAddr       = "localhost:8000"
	defaultWalBaseDir          = "data/wal"
	defaultMaxWalFileSize      = 64 * 1024 * 1024 // 64MB
)

func main() {
	// Parse command-line flags
	addr := flag.String("addr", "localhost:9000", "Node address to listen on")
	discoveryAddr := flag.String("discovery", defaultDiscoveryAddr, "Discovery server address")
	syncInterval := flag.Duration("sync", defaultSyncInterval, "Gossip sync interval")
	maxSyncPeers := flag.Int("max-peers", defaultMaxSyncPeers, "Maximum number of peers to sync with")
	heartbeatInterval := flag.Duration("heartbeat", defaultHeartbeatInterval, "Heartbeat interval to discovery server")
	maxConsecutiveFails := flag.Int("max-fails", defaultMaxConsecutiveFails, "Maximum consecutive fails before marking peer as inactive")
	failureTimeout := flag.Duration("failure-timeout", defaultFailureTimeout, "Timeout before marking peer as inactive")

	// WAL options
	enableWal := flag.Bool("wal", true, "Enable Write-Ahead Log")
	enableWalFsync := flag.Bool("wal-fsync", true, "Enable immediate fsync for WAL")
	walDir := flag.String("wal-dir", "", "WAL directory (defaults to data/wal/<node-addr>)")
	maxWalFileSize := flag.Int64("wal-max-size", defaultMaxWalFileSize, "Maximum WAL file size before rotation")

	flag.Parse()

	// If WAL directory not specified, use default
	nodeWalDir := *walDir
	if nodeWalDir == "" && *enableWal {
		// Convert addr to a valid directory name
		dirName := *addr
		if dirName == "localhost:9000" {
			dirName = "node-default"
		} else {
			dirName = "node-" + filepath.Base(dirName)
		}
		nodeWalDir = filepath.Join(defaultWalBaseDir, dirName)
	}

	// Create transport
	transport, err := protocol.NewTCPTransport(*addr)
	if err != nil {
		log.Fatalf("Failed to create transport: %v", err)
	}

	// Create peer manager
	peerManager := peer.NewPeerManager(*maxConsecutiveFails, *failureTimeout)

	// Create node
	config := node.Config{
		Addr:                *addr,
		SyncInterval:        *syncInterval,
		MaxSyncPeers:        *maxSyncPeers,
		MaxConsecutiveFails: *maxConsecutiveFails,
		FailureTimeout:      *failureTimeout,
		WalDir:              nodeWalDir,
		EnableWal:           *enableWal,
		EnableWalFsync:      *enableWalFsync,
		MaxWalFileSize:      *maxWalFileSize,
	}

	n, err := node.NewNode(config, transport, peerManager)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	// Register with discovery service
	discoveryClient := discovery.NewDiscoveryClient(*discoveryAddr, n)
	if err := discoveryClient.Start(*heartbeatInterval); err != nil {
		log.Fatalf("Failed to start discovery client: %v", err)
	}

	log.Printf("Node started at %s and registered with discovery server at %s", *addr, *discoveryAddr)
	if *enableWal {
		log.Printf("WAL enabled at directory: %s", nodeWalDir)
	}

	// Setup HTTP handlers for interaction
	setupHTTPHandlers(n)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("Shutting down node...")
	discoveryClient.Stop()
	if err := n.Close(); err != nil {
		log.Printf("Error closing node: %v", err)
	} else {
		log.Println("Node shut down successfully")
	}
}

func setupHTTPHandlers(n *node.Node) {
	http.HandleFunc("/increment", func(w http.ResponseWriter, r *http.Request) {
		n.Increment()
		fmt.Fprintf(w, "Counter incremented to %d (version %d)\n", n.GetCounter(), n.GetVersion())
	})

	http.HandleFunc("/decrement", func(w http.ResponseWriter, r *http.Request) {
		n.Decrement()
		fmt.Fprintf(w, "Counter decremented to %d (version %d)\n", n.GetCounter(), n.GetVersion())
	})

	http.HandleFunc("/counter", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Counter: %d (version %d)\n", n.GetCounter(), n.GetVersion())
	})

	http.HandleFunc("/peers", func(w http.ResponseWriter, r *http.Request) {
		peers := n.GetPeerManager().GetPeers()
		fmt.Fprintf(w, "Peers (%d): %v\n", len(peers), peers)
	})

	go func() {
		addr := n.GetAddr()
		httpPort := 8080
		if _, err := fmt.Sscanf(addr, "localhost:%d", &httpPort); err == nil {
			httpPort = 8010 + (httpPort - 9000)
		}
		httpAddr := fmt.Sprintf(":%d", httpPort)
		log.Printf("Starting HTTP server at %s", httpAddr)
		if err := http.ListenAndServe(httpAddr, nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()
}
