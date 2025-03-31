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

	"github.com/ogzhanolguncu/distributed-counter/part4/discovery"
	"github.com/ogzhanolguncu/distributed-counter/part4/node"
	"github.com/ogzhanolguncu/distributed-counter/part4/peer"
	"github.com/ogzhanolguncu/distributed-counter/part4/protocol"
)

const (
	defaultSyncInterval        = 2 * time.Second
	defaultMaxSyncPeers        = 2
	defaultHeartbeatInterval   = 1 * time.Second
	defaultMaxConsecutiveFails = 5
	defaultFailureTimeout      = 30 * time.Second
	defaultDiscoveryAddr       = "localhost:8000"
	defaultDataDir             = "data"
	defaultWALCleanupInterval  = 1 * time.Hour
	defaultWALMaxSegments      = 50
	defaultWALMinSegments      = 5
	defaultWALMaxAge           = 7 * 24 * time.Hour // 7 days
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
	dataDir := flag.String("data-dir", defaultDataDir, "Base directory for data storage")
	snapshotInterval := flag.Duration("snapshot-interval", 5*time.Minute, "How often to take a full snapshot of counter state")

	// WAL cleanup options
	walCleanupInterval := flag.Duration("wal-cleanup-interval", defaultWALCleanupInterval, "How often to run WAL segment cleanup")
	walMaxSegments := flag.Int("wal-max-segments", defaultWALMaxSegments, "Maximum number of WAL segments to keep")
	walMinSegments := flag.Int("wal-min-segments", defaultWALMinSegments, "Minimum number of WAL segments to always keep")
	walMaxAge := flag.Duration("wal-max-age", defaultWALMaxAge, "Maximum age of WAL segments to keep")

	// HTTP server options
	httpPort := flag.Int("http-port", 0, "HTTP server port (defaults to 8010 + (TCP port - 9000))")

	flag.Parse()

	// Create transport
	transport, err := protocol.NewTCPTransport(*addr)
	if err != nil {
		log.Fatalf("Failed to create transport: %v", err)
	}

	// Create peer manager
	peerManager := peer.NewPeerManager(*maxConsecutiveFails, *failureTimeout)

	// Create node configuration
	config := node.Config{
		Addr:                *addr,
		SyncInterval:        *syncInterval,
		FullSyncInterval:    *syncInterval * 10, // Default to 10x regular sync interval
		MaxSyncPeers:        *maxSyncPeers,
		MaxConsecutiveFails: *maxConsecutiveFails,
		FailureTimeout:      *failureTimeout,
		DataDir:             *dataDir,
		SnapshotInterval:    *snapshotInterval,
		// WAL cleanup settings
		WALCleanupInterval: *walCleanupInterval,
		WALMaxSegments:     *walMaxSegments,
		WALMinSegments:     *walMinSegments,
		WALMaxAge:          *walMaxAge,
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
	log.Printf("WAL enabled with storage directory: %s", filepath.Join(*dataDir, "wal", *addr))
	log.Printf("WAL cleanup configuration: max segments=%d, min segments=%d, cleanup interval=%v",
		*walMaxSegments, *walMinSegments, *walCleanupInterval)

	// Setup HTTP handlers for interaction
	setupHTTPHandlers(n, *httpPort)

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

func setupHTTPHandlers(n *node.Node, configuredHttpPort int) {
	http.HandleFunc("/increment", func(w http.ResponseWriter, r *http.Request) {
		n.Increment()
		fmt.Fprintf(w, "Counter incremented to %d\n", n.GetCounter())
	})

	http.HandleFunc("/decrement", func(w http.ResponseWriter, r *http.Request) {
		n.Decrement()
		fmt.Fprintf(w, "Counter decremented to %d\n", n.GetCounter())
	})

	http.HandleFunc("/counter", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Counter: %d\n", n.GetCounter())
	})

	http.HandleFunc("/local-counter", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Local Counter: %d\n", n.GetLocalCounter())
	})

	http.HandleFunc("/peers", func(w http.ResponseWriter, r *http.Request) {
		peers := n.GetPeerManager().GetPeers()
		fmt.Fprintf(w, "Peers (%d): %v\n", len(peers), peers)
	})

	go func() {
		httpPort := configuredHttpPort
		addr := n.GetAddr()

		// If HTTP port not explicitly set, calculate it
		if httpPort == 0 {
			var tcpPort int
			_, err := fmt.Sscanf(addr, "%*[^:]:%d", &tcpPort)
			if err == nil {
				httpPort = 8010 + (tcpPort - 9000)
			} else {
				httpPort = 8010 // Default if parsing fails
			}
		}

		// Always bind to 0.0.0.0 in Docker environment
		httpAddr := fmt.Sprintf("0.0.0.0:%d", httpPort)

		log.Printf("Starting HTTP server at %s", httpAddr)
		if err := http.ListenAndServe(httpAddr, nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()
}
