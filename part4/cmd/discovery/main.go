package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part3/discovery"
)

const (
	defaultPort        = 8000
	defaultCleanupTime = 5 * time.Second
)

func main() {
	port := flag.Int("port", defaultPort, "Port to listen on")
	cleanupInterval := flag.Duration("cleanup", defaultCleanupTime, "Interval to clean up inactive peers")
	flag.Parse()

	addr := fmt.Sprintf(":%d", *port)
	discoveryServer := discovery.NewDiscoveryServer(addr, *cleanupInterval)

	log.Printf("Starting discovery server at %s\n", addr)
	log.Printf("Cleanup interval: %v\n", *cleanupInterval)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down discovery server...")
		if err := discoveryServer.Stop(); err != nil {
			log.Printf("Error stopping discovery server: %v", err)
		} else {
			log.Println("Discovery server stopped successfully")
		}
		os.Exit(0)
	}()

	// Start server
	if err := discoveryServer.Start(); err != nil {
		log.Fatalf("Discovery server failed: %v", err)
	}
}
