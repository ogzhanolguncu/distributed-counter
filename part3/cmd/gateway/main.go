package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultGatewayPort   = 8080
	defaultDiscoveryAddr = "discovery:8000"
)

type NodeInfo struct {
	Addr string `json:"addr"`
}

type GatewayConfig struct {
	Port            int
	DiscoveryAddr   string
	RefreshInterval time.Duration
}

type APIGateway struct {
	config        GatewayConfig
	nodeEndpoints map[string]string // Maps node ID to HTTP endpoint
	mu            sync.RWMutex
	httpClient    *http.Client
	done          chan struct{}
}

func NewAPIGateway(config GatewayConfig) *APIGateway {
	return &APIGateway{
		config:        config,
		nodeEndpoints: make(map[string]string),
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		done:          make(chan struct{}),
	}
}

func (g *APIGateway) Start() error {
	// Initial discovery of nodes - retry a few times to ensure we have nodes
	maxRetries := 5
	retryDelay := time.Second * 2
	var err error
	var nodeCount int

	for i := 0; i < maxRetries; i++ {
		err = g.refreshNodeList()
		g.mu.RLock()
		nodeCount = len(g.nodeEndpoints)
		g.mu.RUnlock()

		if err == nil && nodeCount > 0 {
			log.Printf("Successfully discovered %d nodes", nodeCount)
			break
		}

		log.Printf("Attempt %d: Node discovery found %d nodes, retrying in %v...",
			i+1, nodeCount, retryDelay)
		time.Sleep(retryDelay)
	}

	if nodeCount == 0 {
		log.Printf("WARNING: Could not discover any nodes after %d attempts", maxRetries)
	}

	// Start periodic refresh of nodes
	go g.periodicRefresh()

	// Set up HTTP handlers
	http.HandleFunc("/", g.handleRoot)
	http.HandleFunc("/counter", g.handleCounter)
	http.HandleFunc("/increment", g.handleIncrement)
	http.HandleFunc("/decrement", g.handleDecrement)
	http.HandleFunc("/nodes", g.handleNodes)
	http.HandleFunc("/health", g.handleHealth)

	// Listen on configured port
	addr := fmt.Sprintf("0.0.0.0:%d", g.config.Port)
	log.Printf("API Gateway listening on %s", addr)
	return http.ListenAndServe(addr, nil)
}

func (g *APIGateway) Stop() {
	close(g.done)
}

func (g *APIGateway) periodicRefresh() {
	ticker := time.NewTicker(g.config.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := g.refreshNodeList(); err != nil {
				log.Printf("Failed to refresh node list: %v", err)
			}
		case <-g.done:
			return
		}
	}
}

func (g *APIGateway) refreshNodeList() error {
	// Query discovery server for node list
	url := fmt.Sprintf("http://%s/peers", g.config.DiscoveryAddr)
	resp, err := g.httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("failed to query discovery server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("discovery server returned status code %d", resp.StatusCode)
	}

	var nodes []NodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return fmt.Errorf("failed to decode node list: %w", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Clear existing nodes
	clear(g.nodeEndpoints)

	// Add new nodes
	for i, node := range nodes {
		// Extract node ID from address
		parts := strings.Split(node.Addr, ":")
		if len(parts) != 2 {
			log.Printf("WARNING: Invalid node address format: %s", node.Addr)
			continue
		}

		nodeHost := parts[0]
		nodePort := 8010 + i // Assuming ports are aligned with our configuration

		// Add node to our mapping with its HTTP endpoint
		nodeID := fmt.Sprintf("node%d", i+1)
		g.nodeEndpoints[nodeID] = fmt.Sprintf("http://%s:%d", nodeHost, nodePort)
	}

	log.Printf("Refreshed node list, found %d nodes", len(g.nodeEndpoints))
	return nil
}

func (g *APIGateway) handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"service": "Distributed Counter API Gateway",
		"endpoints": []string{
			"/counter - Get counter value",
			"/increment - Increment counter",
			"/decrement - Decrement counter",
			"/nodes - List available nodes",
			"/health - Service health check",
		},
	})
}

func (g *APIGateway) handleNodes(w http.ResponseWriter, r *http.Request) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes := make([]string, 0, len(g.nodeEndpoints))
	for nodeID := range g.nodeEndpoints {
		nodes = append(nodes, nodeID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"nodes": nodes,
		"count": len(nodes),
	})
}

func (g *APIGateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	g.mu.RLock()
	nodeCount := len(g.nodeEndpoints)
	g.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")

	if nodeCount == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "unhealthy",
			"reason": "no available nodes",
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "healthy",
		"node_count": nodeCount,
	})
}

func (g *APIGateway) handleCounter(w http.ResponseWriter, r *http.Request) {
	// Extract node ID from query param if specified
	nodeID := r.URL.Query().Get("node")

	// Get the endpoint for the specified node or a random node
	endpoint, err := g.getNodeEndpoint(nodeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Forward the request to the selected node
	g.forwardRequest(w, r, endpoint+"/counter")
}

func (g *APIGateway) handleIncrement(w http.ResponseWriter, r *http.Request) {
	// Extract node ID from query param if specified
	nodeID := r.URL.Query().Get("node")

	// Get the endpoint for the specified node or a random node
	endpoint, err := g.getNodeEndpoint(nodeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Forward the request to the selected node
	g.forwardRequest(w, r, endpoint+"/increment")
}

func (g *APIGateway) handleDecrement(w http.ResponseWriter, r *http.Request) {
	// Extract node ID from query param if specified
	nodeID := r.URL.Query().Get("node")

	// Get the endpoint for the specified node or a random node
	endpoint, err := g.getNodeEndpoint(nodeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Forward the request to the selected node
	g.forwardRequest(w, r, endpoint+"/decrement")
}

func (g *APIGateway) getNodeEndpoint(nodeID string) (string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.nodeEndpoints) == 0 {
		return "", fmt.Errorf("no available nodes")
	}

	if nodeID != "" {
		// Return the specific node if requested
		if endpoint, exists := g.nodeEndpoints[nodeID]; exists {
			return endpoint, nil
		}
		return "", fmt.Errorf("node %s not found", nodeID)
	}

	// Select a random node for load balancing
	nodeIDs := make([]string, 0, len(g.nodeEndpoints))
	for id := range g.nodeEndpoints {
		nodeIDs = append(nodeIDs, id)
	}

	randomIndex := rand.Intn(len(nodeIDs))
	selectedNodeID := nodeIDs[randomIndex]
	return g.nodeEndpoints[selectedNodeID], nil
}

func (g *APIGateway) forwardRequest(w http.ResponseWriter, r *http.Request, targetURL string) {
	// Parse the target URL
	target, err := url.Parse(targetURL)
	if err != nil {
		http.Error(w, "Invalid target URL: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(target)

	// Set up error handler to catch node errors
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Origin-Host", target.Host)
	}

	// Add error handler
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Error proxying request to %s: %v", target.Host, err)

		// If there's an error, try a different node
		g.mu.RLock()
		if len(g.nodeEndpoints) <= 1 {
			g.mu.RUnlock()
			http.Error(w, "No available nodes to handle request", http.StatusServiceUnavailable)
			return
		}

		// Get another node (not the one that just failed)
		var alternateEndpoint string
		for _, endpoint := range g.nodeEndpoints {
			if !strings.Contains(endpoint, target.Host) {
				alternateEndpoint = endpoint
				break
			}
		}
		g.mu.RUnlock()

		if alternateEndpoint != "" {
			log.Printf("Retrying request with alternate node: %s", alternateEndpoint)

			// Create the appropriate path
			alternatePath := ""
			if strings.HasSuffix(r.URL.Path, "/counter") {
				alternatePath = alternateEndpoint + "/counter"
			} else if strings.HasSuffix(r.URL.Path, "/increment") {
				alternatePath = alternateEndpoint + "/increment"
			} else if strings.HasSuffix(r.URL.Path, "/decrement") {
				alternatePath = alternateEndpoint + "/decrement"
			} else {
				alternatePath = alternateEndpoint + r.URL.Path
			}

			// Forward to alternate node
			g.forwardRequest(w, r, alternatePath)
			return
		}

		http.Error(w, "Error communicating with node: "+err.Error(), http.StatusBadGateway)
	}

	// Update request URL
	r.URL.Host = target.Host
	r.URL.Scheme = target.Scheme
	r.URL.Path = target.Path

	// Forward the request
	proxy.ServeHTTP(w, r)
}

func main() {
	port := flag.Int("port", defaultGatewayPort, "Gateway HTTP port")
	discoveryAddr := flag.String("discovery", defaultDiscoveryAddr, "Discovery server address")
	refreshInterval := flag.Duration("refresh", 10*time.Second, "Node list refresh interval")
	flag.Parse()

	config := GatewayConfig{
		Port:            *port,
		DiscoveryAddr:   *discoveryAddr,
		RefreshInterval: *refreshInterval,
	}

	gateway := NewAPIGateway(config)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down API Gateway...")
		gateway.Stop()
		os.Exit(0)
	}()

	log.Printf("Starting API Gateway with discovery server at %s", config.DiscoveryAddr)
	if err := gateway.Start(); err != nil {
		log.Fatalf("Gateway failed: %v", err)
	}
}
