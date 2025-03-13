package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
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
	InitialNodes    []string
	FixedHttpPort   int
	UseServiceNames bool
}

type NodeEndpoint struct {
	NodeID       string
	TcpAddress   string
	HttpEndpoint string
	HttpPort     int
}

type APIGateway struct {
	config        GatewayConfig
	nodeEndpoints map[string]*NodeEndpoint
	mu            sync.RWMutex
	httpClient    *http.Client
	done          chan struct{}
}

func NewAPIGateway(config GatewayConfig) *APIGateway {
	return &APIGateway{
		config:        config,
		nodeEndpoints: make(map[string]*NodeEndpoint),
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		done:          make(chan struct{}),
	}
}

func (g *APIGateway) Start() error {
	if len(g.config.InitialNodes) > 0 {
		g.initializeFromStaticConfig()
	}

	// Initial discovery of nodes - retry a few times to ensure we have nodes
	maxRetries := 5
	retryDelay := time.Second * 2
	var err error
	var nodeCount int

	for i := range maxRetries {
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

	// If we couldn't discover any nodes but have initial nodes, that's okay
	g.mu.RLock()
	hasNodes := len(g.nodeEndpoints) > 0
	g.mu.RUnlock()

	if !hasNodes {
		log.Printf("WARNING: Could not discover any nodes after %d attempts", maxRetries)
	}

	g.validateNodeConnectivity()

	go g.periodicRefresh()

	http.HandleFunc("/", g.handleRoot)
	http.HandleFunc("/counter", g.handleCounter)
	http.HandleFunc("/increment", g.handleIncrement)
	http.HandleFunc("/decrement", g.handleDecrement)
	http.HandleFunc("/nodes", g.handleNodes)
	http.HandleFunc("/health", g.handleHealth)

	addr := fmt.Sprintf("0.0.0.0:%d", g.config.Port)
	log.Printf("API Gateway listening on %s", addr)
	return http.ListenAndServe(addr, nil)
}

// Initialize from static configuration (useful for Docker Compose environments)
func (g *APIGateway) initializeFromStaticConfig() {
	g.mu.Lock()
	defer g.mu.Unlock()

	for i, nodeAddr := range g.config.InitialNodes {
		// Extract node host and port
		parts := strings.Split(nodeAddr, ":")
		if len(parts) != 2 {
			log.Printf("WARNING: Invalid node address format in initial config: %s", nodeAddr)
			continue
		}

		nodeHost := parts[0]
		tcpPortStr := parts[1]
		tcpPort, err := strconv.Atoi(tcpPortStr)
		if err != nil {
			log.Printf("WARNING: Invalid TCP port in initial config: %s", tcpPortStr)
			continue
		}

		// Determine HTTP port
		var httpPort int
		if g.config.FixedHttpPort > 0 {
			httpPort = g.config.FixedHttpPort + i
		} else {
			httpPort = 8010 + (tcpPort - 9000)
		}

		// Use service name if configured
		httpHost := nodeHost
		if g.config.UseServiceNames {
			httpHost = fmt.Sprintf("node%d", i+1)
		}

		nodeID := fmt.Sprintf("node%d", i+1)
		httpEndpoint := fmt.Sprintf("http://%s:%d", httpHost, httpPort)

		g.nodeEndpoints[nodeID] = &NodeEndpoint{
			NodeID:       nodeID,
			TcpAddress:   nodeAddr,
			HttpEndpoint: httpEndpoint,
			HttpPort:     httpPort,
		}

		log.Printf("Initialized node %s with endpoint %s (TCP: %s, HTTP port: %d)",
			nodeID, httpEndpoint, nodeAddr, httpPort)
	}

	log.Printf("Initialized %d nodes from static configuration", len(g.nodeEndpoints))
}

func (g *APIGateway) validateNodeConnectivity() {
	g.mu.RLock()
	defer g.mu.RUnlock()

	log.Printf("Validating connectivity to %d nodes", len(g.nodeEndpoints))

	for _, node := range g.nodeEndpoints {
		// Test the counter endpoint to verify connectivity
		testURL := node.HttpEndpoint + "/counter"
		log.Printf("Testing connectivity to node %s at %s", node.NodeID, testURL)

		resp, err := g.httpClient.Get(testURL)
		if err != nil {
			log.Printf("WARNING: Node %s at %s is not reachable: %v",
				node.NodeID, testURL, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("WARNING: Node %s returned status %d: %s",
				node.NodeID, resp.StatusCode, string(body))
		} else {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("Node %s is healthy. Response: %s", node.NodeID, string(body))
		}
	}
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
	if len(g.config.InitialNodes) > 0 && g.config.RefreshInterval == 0 {
		log.Printf("Using static node configuration, skipping discovery")
		return nil
	}

	// Query discovery server for node list
	discoveryURL := fmt.Sprintf("http://%s/peers", g.config.DiscoveryAddr)
	log.Printf("Querying discovery server at %s", discoveryURL)

	resp, err := g.httpClient.Get(discoveryURL)
	if err != nil {
		return fmt.Errorf("failed to query discovery server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("discovery server returned status code %d: %s", resp.StatusCode, string(body))
	}

	var nodes []NodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return fmt.Errorf("failed to decode node list: %w", err)
	}

	// Log raw node list for debugging
	log.Printf("Raw node list from discovery: %+v", nodes)

	g.mu.Lock()
	defer g.mu.Unlock()

	// Store existing nodes to check for changes
	existingEndpoints := make(map[string]string)
	for id, node := range g.nodeEndpoints {
		existingEndpoints[id] = node.HttpEndpoint
	}

	// Clear existing nodes if we're not merging with static configuration
	if len(g.config.InitialNodes) == 0 {
		clear(g.nodeEndpoints)
	}

	// Add new nodes with correct HTTP port mapping
	for i, node := range nodes {
		// Extract node ID and port from address
		parts := strings.Split(node.Addr, ":")
		if len(parts) != 2 {
			log.Printf("WARNING: Invalid node address format: %s", node.Addr)
			continue
		}

		nodeHost := parts[0]
		tcpPort := parts[1]

		// Calculate HTTP port using the same logic as in counter nodes
		var httpPort int
		tcpPortNum, err := strconv.Atoi(tcpPort)
		if err != nil {
			log.Printf("WARNING: Failed to parse TCP port %s: %v", tcpPort, err)
			// Fall back to index-based port assignment
			httpPort = 8010 + i
		} else {
			// Use fixed port if configured, otherwise calculate
			if g.config.FixedHttpPort > 0 {
				httpPort = g.config.FixedHttpPort + i
			} else {
				httpPort = 8010 + (tcpPortNum - 9000)
			}
		}

		httpHost := nodeHost
		if g.config.UseServiceNames || nodeHost == "127.0.0.1" || nodeHost == "localhost" {
			httpHost = fmt.Sprintf("node%d", i+1)
		}

		// Add or update node
		nodeID := fmt.Sprintf("node%d", i+1)
		httpEndpoint := fmt.Sprintf("http://%s:%d", httpHost, httpPort)

		g.nodeEndpoints[nodeID] = &NodeEndpoint{
			NodeID:       nodeID,
			TcpAddress:   node.Addr,
			HttpEndpoint: httpEndpoint,
			HttpPort:     httpPort,
		}

		if oldEndpoint, exists := existingEndpoints[nodeID]; exists && oldEndpoint != httpEndpoint {
			log.Printf("Updated node %s endpoint from %s to %s",
				nodeID, oldEndpoint, httpEndpoint)
		} else if !exists {
			log.Printf("Added node %s with endpoint %s (TCP: %s, HTTP port: %d)",
				nodeID, httpEndpoint, node.Addr, httpPort)
		}
	}

	log.Printf("Refreshed node list, now have %d nodes", len(g.nodeEndpoints))
	return nil
}

func (g *APIGateway) handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
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

	// Build response with full node information
	nodeInfo := make(map[string]map[string]string)
	for id, node := range g.nodeEndpoints {
		nodeInfo[id] = map[string]string{
			"tcp_address":   node.TcpAddress,
			"http_endpoint": node.HttpEndpoint,
			"http_port":     strconv.Itoa(node.HttpPort),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"nodes": nodeInfo,
		"count": len(nodeInfo),
	})
}

func (g *APIGateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	g.mu.RLock()
	nodeCount := len(g.nodeEndpoints)
	g.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")

	if nodeCount == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{
			"status": "unhealthy",
			"reason": "no available nodes",
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"status":     "healthy",
		"node_count": nodeCount,
	})
}

func (g *APIGateway) handleCounter(w http.ResponseWriter, r *http.Request) {
	// Get a node endpoint and forward the request
	nodeID := r.URL.Query().Get("node")
	node, err := g.getNode(nodeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	g.forwardRequest(w, r, node.HttpEndpoint+"/counter")
}

func (g *APIGateway) handleIncrement(w http.ResponseWriter, r *http.Request) {
	// Get a node endpoint and forward the request
	nodeID := r.URL.Query().Get("node")
	node, err := g.getNode(nodeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	g.forwardRequest(w, r, node.HttpEndpoint+"/increment")
}

func (g *APIGateway) handleDecrement(w http.ResponseWriter, r *http.Request) {
	// Get a node endpoint and forward the request
	nodeID := r.URL.Query().Get("node")
	node, err := g.getNode(nodeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	g.forwardRequest(w, r, node.HttpEndpoint+"/decrement")
}

func (g *APIGateway) getNode(nodeID string) (*NodeEndpoint, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.nodeEndpoints) == 0 {
		return nil, fmt.Errorf("no available nodes")
	}

	if nodeID != "" {
		// Return the specific node if requested
		if node, exists := g.nodeEndpoints[nodeID]; exists {
			log.Printf("Using requested node %s with endpoint %s", nodeID, node.HttpEndpoint)
			return node, nil
		}
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// Select a random node for load balancing
	nodeIDs := make([]string, 0, len(g.nodeEndpoints))
	for id := range g.nodeEndpoints {
		nodeIDs = append(nodeIDs, id)
	}

	randomIndex := rand.Intn(len(nodeIDs))
	selectedNodeID := nodeIDs[randomIndex]
	selectedNode := g.nodeEndpoints[selectedNodeID]
	log.Printf("Selected random node %s with endpoint %s", selectedNodeID, selectedNode.HttpEndpoint)
	return selectedNode, nil
}

// Forward request using reverse proxy
func (g *APIGateway) forwardRequest(w http.ResponseWriter, r *http.Request, targetURL string) {
	// Parse the target URL
	target, err := url.Parse(targetURL)
	if err != nil {
		http.Error(w, "Invalid target URL: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Forwarding request to target: %s", targetURL)

	// Create a new request to ensure path is correct
	outReq := new(http.Request)
	*outReq = *r
	outReq.URL = target
	outReq.Host = target.Host

	// Ensure path is correct (no duplication)
	log.Printf("Request path: %s", outReq.URL.Path)

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(target)

	// Set up custom director to ensure path is correct
	// This is the key to fixing the path duplication issue
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)

		// Override the path to exactly match the target path
		req.URL.Path = target.Path

		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Origin-Host", target.Host)

		log.Printf("Request URL after director: %v", req.URL)
	}

	// Add error handler
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Error proxying request to %s: %v", target.Host, err)

		// Try a direct request as a diagnostic
		directReq, _ := http.NewRequest(r.Method, targetURL, nil)
		directResp, directErr := g.httpClient.Do(directReq)

		if directErr != nil {
			log.Printf("Direct request failed: %v", directErr)
		} else {
			defer directResp.Body.Close()
			body, _ := io.ReadAll(directResp.Body)
			log.Printf("Direct request succeeded: %d %s", directResp.StatusCode, body)
		}

		// Try another node if this one fails
		g.mu.RLock()
		alternateNodeID := ""
		currentTargetHost := target.Host

		// Find another node to try
		for id, node := range g.nodeEndpoints {
			if !strings.Contains(node.HttpEndpoint, currentTargetHost) {
				alternateNodeID = id
				break
			}
		}
		g.mu.RUnlock()

		if alternateNodeID != "" {
			log.Printf("Retrying with alternate node: %s", alternateNodeID)
			alternateNode, _ := g.getNode(alternateNodeID)

			// Determine the endpoint path
			path := "/counter" // default
			if strings.HasSuffix(targetURL, "/increment") {
				path = "/increment"
			} else if strings.HasSuffix(targetURL, "/decrement") {
				path = "/decrement"
			}

			g.forwardRequest(w, r, alternateNode.HttpEndpoint+path)
			return
		}

		http.Error(w, "Error forwarding request: "+err.Error(), http.StatusBadGateway)
	}

	// Forward the request
	proxy.ServeHTTP(w, r)
}

func main() {
	port := flag.Int("port", defaultGatewayPort, "Gateway HTTP port")
	discoveryAddr := flag.String("discovery", defaultDiscoveryAddr, "Discovery server address")
	refreshInterval := flag.Duration("refresh", 10*time.Second, "Node list refresh interval (0 for static config only)")
	useServiceNames := flag.Bool("use-service-names", true, "Use service names instead of IPs for HTTP endpoints")
	fixedHttpPort := flag.Int("fixed-http-port", 0, "Fixed base HTTP port for nodes (0 to calculate from TCP port)")
	initialNodesStr := flag.String("initial-nodes", "", "Comma-separated list of initial nodes (tcp addresses)")

	flag.Parse()

	var initialNodes []string
	if *initialNodesStr != "" {
		initialNodes = strings.Split(*initialNodesStr, ",")
		for i, node := range initialNodes {
			initialNodes[i] = strings.TrimSpace(node)
		}
	}

	config := GatewayConfig{
		Port:            *port,
		DiscoveryAddr:   *discoveryAddr,
		RefreshInterval: *refreshInterval,
		InitialNodes:    initialNodes,
		FixedHttpPort:   *fixedHttpPort,
		UseServiceNames: *useServiceNames,
	}

	gateway := NewAPIGateway(config)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down API Gateway...")
		gateway.Stop()
		os.Exit(0)
	}()

	log.Printf("Starting API Gateway with discovery server at %s", config.DiscoveryAddr)
	log.Printf("Using service names: %v", config.UseServiceNames)
	if len(config.InitialNodes) > 0 {
		log.Printf("Initial nodes: %v", config.InitialNodes)
	}

	if err := gateway.Start(); err != nil {
		log.Fatalf("Gateway failed: %v", err)
	}
}
