package main

import (
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part3/discovery"
	"github.com/ogzhanolguncu/distributed-counter/part3/node"
	"github.com/ogzhanolguncu/distributed-counter/part3/peer"
	"github.com/ogzhanolguncu/distributed-counter/part3/protocol"
)

const (
	numNodes            = 5 // Number of nodes
	basePort            = 9000
	discoveryServerPort = 8000
	syncInterval        = 1 * time.Second // Sync interval
	maxSyncPeers        = 3               // Max peers for sync
	heartbeatInterval   = 500 * time.Millisecond
	cleanupInterval     = 3 * time.Second
	maxConsecutiveFails = 3
	failureTimeout      = 10 * time.Second
	testDuration        = 20 * time.Second       // Test duration
	operationRate       = 500 * time.Millisecond // Operation rate
	cooldownTime        = 5 * time.Second        // Cooldown period
	clearScreen         = "\033[H\033[2J"        // ANSI clear screen
	refreshRate         = 300 * time.Millisecond // Slower refresh for stability

	// Terminal width settings for better layout stability
	termWidth       = 80 // Assumed terminal width
	counterColWidth = 10 // Width for counter values
	addrColWidth    = 22 // Width for address column
	idColWidth      = 4  // Width for ID column
)

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
	colorBold   = "\033[1m"
)

// Animation frames for node activity
var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// Type to hold history entries
type historyEntry struct {
	nodeValues []int64
	timestamp  time.Time
	peerCounts []int
}

// Network status to track discovery connections
type networkStatus struct {
	nodeAddrs       []string
	activeNodes     []bool
	peerConnections map[string][]string
	lastUpdate      time.Time
	mutex           sync.Mutex
}

func newNetworkStatus(nodeAddrs []string) *networkStatus {
	ns := &networkStatus{
		nodeAddrs:       nodeAddrs,
		activeNodes:     make([]bool, len(nodeAddrs)),
		peerConnections: make(map[string][]string),
		lastUpdate:      time.Now(),
	}

	for i := range nodeAddrs {
		ns.activeNodes[i] = true
		ns.peerConnections[nodeAddrs[i]] = []string{}
	}

	return ns
}

func (ns *networkStatus) updateNodeStatus(addr string, isActive bool) {
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	for i, nodeAddr := range ns.nodeAddrs {
		if nodeAddr == addr {
			ns.activeNodes[i] = isActive
			break
		}
	}
	ns.lastUpdate = time.Now()
}

func (ns *networkStatus) updatePeerConnections(addr string, peers []string) {
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	ns.peerConnections[addr] = peers
	ns.lastUpdate = time.Now()
}

func (ns *networkStatus) getPeerCount(addr string) int {
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	return len(ns.peerConnections[addr])
}

func (ns *networkStatus) getStatus() ([]string, []bool, map[string][]string, time.Time) {
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	// Make copies to avoid race conditions
	nodeAddrs := make([]string, len(ns.nodeAddrs))
	copy(nodeAddrs, ns.nodeAddrs)

	activeNodes := make([]bool, len(ns.activeNodes))
	copy(activeNodes, ns.activeNodes)

	peerConnections := make(map[string][]string)
	for k, v := range ns.peerConnections {
		peerList := make([]string, len(v))
		copy(peerList, v)
		peerConnections[k] = peerList
	}

	return nodeAddrs, activeNodes, peerConnections, ns.lastUpdate
}

func main() {
	// Clear screen and print header
	fmt.Print(clearScreen)
	fmt.Println(colorBold + "=== DISTRIBUTED COUNTER WITH SERVICE DISCOVERY ===" + colorReset)

	// Display loading animation while setting up
	displayLoadingAnimation("Starting discovery server...", 1*time.Second)

	// Initialize metrics
	var (
		expectedValue    int64
		increments       int64
		decrements       int64
		nodeLastActivity = make([]time.Time, numNodes) // Track when each node was last active
		metricsLock      sync.Mutex
	)

	// Initialize last activity times
	for i := range nodeLastActivity {
		nodeLastActivity[i] = time.Now()
	}

	// Start discovery server
	discoveryServerAddr := fmt.Sprintf("127.0.0.1:%d", discoveryServerPort)
	discoveryServer := discovery.NewDiscoveryServer(discoveryServerAddr, cleanupInterval)

	// Create a separate error channel for server errors
	serverErrCh := make(chan error, 1)
	fmt.Printf("Starting discovery server at %s\n", discoveryServerAddr)

	go func() {
		if err := discoveryServer.Start(); err != nil {
			// Only log fatal if it's not a server closed error
			if !strings.Contains(err.Error(), "Server closed") &&
				!strings.Contains(err.Error(), "closed network connection") {
				serverErrCh <- err
			}
		}
	}()

	// Give the server time to start
	time.Sleep(1 * time.Second)

	// Check if there was an immediate error
	select {
	case err := <-serverErrCh:
		log.Fatalf("Discovery server failed to start: %v", err)
	default:
		// No error, continue
	}

	displayLoadingAnimation("Creating and registering nodes...", 1*time.Second)

	// Create nodes and discovery clients
	nodes := make([]*node.Node, numNodes)
	discoveryClients := make([]*discovery.DiscoveryClient, numNodes)
	nodeAddrs := make([]string, numNodes)

	for i := range numNodes {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		nodeAddrs[i] = addr

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
			LogLevel:            slog.LevelError, // Reduce log noise
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
	}

	// Initialize network status tracker
	netStatus := newNetworkStatus(nodeAddrs)

	// Give discovery clients time to register and connect
	displayLoadingAnimation("Waiting for peer discovery...", 2*time.Second)

	// Update initial peer connections
	for i, n := range nodes {
		peers := n.GetPeerManager().GetPeers()
		netStatus.updatePeerConnections(nodeAddrs[i], peers)
	}

	// Track history for visualization
	history := make([]historyEntry, 0, 50) // Pre-allocate some capacity
	historyMutex := sync.Mutex{}

	// Start concurrent operations
	stopChan := make(chan struct{})
	visualStopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Start the visualization goroutine with synchronization
	wg.Add(1)
	go func() {
		defer wg.Done()
		visualizationTicker := time.NewTicker(refreshRate)
		defer visualizationTicker.Stop()

		testStart := time.Now()
		testEnd := testStart.Add(testDuration)
		frameIndex := 0

		// Use a separate mutex for display updates to prevent flicker
		displayMutex := &sync.Mutex{}

		for {
			select {
			case <-visualizationTicker.C:
				// Lock to prevent other goroutines from updating the display
				displayMutex.Lock()

				metricsLock.Lock()
				incs := increments
				decs := decrements
				expected := expectedValue
				activity := make([]time.Time, len(nodeLastActivity))
				copy(activity, nodeLastActivity)
				metricsLock.Unlock()

				// Get node values and update peer connections
				nodeValues := make([]int64, numNodes)
				peerCounts := make([]int, numNodes)

				for i, n := range nodes {
					// Skip any nil nodes (in case some were shut down)
					if n == nil {
						continue
					}
					nodeValues[i] = n.GetCounter()
					peers := n.GetPeerManager().GetPeers()
					netStatus.updatePeerConnections(nodeAddrs[i], peers)
					peerCounts[i] = len(peers)
				}

				// Add to history (with mutex protection)
				historyMutex.Lock()
				history = append(history, historyEntry{
					nodeValues: append([]int64{}, nodeValues...),
					timestamp:  time.Now(),
					peerCounts: append([]int{}, peerCounts...),
				})
				// Trim history if too long
				if len(history) > 50 {
					history = history[len(history)-50:]
				}
				historyCopy := make([]historyEntry, len(history))
				copy(historyCopy, history)
				historyMutex.Unlock()

				// Calculate time progress
				now := time.Now()
				progress := 0.0
				if now.Before(testEnd) {
					progress = float64(now.Sub(testStart)) / float64(testDuration)
				} else {
					progress = 1.0
				}

				// Update animation frame
				frameIndex = (frameIndex + 1) % len(spinnerFrames)

				// Clear screen once before drawing (reduces flicker)
				fmt.Print(clearScreen)

				// Get the latest network status
				_, activeNodes, peerConnections, _ := netStatus.getStatus()

				// Display visualization (all at once)
				displayVisualization(
					nodeValues,
					incs,
					decs,
					expected,
					progress,
					activity,
					now,
					frameIndex,
					peerCounts,
					activeNodes,
					peerConnections,
					nodeAddrs,
				)

				// Done updating display
				displayMutex.Unlock()

			case <-visualStopChan:
				return
			}
		}
	}()

	// Start concurrent increment/decrement operations
	for i := range numNodes {
		wg.Add(1)
		go func(nodeIndex int) {
			defer wg.Done()

			ticker := time.NewTicker(operationRate + time.Duration(rand.Intn(200))*time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					// Only perform operations if test is still running
					if time.Now().Before(time.Now().Add(testDuration)) {
						// Randomly increment or decrement with 80/20 split
						if rand.Intn(10) < 8 {
							nodes[nodeIndex].Increment()
							metricsLock.Lock()
							expectedValue++
							increments++
							nodeLastActivity[nodeIndex] = time.Now()
							metricsLock.Unlock()
						} else {
							nodes[nodeIndex].Decrement()
							metricsLock.Lock()
							expectedValue--
							decrements++
							nodeLastActivity[nodeIndex] = time.Now()
							metricsLock.Unlock()
						}
					}
				case <-stopChan:
					return
				}
			}
		}(i)
	}

	// Start network status monitor (updates peer info)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				for i, n := range nodes {
					peers := n.GetPeerManager().GetPeers()
					netStatus.updatePeerConnections(nodeAddrs[i], peers)
					netStatus.updateNodeStatus(nodeAddrs[i], true) // Mark as active
				}
			case <-stopChan:
				return
			}
		}
	}()

	// Set up graceful shutdown on ctrl+c
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Use a select to either wait for test duration or receive interrupt
	select {
	case <-time.After(testDuration):
		// Test completed normally
	case sig := <-sigCh:
		fmt.Printf("\nReceived signal %v, initiating early shutdown...\n", sig)
	}

	// Stop operations
	close(stopChan)

	// Allow time for final synchronization
	fmt.Print(clearScreen)
	fmt.Println(colorBold + "Test complete. Waiting for final synchronization..." + colorReset)
	time.Sleep(1 * time.Second)

	// Show enhanced animated cooldown
	displayEnhancedCooldown(nodes, nodeAddrs, cooldownTime)

	// Stop visualization and wait for goroutines
	close(visualStopChan)

	// Add a small timeout for better visualization shutdown
	shutdownTimeout := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownTimeout)
	}()

	select {
	case <-shutdownTimeout:
		// All goroutines completed normally
	case <-time.After(2 * time.Second):
		fmt.Println("Forcing shutdown after timeout")
	}

	// Final results
	fmt.Print(clearScreen)
	fmt.Println("\n" + colorBold + "=== FINAL RESULTS ===" + colorReset)

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

	for i, n := range nodes {
		value := n.GetCounter()
		peers := n.GetPeerManager().GetPeers()
		fmt.Printf("Node %d (%s): %d (Peers: %d)\n", i, nodeAddrs[i], value, len(peers))

		if value != firstValue {
			allSame = false
		}
	}

	// Print convergence status
	if allSame {
		fmt.Printf("\n"+colorGreen+"SUCCESS: All nodes converged to %d\n"+colorReset, firstValue)
		if firstValue == finalExpected {
			fmt.Println(colorGreen + "PERFECT: Value matches expected count!" + colorReset)
		} else {
			fmt.Printf(colorYellow+"PARTIAL: Nodes converged but to unexpected value (expected %d, got %d)\n"+colorReset,
				finalExpected, firstValue)
		}
	} else {
		fmt.Println("\n" + colorRed + "FAILURE: Nodes did not converge to the same value" + colorReset)
	}

	gracefulShutdown(nodes, discoveryClients, discoveryServer)
}

// Visualization for current node states
func displayVisualization(
	nodeValues []int64,
	increments, decrements, expected int64,
	progress float64,
	nodeActivity []time.Time,
	now time.Time,
	frameIndex int,
	peerCounts []int,
	activeNodes []bool,
	peerConnections map[string][]string,
	nodeAddrs []string,
) {
	// Use a fixed-width string builder to reduce flicker and improve alignment
	var sb strings.Builder

	// Header with fixed width
	sb.WriteString(colorBold + "=== DISTRIBUTED COUNTER WITH SERVICE DISCOVERY ===\n" + colorReset)
	sb.WriteString(fmt.Sprintf("Progress: %6.1f%%  %s\n", progress*100, progressBar(progress, 40)))
	sb.WriteString(fmt.Sprintf("Operations: %5d increments, %5d decrements (Expected: %5d)\n\n",
		increments, decrements, expected))

	// Fixed width network topology section
	sb.WriteString(colorBold + "NETWORK TOPOLOGY:\n" + colorReset)
	sb.WriteString(fmt.Sprintf("Discovery Server (%s127.0.0.1:%-5d%s)\n\n",
		colorCyan, discoveryServerPort, colorReset))

	// Fixed width node status table with consistent alignment
	sb.WriteString(colorBold + "NODES STATUS:\n" + colorReset)

	// Table header with fixed width columns
	headers := fmt.Sprintf("%-4s | %-22s | %-10s | %-10s | %-10s | %-10s\n",
		"ID", "Address", "Counter", "Activity", "Peers", "Status")
	sb.WriteString(headers)
	sb.WriteString(strings.Repeat("-", 80) + "\n")

	// Print each node with fixed width values
	for i, value := range nodeValues {
		// Check if we have activity data for this node
		var activityStr string
		if i < len(nodeActivity) {
			timeSinceActivity := now.Sub(nodeActivity[i])
			isActive := timeSinceActivity < 500*time.Millisecond

			if isActive {
				activityStr = colorCyan + spinnerFrames[frameIndex] + colorReset
			} else {
				activityStr = " "
			}
		} else {
			activityStr = " "
		}

		// Value color
		valueColor := colorWhite
		if value > 0 {
			valueColor = colorGreen
		} else if value < 0 {
			valueColor = colorRed
		}

		// Status
		var statusStr string
		if i < len(activeNodes) {
			if activeNodes[i] {
				statusStr = colorGreen + "Online" + colorReset
			} else {
				statusStr = colorRed + "Offline" + colorReset
			}
		} else {
			statusStr = colorYellow + "Unknown" + colorReset
		}

		// Peer count with fixed width
		peerCountStr := "0"
		if i < len(peerCounts) {
			peerCountStr = fmt.Sprintf("%d", peerCounts[i])
		}

		// Format the node line with fixed widths and padding
		nodeLine := fmt.Sprintf("%4d | %-22s | %s%9d%s | %-10s | %-10s | %-10s\n",
			i,
			nodeAddrs[i],
			valueColor, value, colorReset,
			activityStr,
			peerCountStr,
			statusStr)

		sb.WriteString(nodeLine)
	}

	sb.WriteString("\n")

	// Convergence status with fixed width
	sb.WriteString(colorBold + "CONVERGENCE STATUS:\n" + colorReset)

	// Check for convergence
	allSame := true
	firstValue := nodeValues[0]
	for _, v := range nodeValues {
		if v != firstValue {
			allSame = false
			break
		}
	}

	if allSame {
		if firstValue == expected {
			sb.WriteString(colorGreen + "All nodes converged to expected value: " +
				fmt.Sprintf("%+d", expected) + colorReset + "\n")
		} else {
			sb.WriteString(fmt.Sprintf(colorYellow+"All nodes converged to %+d (expected: %+d)"+colorReset+"\n",
				firstValue, expected))
		}
	} else {
		sb.WriteString(colorRed + "Nodes have not yet converged" + colorReset + "\n")

		// Calculate value spread
		min, max := nodeValues[0], nodeValues[0]
		for _, v := range nodeValues {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		sb.WriteString(fmt.Sprintf("Value spread: %+d to %+d (range: %d)\n", min, max, max-min))
	}

	// Print everything at once to reduce flickering
	fmt.Print(sb.String())
}

// Enhanced cooldown animation with peer information
func displayEnhancedCooldown(nodes []*node.Node, nodeAddrs []string, duration time.Duration) {
	startTime := time.Now()
	endTime := startTime.Add(duration)

	ticker := time.NewTicker(150 * time.Millisecond) // Slightly slower for smoother animation
	defer ticker.Stop()

	frameIndex := 0
	lastValues := make([]int64, len(nodes))
	converged := false

	// Use a mutex to prevent display flicker
	displayMutex := &sync.Mutex{}

	// Collect initial values
	for i, n := range nodes {
		if n != nil {
			lastValues[i] = n.GetCounter()
		}
	}

	for now := time.Now(); now.Before(endTime); now = time.Now() {
		select {
		case <-ticker.C:
			displayMutex.Lock()

			progress := float64(now.Sub(startTime)) / float64(duration) * 100

			// Get current node values
			nodeValues := make([]int64, len(nodes))
			peerCounts := make([]int, len(nodes))

			// Safely get values, handling potential nil nodes
			for i, n := range nodes {
				if n == nil {
					continue
				}
				nodeValues[i] = n.GetCounter()
				pm := n.GetPeerManager()
				if pm != nil {
					peerCounts[i] = len(pm.GetPeers())
				}
			}

			// Check for changes
			changes := make([]bool, len(nodes))
			for i := range nodes {
				changes[i] = nodeValues[i] != lastValues[i]
				lastValues[i] = nodeValues[i]
			}

			// Check convergence
			allSame := true
			firstValue := nodeValues[0]
			for _, v := range nodeValues {
				if v != firstValue {
					allSame = false
					break
				}
			}

			// Only update converged status from false to true, not back
			if allSame && !converged {
				converged = true
			}

			// Build the entire output in a string builder
			var sb strings.Builder

			// Header with fixed width
			sb.WriteString(clearScreen)
			sb.WriteString(colorBold + "=== DISTRIBUTED COUNTER COOLDOWN ===\n" + colorReset)
			sb.WriteString(fmt.Sprintf("Synchronization progress: %6.1f%%  %s\n\n",
				progress, progressBar(progress/100, 40)))

			// Fixed-width table header
			sb.WriteString(colorBold + "NODE VALUES:\n" + colorReset)
			sb.WriteString(fmt.Sprintf("%-4s | %-22s | %-15s | %-10s | %-10s\n",
				"ID", "Address", "Counter", "Peers", "Status"))
			sb.WriteString(strings.Repeat("-", 75) + "\n")

			// Format each node line with consistent widths
			for i, value := range nodeValues {
				// Animation prefix
				var prefix string
				if changes[i] {
					prefix = colorCyan + spinnerFrames[frameIndex] + colorReset + " "
				} else {
					prefix = "  "
				}

				// Status indicator
				var statusIcon string
				if value == firstValue {
					statusIcon = colorGreen + "✓" + colorReset
				} else {
					statusIcon = colorYellow + "⟳" + colorReset
				}

				// Fixed-width node line
				nodeLine := fmt.Sprintf("%s%3d | %-22s | %s%12d%s | %10d | %10s\n",
					prefix,
					i,
					nodeAddrs[i],
					colorCyan, value, colorReset,
					peerCounts[i],
					statusIcon)

				sb.WriteString(nodeLine)
			}

			// Convergence status
			if converged {
				sb.WriteString("\n" + colorGreen + "All nodes have converged! Value: " +
					colorBold + fmt.Sprintf("%+d", firstValue) + colorReset + "\n")
			} else {
				sb.WriteString("\n" + colorYellow + "Nodes still syncing..." + colorReset + "\n")
			}

			// Print everything at once
			fmt.Print(sb.String())

			displayMutex.Unlock()
			now = time.Now()
		}
	}
}

// Function to display loading animation during various operations
func displayLoadingAnimation(message string, duration time.Duration) {
	doneChan := make(chan struct{})

	// Start animation in goroutine
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		frameIndex := 0
		for {
			select {
			case <-ticker.C:
				frame := spinnerFrames[frameIndex]
				frameIndex = (frameIndex + 1) % len(spinnerFrames)

				fmt.Print(clearScreen)
				fmt.Println(colorBold + "=== DISTRIBUTED COUNTER WITH SERVICE DISCOVERY ===" + colorReset)
				fmt.Printf("\n%s %s %s\n", colorCyan, frame, colorReset)
				fmt.Println(message)

			case <-doneChan:
				return
			}
		}
	}()

	// Wait for specified duration
	time.Sleep(duration)
	close(doneChan)
}

func progressBar(progress float64, width int) string {
	completed := int(progress * float64(width))
	if completed > width {
		completed = width
	}
	if completed < 0 {
		completed = 0
	}

	remaining := width - completed
	if remaining < 0 {
		remaining = 0
	}

	bar := colorGreen + strings.Repeat("█", completed) + colorReset + strings.Repeat("░", remaining)
	return bar
}

func gracefulShutdown(nodes []*node.Node, clients []*discovery.DiscoveryClient, server *discovery.DiscoveryServer) {
	fmt.Println("\nInitiating graceful shutdown sequence...")

	// First stop clients (which depend on nodes)
	fmt.Println("Stopping discovery clients...")
	for i, client := range clients {
		client.Stop()
		fmt.Printf("Discovery client %d stopped\n", i)
	}

	// Then close nodes
	fmt.Println("Closing node connections...")
	for i, n := range nodes {
		if err := n.Close(); err != nil {
			log.Printf("Error closing node %d: %v", i, err)
		} else {
			fmt.Printf("Node %d shut down\n", i)
		}
	}

	// Finally stop discovery server
	fmt.Println("Stopping discovery server...")

	// Use a separate goroutine to avoid blocking if server.Stop() hangs
	serverStopCh := make(chan error, 1)
	go func() {
		serverStopCh <- server.Stop()
	}()

	// Wait for the server to stop with a timeout
	select {
	case err := <-serverStopCh:
		if err != nil {
			if strings.Contains(err.Error(), "Server closed") ||
				strings.Contains(err.Error(), "closed network connection") {
				fmt.Println("Discovery server stopped successfully")
			} else {
				log.Printf("Error stopping discovery server: %v", err)
			}
		} else {
			fmt.Println("Discovery server stopped successfully")
		}
	case <-time.After(2 * time.Second):
		fmt.Println("Discovery server stop timed out, continuing shutdown")
	}

	fmt.Println(colorGreen + "All components shut down successfully" + colorReset)
}
