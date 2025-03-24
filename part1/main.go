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

	"github.com/ogzhanolguncu/distributed-counter/part1/node"
	"github.com/ogzhanolguncu/distributed-counter/part1/peer"
	"github.com/ogzhanolguncu/distributed-counter/part1/protocol"
)

const (
	numNodes      = 10 // Fewer nodes for simplicity
	basePort      = 9000
	testDuration  = 10 * time.Second       // Longer test for better visualization
	operationRate = 100 * time.Millisecond // Operation rate
	cooldownTime  = 3 * time.Second        // Cooldown period
	clearScreen   = "\033[H\033[2J"        // ANSI escape code to clear screen
	refreshRate   = 200 * time.Millisecond // Faster updates for smoother animation
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
}

func main() {
	// Initialize metrics
	var (
		expectedValue    int64
		increments       int64
		decrements       int64
		nodeLastActivity [numNodes]time.Time // Track when each node was last active
		metricsLock      sync.Mutex
	)

	// Initialize last activity times
	for i := range nodeLastActivity {
		nodeLastActivity[i] = time.Now()
	}

	// Clear screen and print header
	fmt.Print(clearScreen)
	fmt.Println(colorBold + "=== DISTRIBUTED COUNTER ===" + colorReset)

	// Create nodes
	nodes := make([]*node.Node, numNodes)
	for i := range numNodes {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		transport, err := protocol.NewTCPTransport(addr)
		if err != nil {
			log.Fatalf("Failed to create transport: %v", err)
		}

		n, err := node.NewNode(node.Config{
			Addr:         addr,
			SyncInterval: 500 * time.Millisecond,
			MaxSyncPeers: 5,
			LogLevel:     slog.LevelError,
		}, transport, peer.NewPeerManager())
		if err != nil {
			log.Fatalf("Failed to create node: %v", err)
		}
		nodes[i] = n
	}
	fmt.Printf("Created %d nodes\n", numNodes)

	// Connect all nodes (simple full mesh topology)
	for i, node := range nodes {
		pm := node.GetPeerManager()
		for j, peer := range nodes {
			if i != j {
				pm.AddPeer(peer.GetAddr())
			}
		}
	}
	fmt.Println("Connected all nodes in a full mesh")

	// Run concurrent operations
	stopChan := make(chan struct{})
	visualStopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Track history for visualization
	history := make([]historyEntry, 0, 50) // Pre-allocate some capacity
	historyMutex := sync.Mutex{}

	// Display loading animation while setting up
	displayLoadingAnimation("Setting up distributed counter network...", 1*time.Second)

	// Start the visualization goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		visualizationTicker := time.NewTicker(refreshRate)
		defer visualizationTicker.Stop()

		testStart := time.Now()
		testEnd := testStart.Add(testDuration)
		frameIndex := 0

		for {
			select {
			case <-visualizationTicker.C:
				metricsLock.Lock()
				incs := increments
				decs := decrements
				expected := expectedValue
				activity := nodeLastActivity // Copy the activity timestamps
				metricsLock.Unlock()

				// Get node values
				nodeValues := make([]int64, numNodes)
				for i, n := range nodes {
					nodeValues[i] = n.GetCounter()
				}

				// Add to history
				historyMutex.Lock()
				history = append(history, historyEntry{
					nodeValues: append([]int64{}, nodeValues...),
					timestamp:  time.Now(),
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

				// Display the visualization
				fmt.Print(clearScreen)
				displaySimpleVisualization(
					nodeValues,
					incs,
					decs,
					expected,
					progress,
					activity,
					now,
					frameIndex,
				)
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

			ticker := time.NewTicker(operationRate)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					// Randomly increment or decrement
					if rand.Intn(2) == 0 {
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
				case <-stopChan:
					return
				}
			}
		}(i)
	}

	// Run the test for specified duration
	time.Sleep(testDuration)

	// Stop operations
	close(stopChan)

	// Allow time for final synchronization
	fmt.Print(clearScreen)
	fmt.Println(colorBold + "Test complete. Waiting for final synchronization..." + colorReset)
	time.Sleep(500 * time.Millisecond) // Brief pause

	// Show enhanced animated cooldown
	displayEnhancedCooldown(nodes, cooldownTime)

	// Stop visualization
	close(visualStopChan)
	wg.Wait()

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

	for i, node := range nodes {
		value := node.GetCounter()
		fmt.Printf("Node %d: %d\n", i, value)

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

	gracefulShutdown(nodes)
}

// Enhanced cooldown animation with dynamic node status indicators
func displayEnhancedCooldown(nodes []*node.Node, duration time.Duration) {
	startTime := time.Now()
	endTime := startTime.Add(duration)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	frameIndex := 0
	lastValues := make([]int64, numNodes)
	converged := false

	for now := time.Now(); now.Before(endTime); now = time.Now() {
		select {
		case <-ticker.C:
			progress := float64(now.Sub(startTime)) / float64(duration) * 100

			// Get current node values
			nodeValues := make([]int64, numNodes)
			for i, n := range nodes {
				nodeValues[i] = n.GetCounter()
			}

			// Check for changes
			changes := make([]bool, numNodes)
			for i := range numNodes {
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

			// Display current status
			fmt.Print(clearScreen)
			fmt.Println(colorBold + "=== DISTRIBUTED COUNTER COOLDOWN ===" + colorReset)
			fmt.Printf("Synchronization progress: %.1f%%\n\n", progress)

			// Show animation frame only for nodes still changing
			frame := spinnerFrames[frameIndex]
			frameIndex = (frameIndex + 1) % len(spinnerFrames)

			fmt.Println("NODE VALUES:")
			for i, value := range nodeValues {
				var prefix string

				if changes[i] {
					prefix = colorCyan + frame + colorReset + " "
				} else {
					prefix = "  "
				}

				statusIcon := " "
				if value == firstValue {
					statusIcon = colorGreen + "✓" + colorReset
				} else {
					statusIcon = colorYellow + "⟳" + colorReset
				}

				fmt.Printf("%sNode %2d: %s%+4d%s %s\n",
					prefix, i,
					colorCyan, value, colorReset,
					statusIcon)
			}

			if converged {
				fmt.Println("\n" + colorGreen + "All nodes have converged! Value: " +
					colorBold + fmt.Sprintf("%+d", firstValue) + colorReset)
			} else {
				fmt.Println("\n" + colorYellow + "Nodes still syncing..." + colorReset)
			}

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
				fmt.Println(colorBold + "=== DISTRIBUTED COUNTER ===" + colorReset)
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

func displaySimpleVisualization(
	nodeValues []int64,
	increments, decrements, expected int64,
	progress float64,
	nodeActivity [numNodes]time.Time,
	now time.Time,
	frameIndex int,
) {
	// Title and progress
	fmt.Println(colorBold + "=== DISTRIBUTED COUNTER ===")
	fmt.Printf("Progress: %.1f%%\n", progress*100)
	fmt.Printf("Operations: %d increments, %d decrements (Expected: %d)\n\n",
		increments, decrements, expected)

	// Display node information in simple text format
	fmt.Println("NODES:")

	for i, value := range nodeValues {
		// Determine activity status
		timeSinceActivity := now.Sub(nodeActivity[i])
		isActive := timeSinceActivity < 500*time.Millisecond

		// Choose color based on value
		valueColor := colorWhite
		if value > 0 {
			valueColor = colorGreen
		} else if value < 0 {
			valueColor = colorRed
		}

		// Simulate increment/decrement counts (would be real counters in actual implementation)
		inc := int(value)
		if inc < 0 {
			inc = 0
		}
		dec := -int(value)
		if dec < 0 {
			dec = 0
		}

		// Loading indicator with more dynamic animation
		loading := "  "
		if isActive {
			loading = colorCyan + spinnerFrames[frameIndex] + " " + colorReset
		}

		// Display node info
		fmt.Printf("%s Node %d: %s%+d%s (Inc: %02d, Dec: %02d)\n",
			loading, i, valueColor, value, colorReset, inc, dec)
	}

	fmt.Println()

	// Display convergence status
	allSame := true
	firstValue := nodeValues[0]
	for _, v := range nodeValues {
		if v != firstValue {
			allSame = false
			break
		}
	}

	fmt.Println("CONVERGENCE STATUS:")
	if allSame {
		if firstValue == expected {
			fmt.Println(colorGreen + "All nodes converged to expected value: " +
				fmt.Sprintf("%+d", expected) + colorReset)
		} else {
			fmt.Printf(colorYellow+"All nodes converged to %+d (expected: %+d)"+colorReset+"\n",
				firstValue, expected)
		}
	} else {
		fmt.Println(colorRed + "Nodes have not yet converged" + colorReset)

		// Show min/max spread
		min, max := nodeValues[0], nodeValues[0]
		for _, v := range nodeValues {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		fmt.Printf("Value spread: %+d to %+d (range: %d)\n", min, max, max-min)
	}
}

func displaySimpleCooldown(nodes []*node.Node, progress float64) {
	fmt.Print(clearScreen)
	fmt.Println(colorBold + "=== DISTRIBUTED COUNTER COOLDOWN ===" + colorReset)
	fmt.Printf("Waiting for final synchronization: %.1f%%\n\n", progress)

	// Get node values
	nodeValues := make([]int64, numNodes)
	for i, n := range nodes {
		nodeValues[i] = n.GetCounter()
	}

	// Show node values as they converge
	allSame := true
	firstValue := nodeValues[0]

	fmt.Println("NODE VALUES:")
	for i, value := range nodeValues {
		status := " "
		if value == firstValue {
			status = colorGreen + "✓" + colorReset
		} else {
			status = colorYellow + "⟳" + colorReset
			allSame = false
		}

		fmt.Printf("Node %2d: %s%+4d%s %s\n",
			i, colorCyan, value, colorReset, status)
	}

	if allSame {
		fmt.Println("\n" + colorGreen + "All nodes have converged! Value: " +
			colorBold + fmt.Sprintf("%+d", firstValue) + colorReset)
	} else {
		fmt.Println("\n" + colorYellow + "Nodes still syncing..." + colorReset)
	}
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func gracefulShutdown(nodes []*node.Node) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived signal %v, initiating graceful shutdown...\n", sig)
	case <-time.After(5 * time.Second):
		fmt.Println("\nShutting down...")
	}

	fmt.Println("Closing all node connections...")
	for i, n := range nodes {
		if err := n.Close(); err != nil {
			log.Printf("Error closing node %d: %v", i, err)
		} else {
			fmt.Printf("Node %d shut down successfully\n", i)
		}
	}

	fmt.Println("All components shut down successfully")
}
