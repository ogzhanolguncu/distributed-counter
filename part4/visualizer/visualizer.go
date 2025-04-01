package visualizer

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// MessageRecord represents a single message in the gossip protocol
type MessageRecord struct {
	FromNode  string
	ToNode    string
	Type      string
	Timestamp time.Time
	Content   map[string]interface{}
}

// NodeState represents the state of a node at a point in time
type NodeState struct {
	NodeID    string
	Timestamp time.Time
	Counter   int64
	Peers     []string
}

// GossipVisualizer records and visualizes the gossip protocol activity
type GossipVisualizer struct {
	messages   []MessageRecord
	nodeStates map[string][]NodeState
	mu         sync.Mutex
	startTime  time.Time
	enabled    bool
	logFile    *os.File
	writer     *bufio.Writer
}

// NewVisualizer creates a new gossip visualizer
func NewVisualizer(logFilePath string) (*GossipVisualizer, error) {
	var file *os.File
	var writer *bufio.Writer
	var err error

	if logFilePath != "" {
		file, err = os.Create(logFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create log file: %w", err)
		}
		writer = bufio.NewWriter(file)
	}

	return &GossipVisualizer{
		messages:   make([]MessageRecord, 0),
		nodeStates: make(map[string][]NodeState),
		startTime:  time.Now(),
		enabled:    true,
		logFile:    file,
		writer:     writer,
	}, nil
}

// RecordMessage records a gossip message
func (g *GossipVisualizer) RecordMessage(fromNode, toNode, msgType string, content map[string]interface{}) {
	if !g.enabled {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	record := MessageRecord{
		FromNode:  fromNode,
		ToNode:    toNode,
		Type:      msgType,
		Timestamp: time.Now(),
		Content:   content,
	}

	g.messages = append(g.messages, record)

	if g.writer != nil {
		msg := fmt.Sprintf("[MSG] %s | %s -> %s | Type: %s | Content: %v\n",
			record.Timestamp.Format(time.RFC3339Nano),
			record.FromNode,
			record.ToNode,
			record.Type,
			record.Content)
		g.writer.WriteString(msg)
		g.writer.Flush()
	}
}

// RecordNodeState records the state of a node
func (g *GossipVisualizer) RecordNodeState(nodeID string, counter int64, peers []string) {
	if !g.enabled {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	state := NodeState{
		NodeID:    nodeID,
		Timestamp: time.Now(),
		Counter:   counter,
		Peers:     peers,
	}

	if _, exists := g.nodeStates[nodeID]; !exists {
		g.nodeStates[nodeID] = make([]NodeState, 0)
	}

	g.nodeStates[nodeID] = append(g.nodeStates[nodeID], state)

	if g.writer != nil {
		msg := fmt.Sprintf("[STATE] %s | Node: %s | Counter: %d | Peers: %v\n",
			state.Timestamp.Format(time.RFC3339Nano),
			state.NodeID,
			state.Counter,
			state.Peers)
		g.writer.WriteString(msg)
		g.writer.Flush()
	}
}

// GetMessageCount returns the number of messages by type
func (g *GossipVisualizer) GetMessageCount() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()

	counts := make(map[string]int)
	for _, msg := range g.messages {
		counts[msg.Type]++
	}
	return counts
}

// GetNodeActivity returns the message activity per node
func (g *GossipVisualizer) GetNodeActivity() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()

	activity := make(map[string]int)
	for _, msg := range g.messages {
		activity[msg.FromNode]++
		activity[msg.ToNode]++
	}
	return activity
}

// GenerateTimelineReport creates a text-based timeline of events
func (g *GossipVisualizer) GenerateTimelineReport() string {
	g.mu.Lock()
	defer g.mu.Unlock()

	var builder strings.Builder
	builder.WriteString("=== GOSSIP PROTOCOL TIMELINE ===\n\n")

	// Combine messages and state changes into a single timeline
	type timelineEvent struct {
		time    time.Time
		isMsg   bool
		message *MessageRecord
		state   *NodeState
		nodeID  string
	}

	var events []timelineEvent

	// Add messages to timeline
	for i := range g.messages {
		msg := &g.messages[i]
		events = append(events, timelineEvent{
			time:    msg.Timestamp,
			isMsg:   true,
			message: msg,
		})
	}

	// Add state changes to timeline
	for nodeID, states := range g.nodeStates {
		for i := range states {
			state := &states[i]
			events = append(events, timelineEvent{
				time:   state.Timestamp,
				isMsg:  false,
				state:  state,
				nodeID: nodeID,
			})
		}
	}

	// Sort events by timestamp
	sort.Slice(events, func(i, j int) bool {
		return events[i].time.Before(events[j].time)
	})

	// Generate the timeline
	for _, event := range events {
		timeOffset := event.time.Sub(g.startTime).Milliseconds()

		if event.isMsg {
			msg := event.message
			builder.WriteString(fmt.Sprintf("%6dms | MSG | %s -> %s | %s\n",
				timeOffset, msg.FromNode, msg.ToNode, msg.Type))
		} else {
			state := event.state
			builder.WriteString(fmt.Sprintf("%6dms | STATE | %s | Counter: %d | Peers: %v\n",
				timeOffset, state.NodeID, state.Counter, len(state.Peers)))
		}
	}

	return builder.String()
}

// Stop stops the visualizer and closes the log file
func (g *GossipVisualizer) Stop() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.enabled = false
	if g.writer != nil {
		g.writer.Flush()
	}
	if g.logFile != nil {
		return g.logFile.Close()
	}
	return nil
}
