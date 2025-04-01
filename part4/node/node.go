package node

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/ogzhanolguncu/distributed-counter/part4/assertions"
	"github.com/ogzhanolguncu/distributed-counter/part4/crdt"
	"github.com/ogzhanolguncu/distributed-counter/part4/peer"
	"github.com/ogzhanolguncu/distributed-counter/part4/protocol"
	"github.com/ogzhanolguncu/distributed-counter/part4/visualizer"
	"github.com/ogzhanolguncu/distributed-counter/part4/wal"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
)

const (
	defaultChannelBuffer = 10_000
	defaultMaxWALSize    = 64 * 1024 * 1024 // 64MB
	walDirName           = "wal"
)

type Config struct {
	Addr                string
	SyncInterval        time.Duration
	FullSyncInterval    time.Duration
	MaxSyncPeers        int
	MaxConsecutiveFails int
	FailureTimeout      time.Duration
	DataDir             string        // Directory to store WAL files
	SnapshotInterval    time.Duration // How often to take a full snapshot of counter state
	LogLevel            slog.Level

	// WAL cleanup settings
	WALCleanupInterval time.Duration
	WALMaxSegments     int
	WALMinSegments     int
	WALMaxAge          time.Duration
}

type MessageInfo struct {
	message protocol.Message
	addr    string
}

type Node struct {
	config  Config
	counter *crdt.PNCounter
	logger  *slog.Logger

	peers *peer.PeerManager

	transport protocol.Transport
	ctx       context.Context
	cancel    context.CancelFunc

	incomingMsg  chan MessageInfo
	outgoingMsg  chan MessageInfo
	syncTick     <-chan time.Time
	fullSyncTick <-chan time.Time
	snapshotTick <-chan time.Time
	wal          *wal.WAL

	visualizer *visualizer.GossipVisualizer
}

func NewNode(config Config, transport protocol.Transport, peerManager *peer.PeerManager) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	assertions.Assert(config.SyncInterval > 0, "sync interval must be positive")
	assertions.Assert(config.MaxSyncPeers > 0, "max sync peers must be positive")
	assertions.Assert(config.Addr != "", "node address cannot be empty")
	assertions.Assert(config.MaxConsecutiveFails > 0, "max consecutive fails must be positive")
	assertions.Assert(config.FailureTimeout > 0, "failure timeout must be positive")
	assertions.AssertNotNil(transport, "transport cannot be nil")
	assertions.AssertNotNil(peerManager, "peer manager cannot be nil")

	// Set default values for fields not specified
	if config.FullSyncInterval == 0 {
		config.FullSyncInterval = config.SyncInterval * 10 // Default to 10x regular sync interval
	}

	if config.SnapshotInterval == 0 {
		config.SnapshotInterval = 5 * time.Minute // Default snapshot interval
	}

	if config.DataDir == "" {
		config.DataDir = "data" // Default data directory
	}

	// Set defaults for WAL cleanup config
	if config.WALCleanupInterval == 0 {
		config.WALCleanupInterval = 1 * time.Hour // Check for cleanup once per hour
	}
	if config.WALMaxSegments == 0 {
		config.WALMaxSegments = 50 // Keep at most 50 segments
	}
	if config.WALMinSegments == 0 {
		config.WALMinSegments = 5 // Always keep at least 5 segments
	}
	if config.WALMaxAge == 0 {
		config.WALMaxAge = 7 * 24 * time.Hour // Keep segments up to 7 days old
	}

	// Configure logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: config.LogLevel,
	})).With("[NODE]", config.Addr)

	// Create data directory if it doesn't exist
	walPath := filepath.Join(config.DataDir, walDirName, config.Addr)
	if err := os.MkdirAll(walPath, 0755); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize the WAL
	nodeWAL, err := wal.OpenWAL(walPath, config.Addr, true, defaultMaxWALSize)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	// Recover counter state from WAL or initialize new counter
	counter, err := wal.RecoverCounter(walPath, config.Addr)
	if err != nil {
		logger.Warn("failed to recover counter from WAL, initializing new counter",
			"error", err)
		counter = crdt.New(config.Addr)
	}

	node := &Node{
		config:    config,
		counter:   counter,
		peers:     peerManager,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
		transport: transport,
		wal:       nodeWAL,

		incomingMsg:  make(chan MessageInfo, defaultChannelBuffer),
		outgoingMsg:  make(chan MessageInfo, defaultChannelBuffer),
		syncTick:     time.NewTicker(config.SyncInterval).C,
		fullSyncTick: time.NewTicker(config.FullSyncInterval).C,
		snapshotTick: time.NewTicker(config.SnapshotInterval).C,
	}

	assertions.AssertNotNil(node.counter, "node counter must be initialized")
	assertions.AssertNotNil(node.ctx, "node context must be initialized")
	assertions.AssertNotNil(node.cancel, "node cancel function must be initialized")
	assertions.AssertNotNil(node.wal, "node WAL must be initialized")

	// Record initial metadata
	initialMetadata := map[string]interface{}{
		"node_id":      config.Addr,
		"startup_time": time.Now().UnixNano(),
		"version":      "1.0.0",
	}
	if err := nodeWAL.WriteMetadata(initialMetadata); err != nil {
		logger.Warn("failed to write initial metadata to WAL", "error", err)
	}

	// Create initial snapshot of counter state
	if err := nodeWAL.WriteCounterState(counter); err != nil {
		logger.Warn("failed to write initial counter state to WAL", "error", err)
	}

	// Start periodic cleanup of old WAL segments
	cleanupConfig := wal.CleanupConfig{
		MaxSegments: config.WALMaxSegments,
		MinSegments: config.WALMinSegments,
		MaxAge:      config.WALMaxAge,
	}
	nodeWAL.StartPeriodicCleanup(config.WALCleanupInterval, cleanupConfig)
	logger.Info("started WAL segment cleanup",
		"interval", config.WALCleanupInterval,
		"max_segments", config.WALMaxSegments,
		"min_segments", config.WALMinSegments,
		"max_age", config.WALMaxAge)

	if err := node.startTransport(); err != nil {
		cancel() // Clean up if we fail to start
		return nil, err
	}

	go node.eventLoop()
	go node.pruneStaleNodes()

	return node, nil
}

func (n *Node) pruneStaleNodes() {
	ticker := time.NewTicker(n.config.FailureTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.peers.PruneStalePeers()
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) startTransport() error {
	err := n.transport.Listen(func(addr string, data []byte) error {
		assertions.Assert(addr != "", "incoming addr cannot be empty")
		assertions.AssertNotNil(data, "incoming data cannot be nil or empty")

		msg, err := protocol.Decode(data)
		if err != nil {
			n.logger.Error("failed to decode incoming message",
				"node", n.config.Addr,
				"from", addr,
				"error", err)
			return fmt.Errorf("failed to read message: %w", err)
		}
		select {
		case n.incomingMsg <- MessageInfo{message: *msg, addr: addr}:
			// Message queued successfully
		default:
			n.logger.Warn("dropping incoming message, channel is full",
				"node", n.config.Addr,
				"from", addr,
				"message_type", msg.Type)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to start transport listener: %w", err)
	}

	return nil
}

func (n *Node) eventLoop() {
	for {
		select {
		case <-n.ctx.Done():
			n.logCounterState("node shutting down")
			return

		case msg := <-n.incomingMsg:
			n.handleIncMsg(msg)

		case msg := <-n.outgoingMsg:
			assertions.Assert(msg.addr != "", "outgoing addr cannot be empty")
			data, err := protocol.Encode(msg.message)
			if err != nil {
				n.logger.Error("failed to encode outgoing message",
					"node", n.config.Addr,
					"target", msg.addr,
					"message_type", msg.message.Type,
					"error", err)
				continue
			}

			if err := n.transport.Send(msg.addr, data); err != nil {
				n.logger.Error("failed to send message",
					"node", n.config.Addr,
					"target", msg.addr,
					"message_type", msg.message.Type,
					"error", err)

				if n.peers.MarkPeerFailed(msg.addr) {
					n.logger.Warn("peer is now inactive after consecutive failures",
						"peer", msg.addr,
						"max_fails", n.config.MaxConsecutiveFails)
				}
			} else {
				n.peers.MarkPeerActive(msg.addr)
			}

		case <-n.syncTick:
			n.initiateDigestSync()

		case <-n.fullSyncTick:
			n.performFullStateSync()

		case <-n.snapshotTick:
			// Periodically snapshot the full counter state to optimize recovery
			if err := n.wal.WriteCounterState(n.counter); err != nil {
				n.logger.Error("failed to write counter snapshot to WAL",
					"node", n.config.Addr,
					"error", err)
			} else {
				n.logger.Info("wrote counter snapshot to WAL",
					"node", n.config.Addr,
					"counter_value", n.counter.Value())
			}
		}
	}
}

func (n *Node) performFullStateSync() {
	peers := n.peers.GetPeers()
	if len(peers) == 0 {
		n.logger.Info("skipping full state sync - no peers available", "node", n.config.Addr)
		return
	}

	// Select a subset of random peers for full state sync
	numPeers := max(1, min(n.config.MaxSyncPeers/2, len(peers)))
	selectedPeers := make([]string, len(peers))
	copy(selectedPeers, peers)
	rand.Shuffle(len(selectedPeers), func(i, j int) {
		selectedPeers[i], selectedPeers[j] = selectedPeers[j], selectedPeers[i]
	})

	// Prepare full state message
	message := n.prepareCounterMessage(protocol.MessageTypePush)

	n.logCounterState("performing full state sync (anti-entropy)",
		"peers_count", numPeers,
		"selected_peers", selectedPeers[:numPeers])

	// Send to selected peers
	for _, peer := range selectedPeers[:numPeers] {
		n.outgoingMsg <- MessageInfo{
			message: message,
			addr:    peer,
		}
	}

	// After full sync, consider taking a snapshot of counter state
	if err := n.wal.WriteCounterState(n.counter); err != nil {
		n.logger.Error("failed to write counter snapshot after full sync",
			"node", n.config.Addr,
			"error", err)
	}
}

// Create a message with the current counter state
func (n *Node) prepareCounterMessage(msgType uint8) protocol.Message {
	// Get both increment and decrement counters
	increments, decrements := n.counter.Counters()
	return protocol.Message{
		Type:            msgType,
		NodeID:          n.config.Addr,
		IncrementValues: increments,
		DecrementValues: decrements,
	}
}

func (n *Node) prepareDigestMessage(msgType uint8, digestValue ...uint64) protocol.Message {
	// Base message with common fields
	msg := protocol.Message{
		Type:   msgType,
		NodeID: n.config.Addr,
	}

	// If it's a digest pull or digest ack, set the digest
	if msgType == protocol.MessageTypeDigestPull || msgType == protocol.MessageTypeDigestAck {
		// For digest pull, calculate our current digest
		if msgType == protocol.MessageTypeDigestPull || len(digestValue) == 0 {
			// Get both increment and decrement counters
			increments, decrements := n.counter.Counters()
			data, err := deterministicSerialize(increments, decrements)
			if err != nil {
				n.logger.Error("failed to create digest message",
					"node", n.config.Addr,
					"message_type", msgType,
					"error", err)
				return msg
			}

			msg.Digest = xxhash.Sum64(data)
		} else {
			// For digest ack, use the provided digest value
			msg.Digest = digestValue[0]
		}
	}

	return msg
}

func (n *Node) initiateDigestSync() {
	peers := n.peers.GetPeers()

	if len(peers) == 0 {
		n.logger.Info("skipping digest sync - no peers available", "node", n.config.Addr)
		return
	}

	numPeers := min(n.config.MaxSyncPeers, len(peers))
	assertions.Assert(numPeers > 0, "number of peers to sync with must be positive")

	selectedPeers := make([]string, len(peers))
	copy(selectedPeers, peers)
	rand.Shuffle(len(selectedPeers), func(i, j int) {
		selectedPeers[i], selectedPeers[j] = selectedPeers[j], selectedPeers[i]
	})

	ctx, cancel := context.WithTimeout(n.ctx, n.config.SyncInterval/2)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	message := n.prepareDigestMessage(protocol.MessageTypeDigestPull)

	n.logCounterState("initiating digest sync",
		"selected_peers", selectedPeers[:numPeers],
		"digest", message.Digest)

	for _, peer := range selectedPeers[:numPeers] {
		peerAddr := peer // Shadow the variable for goroutine
		g.Go(func() error {
			n.logger.Info("sending digest pull",
				"node", n.config.Addr,
				"target", peerAddr,
				"digest", message.Digest)

			select {
			case n.outgoingMsg <- MessageInfo{
				message: message,
				addr:    peerAddr,
			}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	if err := g.Wait(); err != nil {
		n.logger.Error("sync round failed",
			"node", n.config.Addr,
			"error", err)
	}
}

func (n *Node) handleIncMsg(inc MessageInfo) {
	assertions.Assert(
		inc.message.Type == protocol.MessageTypePush ||
			inc.message.Type == protocol.MessageTypeDigestAck ||
			inc.message.Type == protocol.MessageTypeDigestPull,
		"invalid message type")

	n.logger.Info("received message",
		"node", n.config.Addr,
		"from", inc.addr,
		"message_type", inc.message.Type,
		"remote_node_id", inc.message.NodeID)

	// This is required for nodes that joined after that missed initial discovery
	if !slices.Contains(n.peers.GetPeers(), inc.addr) {
		n.peers.AddPeer(inc.addr)
	} else {
		n.peers.MarkPeerActive(inc.addr)
	}

	switch inc.message.Type {
	case protocol.MessageTypeDigestPull:
		// Create our counters hash
		increments, decrements := n.counter.Counters()
		data, err := deterministicSerialize(increments, decrements)
		if err != nil {
			n.logger.Error("failed to serialize counters",
				"node", n.config.Addr,
				"counter_value", n.counter.Value(),
				"error", err)
			return
		}

		countersHash := xxhash.Sum64(data)

		if countersHash == inc.message.Digest {
			// Digests match - send ack with matching digest
			ackMsg := n.prepareDigestMessage(protocol.MessageTypeDigestAck, inc.message.Digest)

			n.logger.Info("sending digest acknowledgment (digests match)",
				"node", n.config.Addr,
				"target", inc.addr,
				"digest", countersHash,
				"counter_value", n.counter.Value())

			n.outgoingMsg <- MessageInfo{
				addr:    inc.addr,
				message: ackMsg,
			}
		} else {
			// Digests don't match - send full state
			responseMsg := n.prepareCounterMessage(protocol.MessageTypePush)

			n.logger.Info("sending full state (digests don't match)",
				"node", n.config.Addr,
				"target", inc.addr,
				"local_digest", countersHash,
				"remote_digest", inc.message.Digest,
				"counter_value", n.counter.Value())

			n.outgoingMsg <- MessageInfo{
				message: responseMsg,
				addr:    inc.addr,
			}
		}
	case protocol.MessageTypeDigestAck:
		n.logger.Info("received digest acknowledgment",
			"node", n.config.Addr,
			"from", inc.addr,
			"digest", inc.message.Digest,
			"counter_value", n.counter.Value())
		return

	case protocol.MessageTypePush:
		oldValue := n.counter.Value()

		// For push messages, create a counter and merge it
		tempCounter := crdt.New(inc.message.NodeID)
		tempCounter.MergeIncrements(inc.message.IncrementValues)
		tempCounter.MergeDecrements(inc.message.DecrementValues)

		// Merge with our local counter
		updated := n.counter.Merge(tempCounter)

		if updated {
			newValue := n.counter.Value()
			increments, decrements := n.counter.Counters()
			n.logger.Info("counter updated after merge",
				"node", n.config.Addr,
				"from", oldValue,
				"to", newValue,
				"increments", increments,
				"decrements", decrements,
				"from_node", inc.message.NodeID)

			// Write counter state to WAL after successful merge
			if err := n.wal.WriteCounterState(n.counter); err != nil {
				n.logger.Error("failed to write counter state to WAL after merge",
					"node", n.config.Addr,
					"error", err)
			}
		} else {
			n.logger.Info("received push message (no changes)",
				"node", n.config.Addr,
				"from", inc.addr,
				"counter_value", n.counter.Value())
		}
	}
}

func (n *Node) Increment() {
	assertions.AssertNotNil(n.counter, "node counter cannot be nil")

	oldValue := n.counter.Value()
	newValue := n.counter.Increment(n.config.Addr)

	increments, decrements := n.counter.Counters()

	n.logger.Info("incremented counter",
		"node", n.config.Addr,
		"from", oldValue,
		"to", newValue,
		"increments", increments,
		"decrements", decrements)

	// Log increment operation to WAL
	if err := n.wal.WriteCounterIncrement(n.config.Addr); err != nil {
		n.logger.Error("failed to write increment to WAL",
			"node", n.config.Addr,
			"error", err)
	}
}

func (n *Node) Decrement() {
	assertions.AssertNotNil(n.counter, "node counter cannot be nil")

	oldValue := n.counter.Value()
	newValue := n.counter.Decrement(n.config.Addr)

	increments, decrements := n.counter.Counters()

	n.logger.Info("decremented counter",
		"node", n.config.Addr,
		"from", oldValue,
		"to", newValue,
		"increments", increments,
		"decrements", decrements)

	// Log decrement operation to WAL
	if err := n.wal.WriteCounterDecrement(n.config.Addr); err != nil {
		n.logger.Error("failed to write decrement to WAL",
			"node", n.config.Addr,
			"error", err)
	}
}

func (n *Node) GetCounter() int64 {
	assertions.AssertNotNil(n.counter, "node counter cannot be nil")
	return n.counter.Value()
}

func (n *Node) GetLocalCounter() int64 {
	assertions.AssertNotNil(n.counter, "node counter cannot be nil")
	return n.counter.LocalValue(n.config.Addr)
}

func (n *Node) GetAddr() string {
	assertions.Assert(n.config.Addr != "", "node addr cannot be empty")
	return n.config.Addr
}

func (n *Node) Close() error {
	assertions.AssertNotNil(n.cancel, "cancel function cannot be nil")
	assertions.AssertNotNil(n.transport, "transport cannot be nil")
	assertions.AssertNotNil(n.wal, "WAL cannot be nil")

	// Take final snapshot of counter state
	if err := n.wal.WriteCounterState(n.counter); err != nil {
		n.logger.Error("failed to write final counter state to WAL",
			"node", n.config.Addr,
			"error", err)
	}

	// Do a final cleanup before closing
	if err := n.wal.CleanupSegments(wal.CleanupConfig{
		MaxSegments: n.config.WALMaxSegments,
		MinSegments: n.config.WALMinSegments,
		MaxAge:      n.config.WALMaxAge,
	}); err != nil {
		n.logger.Error("failed to cleanup WAL segments during shutdown",
			"node", n.config.Addr,
			"error", err)
	}

	// Close WAL
	if err := n.wal.Close(); err != nil {
		n.logger.Error("failed to close WAL",
			"node", n.config.Addr,
			"error", err)
	}

	n.cancel()
	return n.transport.Close()
}

func (n *Node) GetPeerManager() *peer.PeerManager {
	assertions.AssertNotNil(n.peers, "peer manager cannot be nil")
	return n.peers
}

func (n *Node) logCounterState(msg string, additionalFields ...any) {
	increments, decrements := n.counter.Counters()
	fields := []any{
		"node", n.config.Addr,
		"counter_value", n.counter.Value(),
		"increments", increments,
		"decrements", decrements,
	}

	// Append any additional context fields
	fields = append(fields, additionalFields...)

	n.logger.Info(msg, fields...)
}

type CounterEntry struct {
	NodeID string
	Value  uint64
}

func deterministicSerialize(increments, decrements crdt.PNMap) ([]byte, error) {
	// Combine all keys from both maps to get the complete set of node IDs
	allNodeIDs := make(map[string]struct{})
	for nodeID := range increments {
		allNodeIDs[nodeID] = struct{}{}
	}
	for nodeID := range decrements {
		allNodeIDs[nodeID] = struct{}{}
	}

	// Create a sorted slice of all node IDs
	sortedNodeIDs := make([]string, 0, len(allNodeIDs))
	for nodeID := range allNodeIDs {
		sortedNodeIDs = append(sortedNodeIDs, nodeID)
	}
	sort.Strings(sortedNodeIDs)

	orderedIncrements := make([]CounterEntry, len(sortedNodeIDs))
	orderedDecrements := make([]CounterEntry, len(sortedNodeIDs))

	for i, nodeID := range sortedNodeIDs {
		// Use the value if it exists, or 0 if it doesn't
		incValue := increments[nodeID] // Will be 0 if key doesn't exist
		decValue := decrements[nodeID] // Will be 0 if key doesn't exist

		orderedIncrements[i] = CounterEntry{NodeID: nodeID, Value: incValue}
		orderedDecrements[i] = CounterEntry{NodeID: nodeID, Value: decValue}
	}

	return msgpack.Marshal([]any{orderedIncrements, orderedDecrements})
}

func (n *Node) SetVisualizer(vis *visualizer.GossipVisualizer) {
	n.visualizer = vis

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if n.visualizer != nil {
					n.visualizer.RecordNodeState(
						n.GetAddr(),
						n.GetCounter(),
						n.peers.GetPeers(),
					)
				}
			case <-n.ctx.Done():
				return
			}
		}
	}()
}
