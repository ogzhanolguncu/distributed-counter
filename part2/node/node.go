package node

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/ogzhanolguncu/distributed-counter/part2/assertions"
	"github.com/ogzhanolguncu/distributed-counter/part2/crdt"
	"github.com/ogzhanolguncu/distributed-counter/part2/peer"
	"github.com/ogzhanolguncu/distributed-counter/part2/protocol"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
)

const defaultChannelBuffer = 10_000

type Config struct {
	Addr             string
	SyncInterval     time.Duration
	FullSyncInterval time.Duration
	MaxSyncPeers     int
	LogLevel         slog.Level
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
}

func NewNode(config Config, transport protocol.Transport, peerManager *peer.PeerManager, logger *slog.Logger) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	assertions.Assert(config.SyncInterval > 0, "sync interval must be positive")
	assertions.Assert(config.MaxSyncPeers > 0, "max sync peers must be positive")
	assertions.Assert(config.Addr != "", "node address cannot be empty")
	assertions.AssertNotNil(transport, "transport cannot be nil")
	assertions.AssertNotNil(peerManager, "peer manager cannot be nil")

	if config.FullSyncInterval == 0 {
		config.FullSyncInterval = config.SyncInterval * 10 // Default to 10x regular sync interval
	}

	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}

	nodeLogger := logger.With("component", "NODE", "addr", config.Addr)

	node := &Node{
		config:    config,
		counter:   crdt.New(config.Addr),
		peers:     peerManager,
		logger:    nodeLogger,
		ctx:       ctx,
		cancel:    cancel,
		transport: transport,

		incomingMsg:  make(chan MessageInfo, defaultChannelBuffer),
		outgoingMsg:  make(chan MessageInfo, defaultChannelBuffer),
		syncTick:     time.NewTicker(config.SyncInterval).C,
		fullSyncTick: time.NewTicker(config.FullSyncInterval).C, // Initialize full sync ticker
	}

	assertions.AssertNotNil(node.counter, "node counter must be initialized")
	assertions.AssertNotNil(node.ctx, "node context must be initialized")
	assertions.AssertNotNil(node.cancel, "node cancel function must be initialized")

	if err := node.startTransport(); err != nil {
		cancel() // Clean up if we fail to start
		return nil, err
	}

	go node.eventLoop()
	return node, nil
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
			}

		case <-n.syncTick:
			n.initiateDigestSync()
		case <-n.fullSyncTick:
			n.performFullStateSync()
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
	n.counter.Increment(n.config.Addr)
	newValue := n.counter.Value()

	increments, decrements := n.counter.Counters()
	n.logger.Info("incremented counter",
		"node", n.config.Addr,
		"from", oldValue,
		"to", newValue,
		"increments", increments,
		"decrements", decrements,
	)
}

func (n *Node) Decrement() {
	assertions.AssertNotNil(n.counter, "node counter cannot be nil")

	oldValue := n.counter.Value()
	n.counter.Decrement(n.config.Addr)
	newValue := n.counter.Value()

	increments, decrements := n.counter.Counters()

	n.logger.Info("decremented counter",
		"node", n.config.Addr,
		"from", oldValue,
		"to", newValue,
		"increments", increments,
		"decrements", decrements)
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

	n.cancel()
	return n.transport.Close()
}

func (n *Node) GetPeerManager() *peer.PeerManager {
	assertions.AssertNotNil(n.peers, "peer manager cannot be nil")
	return n.peers
}

// Helper method to standardize logging of counter state
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
