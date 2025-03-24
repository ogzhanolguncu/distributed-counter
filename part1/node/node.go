package node

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/ogzhanolguncu/distributed-counter/part1/assertions"
	"github.com/ogzhanolguncu/distributed-counter/part1/crdt"
	"github.com/ogzhanolguncu/distributed-counter/part1/peer"
	"github.com/ogzhanolguncu/distributed-counter/part1/protocol"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
)

const defaultChannelBuffer = 10_000

type Config struct {
	Addr         string
	SyncInterval time.Duration
	MaxSyncPeers int
	LogLevel     slog.Level
}

type State struct {
	counter atomic.Uint64
	version atomic.Uint32
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

	incomingMsg chan MessageInfo
	outgoingMsg chan MessageInfo
	syncTick    <-chan time.Time
}

func NewNode(config Config, transport protocol.Transport, peerManager *peer.PeerManager) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	assertions.Assert(config.SyncInterval > 0, "sync interval must be positive")
	assertions.Assert(config.MaxSyncPeers > 0, "max sync peers must be positive")
	assertions.Assert(config.Addr != "", "node address cannot be empty")
	assertions.AssertNotNil(transport, "transport cannot be nil")
	assertions.AssertNotNil(peerManager, "peer manager cannot be nil")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: config.LogLevel,
	})).With("[NODE]", config.Addr)

	node := &Node{
		config:    config,
		counter:   crdt.New(config.Addr),
		peers:     peerManager,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
		transport: transport,

		incomingMsg: make(chan MessageInfo, defaultChannelBuffer),
		outgoingMsg: make(chan MessageInfo, defaultChannelBuffer),
		syncTick:    time.NewTicker(config.SyncInterval).C,
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
			return fmt.Errorf("failed to read message: %w", err)
		}
		select {
		case n.incomingMsg <- MessageInfo{message: *msg, addr: addr}:
		default:
			n.logger.Warn("dropping message, channel is full", "from", addr)
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
			n.logger.Info("shutting down", "counter", n.counter.Value())
			return

		case msg := <-n.incomingMsg:
			n.handleIncMsg(msg)

		case msg := <-n.outgoingMsg:
			assertions.Assert(msg.addr != "", "outgoing addr cannot be empty")
			data, err := protocol.Encode(msg.message)
			if err != nil {
				n.logger.Error("failed to encode message",
					"to", msg.addr,
					"error", err)
				continue
			}

			if err := n.transport.Send(msg.addr, data); err != nil {
				n.logger.Error("failed to send message",
					"to", msg.addr,
					"error", err)
			}

		case <-n.syncTick:
			n.pullState()
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

	// For MessageTypeDigestPull, calculate our current digest
	if msgType == protocol.MessageTypeDigestPull {
		// Get both increment and decrement counters
		increments, decrements := n.counter.Counters()
		counters := []any{increments, decrements}
		data, err := msgpack.Marshal(counters)
		if err != nil {
			n.logger.Error("failed to create digest message", "error", err)
			return msg
		}

		msg.Digest = xxhash.Sum64(data)
	}

	// For MessageTypeDigestAck, use the digest value that was passed in
	if msgType == protocol.MessageTypeDigestAck && len(digestValue) > 0 {
		msg.Digest = digestValue[0]
	}

	return msg
}

func (n *Node) pullState() {
	assertions.AssertNotNil(n.peers, "peer manager cannot be nil")

	peers := n.peers.GetPeers()

	if len(peers) == 0 {
		n.logger.Info("no peers available for sync")
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

	for _, peer := range selectedPeers[:numPeers] {
		peerAddr := peer // Shadow the variable for goroutine
		g.Go(func() error {
			// Get both increment and decrement counters for logging
			increments, decrements := n.counter.Counters()

			n.logger.Info("pulling state",
				"from", peerAddr,
				"type", message.Type,
				"counter", n.counter.Value(),
				"increments", fmt.Sprintf("%v", increments),
				"decrements", fmt.Sprintf("%v", decrements))

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
		n.logger.Error("sync round failed", "error", err)
	}
}

func (n *Node) handleIncMsg(inc MessageInfo) {
	assertions.Assert(
		inc.message.Type == protocol.MessageTypePull ||
			inc.message.Type == protocol.MessageTypePush ||
			inc.message.Type == protocol.MessageTypeDigestAck ||
			inc.message.Type == protocol.MessageTypeDigestPull,
		"invalid message type")

	n.logger.Info("received message",
		"from", inc.addr,
		"type", inc.message.Type,
		"nodeID", inc.message.NodeID,
		"increments", fmt.Sprintf("%v", inc.message.IncrementValues),
		"decrements", fmt.Sprintf("%v", inc.message.DecrementValues))

	if inc.message.Type == protocol.MessageTypeDigestPull {
		// Create our counters hash
		increments, decrements := n.counter.Counters()
		counters := []any{increments, decrements}
		data, err := msgpack.Marshal(counters)
		if err != nil {
			n.logger.Error("failed to marshal counters", "error", err)
			return
		}

		countersHash := xxhash.Sum64(data)

		if countersHash == inc.message.Digest {
			// Digests match - send ack with matching digest
			ackMsg := n.prepareDigestMessage(protocol.MessageTypeDigestAck, inc.message.Digest)

			n.logger.Info("digests match, sending ack",
				"to", inc.addr,
				"digest", countersHash)

			n.outgoingMsg <- MessageInfo{
				addr:    inc.addr,
				message: ackMsg,
			}
		} else {
			// Digests don't match - send full state
			responseMsg := n.prepareCounterMessage(protocol.MessageTypePush)

			n.logger.Info("digests don't match, sending full state",
				"to", inc.addr,
				"local_digest", countersHash,
				"remote_digest", inc.message.Digest)

			n.outgoingMsg <- MessageInfo{
				message: responseMsg,
				addr:    inc.addr,
			}
		}
	}

	// Create a PNCounter from received values
	tempCounter := crdt.New(inc.message.NodeID)
	tempCounter.MergeIncrements(inc.message.IncrementValues)
	tempCounter.MergeDecrements(inc.message.DecrementValues)

	// Merge with our local counter
	updated := n.counter.Merge(tempCounter)

	if updated {
		increments, decrements := n.counter.Counters()

		n.logger.Info("counter updated after merge",
			"total", n.counter.Value(),
			"increments", fmt.Sprintf("%v", increments),
			"decrements", fmt.Sprintf("%v", decrements))
	}

	// If it's a pull request, always respond with our current state
	if inc.message.Type == protocol.MessageTypePull {
		responseMsg := n.prepareCounterMessage(protocol.MessageTypePush)

		// Get both increment and decrement counters for logging
		increments, decrements := n.counter.Counters()

		n.logger.Info("responding to pull",
			"to", inc.addr,
			"type", responseMsg.Type,
			"counter", n.counter.Value(),
			"increments", fmt.Sprintf("%v", increments),
			"decrements", fmt.Sprintf("%v", decrements))

		n.outgoingMsg <- MessageInfo{
			message: responseMsg,
			addr:    inc.addr,
		}
	}
}

func (n *Node) Increment() {
	assertions.AssertNotNil(n.counter, "node counter cannot be nil")

	oldValue := n.counter.Value()
	newValue := n.counter.Increment(n.config.Addr)

	increments, decrements := n.counter.Counters()

	n.logger.Info("incremented counter",
		"from", oldValue,
		"to", newValue,
		"increments", fmt.Sprintf("%v", increments),
		"decrements", fmt.Sprintf("%v", decrements))
}

func (n *Node) Decrement() {
	assertions.AssertNotNil(n.counter, "node counter cannot be nil")

	oldValue := n.counter.Value()
	newValue := n.counter.Decrement(n.config.Addr)

	increments, decrements := n.counter.Counters()

	n.logger.Info("decremented counter",
		"from", oldValue,
		"to", newValue,
		"increments", fmt.Sprintf("%v", increments),
		"decrements", fmt.Sprintf("%v", decrements))
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
