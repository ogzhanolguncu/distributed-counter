package node

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part0/assertions"
	"github.com/ogzhanolguncu/distributed-counter/part0/crdt"
	"github.com/ogzhanolguncu/distributed-counter/part0/protocol"
	"golang.org/x/sync/errgroup"
)

const defaultChannelBuffer = 10000

type Config struct {
	Addr         string
	SyncInterval time.Duration
	MaxSyncPeers int
}

type MessageInfo struct {
	message protocol.Message
	addr    string
}

type Node struct {
	config  Config
	counter *crdt.PNCounter // Using PNCounter CRDT instead of GCounter

	peers   []string
	peersMu sync.RWMutex

	transport protocol.Transport
	ctx       context.Context
	cancel    context.CancelFunc

	incomingMsg chan MessageInfo
	outgoingMsg chan MessageInfo
	syncTick    <-chan time.Time
}

func (n *Node) SetPeers(peers []string) {
	assertions.Assert(len(peers) > 0, "arg peers cannot be empty")

	n.peersMu.Lock()
	defer n.peersMu.Unlock()
	n.peers = make([]string, len(peers))
	copy(n.peers, peers)

	assertions.AssertEqual(len(n.peers), len(peers), "node's peers should be equal to peers")
}

func (n *Node) GetPeers() []string {
	n.peersMu.RLock()
	defer n.peersMu.RUnlock()
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	return peers
}

func NewNode(config Config, transport protocol.Transport) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	assertions.Assert(config.SyncInterval > 0, "sync interval must be positive")
	assertions.Assert(config.MaxSyncPeers > 0, "max sync peers must be positive")
	assertions.Assert(config.Addr != "", "node address cannot be empty")
	assertions.AssertNotNil(transport, "transport cannot be nil")

	node := &Node{
		config:    config,
		counter:   crdt.NewPNCounter(config.Addr), // Initialize PNCounter CRDT
		peers:     make([]string, 0),
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

		msg, err := protocol.DecodeMessage(data)
		if err != nil {
			return fmt.Errorf("[Node %s]: StartTransport failed to read %w", n.config.Addr, err)
		}
		select {
		case n.incomingMsg <- MessageInfo{message: *msg, addr: addr}:
		default:
			log.Printf("[Node %s]: Dropping message. Channel is full", addr)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("[Node %s]: Failed to start transport listener: %w", n.config.Addr, err)
	}

	return nil
}

func (n *Node) eventLoop() {
	for {
		select {
		case <-n.ctx.Done():
			log.Printf("[Node %s] Shutting down with counter=%d", n.config.Addr, n.counter.Value())
			return

		case msg := <-n.incomingMsg:
			n.handleIncMsg(msg)

		case msg := <-n.outgoingMsg:
			assertions.Assert(msg.addr != "", "outgoing addr cannot be empty")
			data := msg.message.Encode()

			if err := n.transport.Send(msg.addr, data); err != nil {
				log.Printf("[Node %s] Failed to send message to %s: %v",
					n.config.Addr, msg.addr, err)
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

func (n *Node) broadcastUpdate() {
	ctx, cancel := context.WithTimeout(n.ctx, n.config.SyncInterval/2)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	n.peersMu.RLock()
	peers := n.peers
	n.peersMu.RUnlock()

	message := n.prepareCounterMessage(protocol.MessageTypePush)

	for _, peerAddr := range peers {
		peerAddr := peerAddr // Shadow the variable for goroutine
		g.Go(func() error {
			// Get both increment and decrement counters for logging
			increments, decrements := n.counter.Counters()

			log.Printf("[Node %s] Sent message to %s type=%d, counter=%d, inc=%v, dec=%v",
				n.config.Addr, peerAddr, message.Type, n.counter.Value(), increments, decrements)

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
		log.Printf("[Node %s] Sync round failed: %v", n.config.Addr, err)
	}
}

func (n *Node) pullState() {
	n.peersMu.RLock()
	peers := n.peers
	n.peersMu.RUnlock()

	if len(peers) == 0 {
		log.Printf("[Node %s] No peers available for sync", n.config.Addr)
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

	message := n.prepareCounterMessage(protocol.MessageTypePull)

	for _, peer := range selectedPeers[:numPeers] {
		peerAddr := peer // Shadow the variable for goroutine
		g.Go(func() error {
			// Get both increment and decrement counters for logging
			increments, decrements := n.counter.Counters()

			log.Printf("[Node %s] Sent message to %s type=%d, counter=%d, inc=%v, dec=%v",
				n.config.Addr, peerAddr, message.Type, n.counter.Value(), increments, decrements)

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
		log.Printf("[Node %s] Sync round failed: %v", n.config.Addr, err)
	}
}

func (n *Node) handleIncMsg(inc MessageInfo) {
	assertions.Assert(inc.message.Type == protocol.MessageTypePull ||
		inc.message.Type == protocol.MessageTypePush,
		"invalid message type")

	log.Printf("[Node %s] Received message from %s type=%d, nodeID=%s, inc=%v, dec=%v",
		n.config.Addr, inc.addr, inc.message.Type, inc.message.NodeID,
		inc.message.IncrementValues, inc.message.DecrementValues)

	// Create a PNCounter from received values
	tempCounter := crdt.NewPNCounter(inc.message.NodeID)
	tempCounter.MergeIncrements(inc.message.IncrementValues)
	tempCounter.MergeDecrements(inc.message.DecrementValues)

	// Merge with our local counter
	updated := n.counter.Merge(tempCounter)

	if updated {
		// If we updated our state, broadcast to other peers
		// Get both increment and decrement counters for logging
		increments, decrements := n.counter.Counters()

		log.Printf("[Node %s] Counter updated after merge: total=%d, inc=%v, dec=%v",
			n.config.Addr, n.counter.Value(), increments, decrements)
		n.broadcastUpdate()
	}

	// If it's a pull request, always respond with our current state
	if inc.message.Type == protocol.MessageTypePull {
		responseMsg := n.prepareCounterMessage(protocol.MessageTypePush)

		// Get both increment and decrement counters for logging
		increments, decrements := n.counter.Counters()

		log.Printf("[Node %s] Sent message to %s type=%d, counter=%d, inc=%v, dec=%v",
			n.config.Addr, inc.addr, responseMsg.Type, n.counter.Value(), increments, decrements)

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

	// Get both increment and decrement counters for logging
	increments, decrements := n.counter.Counters()

	log.Printf("[Node %s] Incremented counter from %d to %d, inc=%v, dec=%v",
		n.config.Addr, oldValue, newValue, increments, decrements)

	n.broadcastUpdate()
}

func (n *Node) Decrement() {
	assertions.AssertNotNil(n.counter, "node counter cannot be nil")

	oldValue := n.counter.Value()
	newValue := n.counter.Decrement(n.config.Addr)

	// Get both increment and decrement counters for logging
	increments, decrements := n.counter.Counters()

	log.Printf("[Node %s] Decremented counter from %d to %d, inc=%v, dec=%v",
		n.config.Addr, oldValue, newValue, increments, decrements)

	n.broadcastUpdate()
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
