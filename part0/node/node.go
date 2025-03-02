package node

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part0/assertions"
	"github.com/ogzhanolguncu/distributed-counter/part0/protocol"
	"golang.org/x/sync/errgroup"
)

const defaultChannelBuffer = 100

type Config struct {
	Addr         string
	SyncInterval time.Duration
	MaxSyncPeers int
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
	config Config
	state  *State

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
		state:     &State{},
		peers:     make([]string, 0),
		ctx:       ctx,
		cancel:    cancel,
		transport: transport,

		incomingMsg: make(chan MessageInfo, defaultChannelBuffer),
		outgoingMsg: make(chan MessageInfo, defaultChannelBuffer),
		syncTick:    time.NewTicker(config.SyncInterval).C,
	}

	assertions.AssertNotNil(node.state, "node state must be initialized")
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
			log.Printf("[Node %s] Shutting down with version=%d and counter=%d",
				n.config.Addr, n.state.version.Load(), n.state.counter.Load())
			return

		case msg := <-n.incomingMsg:
			n.handleIncMsg(msg)

		case msg := <-n.outgoingMsg:
			assertions.Assert(msg.addr != "", "outgoing addr cannot be empty")
			assertions.AssertEqual(protocol.MessageSize, len(msg.message.Encode()), fmt.Sprintf("formatted message cannot be smaller than %d", protocol.MessageSize))

			if err := n.transport.Send(msg.addr, msg.message.Encode()); err != nil {
				log.Printf("[Node %s] Failed to send message to %s: %v",
					n.config.Addr, msg.addr, err)
			}

		case <-n.syncTick:
			n.pullState()
		}
	}
}

func (n *Node) broadcastUpdate() {
	ctx, cancel := context.WithTimeout(n.ctx, n.config.SyncInterval/2)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	n.peersMu.RLock()
	peers := n.peers
	n.peersMu.RUnlock()

	for _, peerAddr := range peers {
		peerAddr := peerAddr // Shadow the variable for goroutine
		g.Go(func() error {
			log.Printf("[Node %s] Sent message to %s type=%d, version=%d, counter=%d",
				n.config.Addr, peerAddr, protocol.MessageTypePush, n.state.version.Load(), n.state.counter.Load())

			select {
			case n.outgoingMsg <- MessageInfo{
				message: protocol.Message{
					Type:    protocol.MessageTypePush,
					Version: n.state.version.Load(),
					Counter: n.state.counter.Load(),
				},
				addr: peerAddr,
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

// We periodically pull other node's states. This is also called "Anti-entropy".
// This is really good to prevent data loss and to make late joining nodes converge faster
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

	for _, peer := range selectedPeers[:numPeers] {
		peerAddr := peer // Shadow the variable for goroutine
		g.Go(func() error {
			log.Printf("[Node %s] Sent message to %s type=%d, version=%d, counter=%d",
				n.config.Addr, peerAddr, protocol.MessageTypePull, n.state.version.Load(), n.state.counter.Load())

			select {
			case n.outgoingMsg <- MessageInfo{
				message: protocol.Message{
					Type:    protocol.MessageTypePull,
					Version: n.state.version.Load(),
					Counter: n.state.counter.Load(),
				},
				addr: peerAddr,
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

	log.Printf("[Node %s] Received message from %s type=%d, version=%d, counter=%d",
		n.config.Addr, inc.addr, inc.message.Type, inc.message.Version, inc.message.Counter)

	switch inc.message.Type {
	case protocol.MessageTypePull:
		if inc.message.Version > n.state.version.Load() {
			oldVersion := n.state.version.Load()
			n.state.version.Store(inc.message.Version)
			n.state.counter.Store(inc.message.Counter)
			assertions.Assert(n.state.version.Load() > oldVersion, "version must increase after update")
			n.broadcastUpdate()
		}

		log.Printf("[Node %s] Sent message to %s type=%d, version=%d, counter=%d",
			n.config.Addr, inc.addr, protocol.MessageTypePush, n.state.version.Load(), n.state.counter.Load())
		n.outgoingMsg <- MessageInfo{
			message: protocol.Message{
				Type:    protocol.MessageTypePush,
				Version: n.state.version.Load(),
				Counter: n.state.counter.Load(),
			},
			addr: inc.addr,
		}
	case protocol.MessageTypePush:
		if inc.message.Version > n.state.version.Load() {
			oldVersion := n.state.version.Load()
			n.state.version.Store(inc.message.Version)
			n.state.counter.Store(inc.message.Counter)
			assertions.Assert(n.state.version.Load() > oldVersion, "version must increase after update")
			n.broadcastUpdate()
		}
	}
}

func (n *Node) Increment() {
	assertions.AssertNotNil(n.state, "node state cannot be nil")
	oldCounter := n.state.counter.Load()
	oldVersion := n.state.version.Load()

	n.state.counter.Add(1)
	n.state.version.Add(1)

	assertions.Assert(oldCounter < n.state.counter.Load(), "counter must increase after Increment")
	assertions.Assert(oldVersion < n.state.version.Load(), "version must increase after Increment")
	n.broadcastUpdate()
}

func (n *Node) Decrement() {
	assertions.AssertNotNil(n.state, "node state cannot be nil")
	oldCounter := n.state.counter.Load()
	oldVersion := n.state.version.Load()

	n.state.counter.Add(^uint64(0))
	n.state.version.Add(1)

	assertions.Assert(oldCounter > n.state.counter.Load(), "counter must decrease after Decrease")
	assertions.Assert(oldVersion < n.state.version.Load(), "version must increase after Increment")

	n.broadcastUpdate()
}

func (n *Node) GetCounter() uint64 {
	assertions.AssertNotNil(n.state, "node state cannot be nil")
	return n.state.counter.Load()
}

func (n *Node) GetVersion() uint32 {
	assertions.AssertNotNil(n.state, "node state cannot be nil")
	return n.state.version.Load()
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
