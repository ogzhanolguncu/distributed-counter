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

// CounterVersionState holds counter and version in a single 16-byte struct
// that can be atomically updated
type CounterVersionState struct {
	Counter uint64
	Version uint64
}

type State struct {
	// Use atomic.Pointer to allow atomic updates of the entire structure
	state atomic.Pointer[CounterVersionState]
}

func NewState() *State {
	s := &State{}
	initialState := &CounterVersionState{
		Counter: 0,
		Version: 0,
	}
	s.state.Store(initialState)
	return s
}

// GetState returns the current state atomically
func (s *State) GetState() *CounterVersionState {
	return s.state.Load()
}

// Increment atomically increments the counter and version
func (s *State) Increment() (*CounterVersionState, *CounterVersionState) {
	for {
		// Load current state
		current := s.state.Load()

		// Create new state
		next := &CounterVersionState{
			Counter: current.Counter + 1,
			Version: current.Version + 1,
		}

		// Try to swap - if successful, return old and new states
		if s.state.CompareAndSwap(current, next) {
			return current, next
		}
		// If unsuccessful, someone else updated it, try again
	}
}

// Decrement atomically decrements the counter and increments the version
// If counter is already 0, this is a no-op and returns the current state twice
func (s *State) Decrement() (*CounterVersionState, *CounterVersionState) {
	for {
		// Load current state
		current := s.state.Load()

		// Check if counter is already 0
		if current.Counter == 0 {
			// Return the same state twice to indicate no change
			return current, current
		}

		// Create new state
		next := &CounterVersionState{
			Counter: current.Counter - 1,
			Version: current.Version + 1,
		}

		// Try to swap - if successful, return old and new states
		if s.state.CompareAndSwap(current, next) {
			return current, next
		}
		// If unsuccessful, someone else updated it, try again
	}
}

// UpdateIfNewer atomically updates state only if the new version is newer
func (s *State) UpdateIfNewer(newCounter uint64, newVersion uint64) bool {
	for {
		current := s.state.Load()

		// Check if we need to update
		if current.Version >= newVersion {
			return false // No update needed
		}

		// Create new state with updated values
		next := &CounterVersionState{
			Counter: newCounter,
			Version: newVersion,
		}

		// Try atomic update
		if s.state.CompareAndSwap(current, next) {
			return true
		}
		// If update failed, loop and try again
	}
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
		state:     NewState(),
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
			currentState := n.state.GetState()
			log.Printf("[Node %s] Shutting down with version=%d and counter=%d",
				n.config.Addr, currentState.Version, currentState.Counter)
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

	currentState := n.state.GetState()

	for _, peerAddr := range peers {
		peerAddr := peerAddr // Shadow the variable for goroutine
		g.Go(func() error {
			log.Printf("[Node %s] Sent message to %s type=%d, version=%d, counter=%d",
				n.config.Addr, peerAddr, protocol.MessageTypePush, currentState.Version, currentState.Counter)

			select {
			case n.outgoingMsg <- MessageInfo{
				message: protocol.Message{
					Type:    protocol.MessageTypePush,
					Version: currentState.Version,
					Counter: currentState.Counter,
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

	currentState := n.state.GetState()

	for _, peer := range selectedPeers[:numPeers] {
		peerAddr := peer // Shadow the variable for goroutine
		g.Go(func() error {
			log.Printf("[Node %s] Sent message to %s type=%d, version=%d, counter=%d",
				n.config.Addr, peerAddr, protocol.MessageTypePull, currentState.Version, currentState.Counter)

			select {
			case n.outgoingMsg <- MessageInfo{
				message: protocol.Message{
					Type:    protocol.MessageTypePull,
					Version: currentState.Version,
					Counter: currentState.Counter,
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

	updated := false

	// Try to update our state if the incoming message has a newer version
	if inc.message.Version > n.state.GetState().Version {
		updated = n.state.UpdateIfNewer(inc.message.Counter, inc.message.Version)

		if updated {
			// If we updated our state, broadcast to other peers
			n.broadcastUpdate()
		}
	}

	// If it's a pull request, always respond with our current state
	if inc.message.Type == protocol.MessageTypePull {
		currentState := n.state.GetState()
		log.Printf("[Node %s] Sent message to %s type=%d, version=%d, counter=%d",
			n.config.Addr, inc.addr, protocol.MessageTypePush, currentState.Version, currentState.Counter)

		n.outgoingMsg <- MessageInfo{
			message: protocol.Message{
				Type:    protocol.MessageTypePush,
				Version: currentState.Version,
				Counter: currentState.Counter,
			},
			addr: inc.addr,
		}
	}
}

func (n *Node) Increment() {
	assertions.AssertNotNil(n.state, "node state cannot be nil")

	oldState, newState := n.state.Increment()

	log.Printf("[Node %s] Incremented counter from %d to %d, version from %d to %d",
		n.config.Addr, oldState.Counter, newState.Counter, oldState.Version, newState.Version)

	n.broadcastUpdate()
}

func (n *Node) Decrement() {
	assertions.AssertNotNil(n.state, "node state cannot be nil")

	oldState, newState := n.state.Decrement()

	// Check if the state actually changed (no-op check)
	if oldState.Counter == newState.Counter && oldState.Version == newState.Version {
		log.Printf("[Node %s] Decrement no-op: counter already at zero", n.config.Addr)
		return // No need to broadcast since state didn't change
	}

	log.Printf("[Node %s] Decremented counter from %d to %d, version from %d to %d",
		n.config.Addr, oldState.Counter, newState.Counter, oldState.Version, newState.Version)

	n.broadcastUpdate()
}

func (n *Node) GetCounter() uint64 {
	assertions.AssertNotNil(n.state, "node state cannot be nil")
	return n.state.GetState().Counter
}

func (n *Node) GetVersion() uint64 {
	assertions.AssertNotNil(n.state, "node state cannot be nil")
	return n.state.GetState().Version
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
