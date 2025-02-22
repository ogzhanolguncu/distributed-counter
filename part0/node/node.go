package node

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

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
	counter uint64
	version uint32
	mu      sync.RWMutex
}

type MessageInfo struct {
	message protocol.Message
	addr    string
}

type Node struct {
	config    Config
	state     *State
	peers     []string
	transport protocol.Transport
	ctx       context.Context
	cancel    context.CancelFunc

	incomingMsg chan MessageInfo
	outgoingMsg chan MessageInfo
	syncTick    <-chan time.Time
}

func NewNode(config Config, transport protocol.Transport) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

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

	if err := node.startTransport(); err != nil {
		cancel() // Clean up if we fail to start
		return nil, err
	}

	go node.eventLoop()
	return node, nil
}

func (n *Node) startTransport() error {
	err := n.transport.Listen(func(addr string, data []byte) error {
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
				n.config.Addr, n.state.version, n.state.counter)
			return

		case msg := <-n.incomingMsg:
			n.handleIncMsg(msg)

		case msg := <-n.outgoingMsg:
			if err := n.transport.Send(msg.addr, msg.message.Encode()); err != nil {
				log.Printf("[Node %s] Failed to send message to %s: %v",
					n.config.Addr, msg.addr, err)
			}
		case <-n.syncTick:
			n.pullState()
		}
	}
}

// To make the convergence faster, we broadcast latest update to every other node in the cluster
// func (n *Node) broadcastUpdate() {}

// We periodically pull other node's states. This is also called "Anti-entropy".
// This is really good to prevent data loss and to make late joining nodes converge faster
func (n *Node) pullState() {
	n.state.mu.RLock()
	peers := n.peers
	currentVersion := n.state.version
	n.state.mu.RUnlock()

	if len(peers) == 0 {
		log.Printf("[Node %s] No peers available for sync", n.config.Addr)
		return
	}

	numPeers := min(n.config.MaxSyncPeers, len(peers))
	selectedPeers := make([]string, len(peers))
	copy(selectedPeers, peers)
	rand.Shuffle(len(selectedPeers), func(i, j int) {
		selectedPeers[i], selectedPeers[j] = selectedPeers[j], selectedPeers[i]
	})

	ctx, cancel := context.WithTimeout(n.ctx, n.config.SyncInterval/2)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	for _, peer := range selectedPeers[:numPeers] {
		peerAddr := peer // Create new variable for goroutine
		g.Go(func() error {
			log.Printf("[Node %s] Sent message to %s type=%d, version=%d, counter=%d",
				n.config.Addr, peerAddr, protocol.MessageTypePull, n.state.version, n.state.counter)

			select {
			case n.outgoingMsg <- MessageInfo{
				message: protocol.Message{
					Type:    protocol.MessageTypePull,
					Version: currentVersion,
					Counter: n.state.counter,
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

// Handle all incoming traffic then decide if it's pull or push then act
func (n *Node) handleIncMsg(inc MessageInfo) {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()

	log.Printf("[Node %s] Received message from %s type=%d, version=%d, counter=%d",
		n.config.Addr, inc.addr, inc.message.Type, inc.message.Version, inc.message.Counter)

	switch inc.message.Type {
	case protocol.MessageTypePull:

		// TODO: Later we'll propagate that change to others to converge faster
		if inc.message.Version > n.state.version {
			n.state.version = inc.message.Version
			n.state.counter = inc.message.Counter
		}

		log.Printf("[Node %s] Sent message to %s type=%d, version=%d, counter=%d",
			n.config.Addr, inc.addr, protocol.MessageTypePush, n.state.version, n.state.counter)
		n.outgoingMsg <- MessageInfo{
			message: protocol.Message{
				Type:    protocol.MessageTypePush,
				Version: n.state.version,
				Counter: n.state.counter,
			},
			addr: inc.addr,
		}

	case protocol.MessageTypePush:
		// TODO: Later we'll propagate that change to others to converge faster
		if inc.message.Version > n.state.version {
			n.state.version = inc.message.Version
			n.state.counter = inc.message.Counter
		}
	}
}

func (n *Node) Close() error {
	n.cancel()
	return n.transport.Close()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
