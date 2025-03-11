package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part3/assertions"
	"github.com/ogzhanolguncu/distributed-counter/part3/peer"
	"github.com/ogzhanolguncu/distributed-counter/part3/protocol"
	"github.com/ogzhanolguncu/distributed-counter/part3/wal"
	"golang.org/x/sync/errgroup"
)

const (
	defaultChannelBuffer = 100
	defaultMaxFileSize   = 64 * 1024 * 1024 // 64MB
	defaultWalDir        = "wal"
)

type OperationType string

const (
	OpIncrement OperationType = "increment"
	OpDecrement OperationType = "decrement"
	OpUpdate    OperationType = "update"
)

type NodeState struct {
	Counter uint64 `json:"counter"`
	Version uint32 `json:"version"`
}

type WalEntry struct {
	Operation OperationType `json:"operation"`
	Counter   uint64        `json:"counter"`
	Version   uint32        `json:"version"`
	Timestamp int64         `json:"timestamp"`
}

type State struct {
	counter atomic.Uint64
	version atomic.Uint32
}

type Config struct {
	Addr                string
	SyncInterval        time.Duration
	MaxSyncPeers        int
	MaxConsecutiveFails int
	FailureTimeout      time.Duration
	WalDir              string
	EnableWal           bool
	EnableWalFsync      bool
	MaxWalFileSize      int64
}

type MessageInfo struct {
	message protocol.Message
	addr    string
}

type Node struct {
	config Config
	state  *State

	peers *peer.PeerManager

	transport protocol.Transport
	ctx       context.Context
	cancel    context.CancelFunc

	incomingMsg chan MessageInfo
	outgoingMsg chan MessageInfo
	syncTick    <-chan time.Time

	wal *wal.WAL
}

func NewNode(config Config, transport protocol.Transport, peerManager *peer.PeerManager) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	assertions.Assert(config.SyncInterval > 0, "sync interval must be positive")
	assertions.Assert(config.FailureTimeout > 0, "failure timeout must be positive")
	assertions.Assert(config.MaxConsecutiveFails > 0, "max consecutive fails must be positive")
	assertions.Assert(config.MaxSyncPeers > 0, "max sync peers must be positive")
	assertions.Assert(config.Addr != "", "node address cannot be empty")
	assertions.AssertNotNil(transport, "transport cannot be nil")
	assertions.AssertNotNil(peerManager, "peer manager cannot be nil")

	if config.WalDir == "" && config.EnableWal {
		config.WalDir = filepath.Join(defaultWalDir, config.Addr)
	}

	if config.MaxWalFileSize <= 0 && config.EnableWal {
		config.MaxWalFileSize = defaultMaxFileSize
	}

	node := &Node{
		config:    config,
		state:     &State{},
		peers:     peerManager,
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

	// Setup WAL if enabled
	if config.EnableWal {
		if err := node.setupWAL(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to setup WAL: %w", err)
		}
	}

	if err := node.startTransport(); err != nil {
		cancel() // Clean up if we fail to start
		if node.wal != nil {
			node.wal.Close()
		}
		return nil, err
	}

	go node.eventLoop()
	go node.pruneStaleNodes()

	return node, nil
}

func (n *Node) setupWAL() error {
	walInstance, err := wal.OpenWAL(n.config.WalDir, n.config.EnableWalFsync, n.config.MaxWalFileSize)
	if err != nil {
		return fmt.Errorf("failed to initialize WAL: %w", err)
	}
	n.wal = walInstance

	if err := n.restoreFromWAL(); err != nil {
		log.Printf("[Node %s] Warning: Failed to restore from WAL: %v", n.config.Addr, err)
	}

	return nil
}

func (n *Node) restoreFromWAL() error {
	if n.wal == nil {
		return fmt.Errorf("WAL not initialized")
	}

	entries, err := n.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read from WAL: %w", err)
	}

	if len(entries) == 0 {
		log.Printf("[Node %s] No entries in WAL to restore", n.config.Addr)
		return nil
	}

	// Replay the WAL entries to reconstruct state
	var highestVersion uint32 = 0
	var finalCounter uint64 = 0
	var lastAppliedSeq uint64 = 0

	// First pass: find the highest sequence we've seen
	for _, entry := range entries {
		if entry.SequenceNumber > lastAppliedSeq {
			lastAppliedSeq = entry.SequenceNumber
		}
	}

	// Second pass: apply operations in order
	for _, entry := range entries {
		var walEntry WalEntry
		if err := json.Unmarshal(entry.Data, &walEntry); err != nil {
			// Try the old format for backward compatibility
			var oldState NodeState
			if err := json.Unmarshal(entry.Data, &oldState); err != nil {
				log.Printf("[Node %s] Warning: Failed to decode WAL entry: %v", n.config.Addr, err)
				continue
			}

			// Use the highest version state from old format
			if oldState.Version > highestVersion {
				highestVersion = oldState.Version
				finalCounter = oldState.Counter
			}
			continue
		}

		switch walEntry.Operation {
		case OpIncrement:
			finalCounter++
			highestVersion = walEntry.Version
		case OpDecrement:
			finalCounter--
			highestVersion = walEntry.Version
		case OpUpdate:
			finalCounter = walEntry.Counter
			highestVersion = walEntry.Version
		}
	}

	n.state.version.Store(highestVersion)
	n.state.counter.Store(finalCounter)

	log.Printf("[Node %s] Restored from WAL: version=%d, counter=%d",
		n.config.Addr, highestVersion, finalCounter)

	return nil
}

func (n *Node) logOperation(opType OperationType, counter uint64, version uint32) error {
	if n.wal == nil {
		return nil // WAL not enabled, silently succeed
	}

	walEntry := WalEntry{
		Operation: opType,
		Counter:   counter,
		Version:   version,
		Timestamp: time.Now().UnixNano(),
	}

	data, err := json.Marshal(walEntry)
	if err != nil {
		return fmt.Errorf("failed to encode operation: %w", err)
	}

	if err := n.wal.WriteEntry(data); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	return nil
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
		assertions.AssertNotNil(data, "incoming data cannot be nil")

		msg, err := protocol.DecodeMessage(data)
		if err != nil {
			return fmt.Errorf("[Node %s]: StartTransport failed to read %w", n.config.Addr, err)
		}

		assertions.AssertNotNil(msg, "decoded message cannot be nil")
		assertions.Assert(msg.Type == protocol.MessageTypePull || msg.Type == protocol.MessageTypePush,
			"invalid message type")

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
			assertions.Assert(msg.addr != "", "incoming message addr cannot be empty")
			n.handleIncMsg(msg)

		case msg := <-n.outgoingMsg:
			assertions.Assert(msg.addr != "", "outgoing addr cannot be empty")
			assertions.Assert(msg.message.Type == protocol.MessageTypePull ||
				msg.message.Type == protocol.MessageTypePush, "invalid message type")

			encodedMsg := msg.message.Encode()
			assertions.AssertEqual(protocol.MessageSize, len(encodedMsg),
				fmt.Sprintf("encoded message must be exactly %d bytes", protocol.MessageSize))

			if err := n.transport.Send(msg.addr, encodedMsg); err != nil {
				log.Printf("[Node %s] Failed to send message to %s: %v",
					n.config.Addr, msg.addr, err)

				if n.peers.MarkPeerFailed(msg.addr) {
					log.Printf("[Node %s] Peer %s is now considered inactive after %d consecutive failures",
						n.config.Addr, msg.addr, n.config.MaxConsecutiveFails)
				}

			} else {
				n.peers.MarkPeerActive(msg.addr)
			}

		case <-n.syncTick:
			n.pullState()
		}
	}
}

func (n *Node) broadcastUpdate() {
	assertions.AssertNotNil(n.state, "node state cannot be nil")
	assertions.AssertNotNil(n.peers, "peer manager cannot be nil")

	ctx, cancel := context.WithTimeout(n.ctx, n.config.SyncInterval/2)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	peers := n.peers.GetPeers()
	for _, peerAddr := range peers {
		assertions.Assert(peerAddr != "", "peer address cannot be empty")

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

func (n *Node) pullState() {
	assertions.AssertNotNil(n.peers, "peer manager cannot be nil")

	peers := n.peers.GetPeers()

	if len(peers) == 0 {
		log.Printf("[Node %s] No peers available for sync", n.config.Addr)
		return
	}

	numPeers := min(n.config.MaxSyncPeers, len(peers))
	assertions.Assert(numPeers > 0, "number of peers to sync with must be positive")

	selectedPeers := make([]string, 0, len(peers))
	for _, peer := range peers {
		assertions.Assert(peer != "", "peer address cannot be empty")
		selectedPeers = append(selectedPeers, peer)
	}

	assertions.AssertEqual(len(selectedPeers), len(peers), "all peers must be selected initially")

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
	assertions.Assert(inc.addr != "", "incoming message address cannot be empty")
	assertions.Assert(inc.message.Type == protocol.MessageTypePull ||
		inc.message.Type == protocol.MessageTypePush, "invalid message type")

	log.Printf("[Node %s] Received message from %s type=%d, version=%d, counter=%d",
		n.config.Addr, inc.addr, inc.message.Type, inc.message.Version, inc.message.Counter)

	// Add new peers that were not previously known
	if !slices.Contains(n.peers.GetPeers(), inc.addr) {
		n.peers.AddPeer(inc.addr)
	} else {
		n.peers.MarkPeerActive(inc.addr)
	}

	switch inc.message.Type {
	case protocol.MessageTypePull:
		if inc.message.Version > n.state.version.Load() {
			if err := n.logOperation(OpUpdate, inc.message.Counter, inc.message.Version); err != nil {
				log.Printf("[Node %s] Failed to log update operation to WAL: %v", n.config.Addr, err)
				// Continue with the operation anyway as we already have the data from peer
			}

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
			if err := n.logOperation(OpUpdate, inc.message.Counter, inc.message.Version); err != nil {
				log.Printf("[Node %s] Failed to log update operation to WAL: %v", n.config.Addr, err)
				// Continue with the operation anyway as we already have the data from peer
			}

			oldVersion := n.state.version.Load()
			n.state.version.Store(inc.message.Version)
			n.state.counter.Store(inc.message.Counter)
			assertions.Assert(n.state.version.Load() > oldVersion, "version must increase after update")

			n.broadcastUpdate()
		}
	}
}

func (n *Node) GetPeerManager() *peer.PeerManager {
	assertions.AssertNotNil(n.peers, "peer manager cannot be nil")
	return n.peers
}

func (n *Node) Increment() {
	assertions.AssertNotNil(n.state, "node state cannot be nil")
	oldCounter := n.state.counter.Load()
	oldVersion := n.state.version.Load()
	newVersion := oldVersion + 1

	if err := n.logOperation(OpIncrement, oldCounter+1, newVersion); err != nil {
		log.Printf("[Node %s] Failed to log increment operation to WAL: %v", n.config.Addr, err)
		return
	}

	n.state.counter.Add(1)
	n.state.version.Add(1)

	assertions.Assert(n.state.counter.Load() > oldCounter, "counter must increase after Increment")
	assertions.Assert(n.state.version.Load() > oldVersion, "version must increase after Increment")

	n.broadcastUpdate()
}

func (n *Node) Decrement() {
	assertions.AssertNotNil(n.state, "node state cannot be nil")
	oldCounter := n.state.counter.Load()
	oldVersion := n.state.version.Load()
	newVersion := oldVersion + 1

	if err := n.logOperation(OpDecrement, oldCounter-1, newVersion); err != nil {
		log.Printf("[Node %s] Failed to log decrement operation to WAL: %v", n.config.Addr, err)
		return
	}

	n.state.counter.Add(^uint64(0)) // Subtract 1
	n.state.version.Add(1)

	assertions.Assert(n.state.counter.Load() < oldCounter, "counter must decrease after Decrement")
	assertions.Assert(n.state.version.Load() > oldVersion, "version must increase after Decrement")

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

	if n.wal != nil {
		if err := n.wal.Close(); err != nil {
			log.Printf("[Node %s] Error closing WAL: %v", n.config.Addr, err)
		}
	}

	return n.transport.Close()
}
