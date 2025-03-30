package peer

import (
	"sync"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part3/assertions"
)

type Peer struct {
	Addr             string
	LastActive       time.Time
	ConsecutiveFails int
}

type PeerManager struct {
	peers               map[string]*Peer
	mu                  sync.RWMutex
	maxConsecutiveFails int
	failureTimeout      time.Duration
}

func NewPeerManager(maxConsecutiveFails int, failureTimeout time.Duration) *PeerManager {
	assertions.Assert(failureTimeout > 0, "failureTimeout must be positive")
	assertions.Assert(maxConsecutiveFails > 0, "maxConsecutiveFails must be positive")

	pm := &PeerManager{
		peers:               make(map[string]*Peer),
		maxConsecutiveFails: maxConsecutiveFails,
		failureTimeout:      failureTimeout,
	}

	assertions.AssertNotNil(pm.peers, "peers map must be initialized")
	return pm
}

func (pm *PeerManager) AddPeer(addr string) {
	assertions.Assert(addr != "", "peer address cannot be empty")
	assertions.AssertNotNil(pm.peers, "peers map cannot be nil")

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if existing, exists := pm.peers[addr]; exists {
		existing.ConsecutiveFails = 0
		existing.LastActive = time.Now()
	} else {
		pm.peers[addr] = &Peer{
			Addr:             addr,
			LastActive:       time.Now(),
			ConsecutiveFails: 0,
		}
	}

	assertions.AssertNotNil(pm.peers[addr], "peer must exist after adding")
}

func (pm *PeerManager) RemovePeer(addr string) {
	assertions.Assert(addr != "", "peer address cannot be empty")
	assertions.AssertNotNil(pm.peers, "peers map cannot be nil")

	pm.mu.Lock()
	defer pm.mu.Unlock()

	delete(pm.peers, addr)

	assertions.Assert(pm.peers[addr] == nil, "peer must not exist after removal")
}

func (pm *PeerManager) GetPeers() []string {
	assertions.AssertNotNil(pm.peers, "peers map cannot be nil")

	pm.mu.RLock()
	defer pm.mu.RUnlock()

	peers := make([]string, 0, len(pm.peers))

	for addr, peer := range pm.peers {
		if time.Since(peer.LastActive) >= pm.failureTimeout || peer.ConsecutiveFails >= pm.maxConsecutiveFails {
			continue
		}
		assertions.Assert(addr != "", "stored peer address cannot be empty")
		peers = append(peers, addr)
	}

	return peers
}

func (pm *PeerManager) ClearPeers() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	clear(pm.peers)

	assertions.AssertEqual(len(pm.peers), 0, "peers should be empty after clear")
}

func (pm *PeerManager) MarkPeerActive(addr string) {
	assertions.Assert(addr != "", "peer address cannot be empty")
	assertions.AssertNotNil(pm.peers, "peers map cannot be nil")

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if peer, exists := pm.peers[addr]; exists {
		peer.ConsecutiveFails = 0
		peer.LastActive = time.Now()
	}
}

func (pm *PeerManager) MarkPeerFailed(addr string) bool {
	assertions.Assert(addr != "", "peer address cannot be empty")
	assertions.AssertNotNil(pm.peers, "peers map cannot be nil")

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if peer, exists := pm.peers[addr]; exists {
		peer.ConsecutiveFails++

		// Peer should be considered inactive
		return peer.ConsecutiveFails >= pm.maxConsecutiveFails
	}

	return false
}

func (pm *PeerManager) PruneStalePeers() {
	assertions.AssertNotNil(pm.peers, "peers map cannot be nil")

	pm.mu.Lock()
	defer pm.mu.Unlock()

	for addr, peer := range pm.peers {
		if time.Since(peer.LastActive) >= pm.failureTimeout || peer.ConsecutiveFails >= pm.maxConsecutiveFails {
			delete(pm.peers, addr)
		}
	}
}
