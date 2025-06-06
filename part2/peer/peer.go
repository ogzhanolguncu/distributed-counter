package peer

import (
	"sync"

	"github.com/ogzhanolguncu/distributed-counter/part2/assertions"
)

type Peer struct {
	Addr string
}

type PeerManager struct {
	peers map[string]*Peer
	mu    sync.RWMutex
}

func NewPeerManager() *PeerManager {
	pm := &PeerManager{
		peers: make(map[string]*Peer),
	}

	assertions.AssertNotNil(pm.peers, "peers map must be initialized")

	return pm
}

func (pm *PeerManager) AddPeer(addr string) {
	assertions.Assert(addr != "", "peer address cannot be empty")
	assertions.AssertNotNil(pm.peers, "peers map cannot be nil")

	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.peers[addr] = &Peer{Addr: addr}

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
	for addr := range pm.peers {
		assertions.Assert(addr != "", "stored peer address cannot be empty")
		peers = append(peers, addr)
	}

	assertions.AssertEqual(len(peers), len(pm.peers), "returned peers slice must contain all peers")

	return peers
}
