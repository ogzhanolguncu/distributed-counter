package peer

import "sync"

type PeerManager interface {
	AddPeer(addr string) Peer
	RemovePeer(addr string)
	GetPeer(addr string) (Peer, bool)
	GetAllPeers() []Peer
	GetAllActivePeers() []Peer
	UpdatePeerVersion(addr string, version uint32)
}

type Manager struct {
	peers map[string]Peer
	mu    sync.RWMutex
}

func NewPeerManager() *Manager {
	return &Manager{
		peers: make(map[string]Peer),
		mu:    sync.RWMutex{},
	}
}

func (pm *Manager) AddPeer(addr string) Peer {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	newPeer := NewPeer(addr)
	pm.peers[addr] = newPeer
	return newPeer
}

func (pm *Manager) RemovePeer(addr string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	delete(pm.peers, addr)
}

func (pm *Manager) GetPeer(addr string) (Peer, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	peer, found := pm.peers[addr]
	return peer, found
}

func (pm *Manager) GetAllPeers() []Peer {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	peers := make([]Peer, 0, len(pm.peers))

	for _, peer := range pm.peers {
		peers = append(peers, peer)
	}

	return peers
}

func (pm *Manager) UpdatePeerVersion(addr string, version uint32) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if peer, exists := pm.peers[addr]; exists {
		peer.UpdateVersion(version)
		peer.UpdateLastSeen()
	}
}

func (pm *Manager) Cleanup() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for addr, peer := range pm.peers {
		if !peer.IsActive() {
			delete(pm.peers, addr)
		}
	}
}

func (pm *Manager) GetAllActivePeers() []Peer {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	peers := make([]Peer, 0)
	for _, peer := range pm.peers {
		if peer.IsActive() {
			peers = append(peers, peer)
		}
	}
	return peers
}
