package peer

import "sync"

type Peer struct {
	Addr string
}

type PeerManager struct {
	peers map[string]*Peer
	mu    sync.RWMutex
}

func NewPeerManager() *PeerManager {
	return &PeerManager{
		peers: make(map[string]*Peer),
	}
}

func (pm *PeerManager) AddPeer(addr string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.peers[addr] = &Peer{Addr: addr}
}

func (pm *PeerManager) RemovePeer(addr string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.peers, addr)
}

func (pm *PeerManager) GetPeers() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	peers := make([]string, 0, len(pm.peers))
	for addr := range pm.peers {
		peers = append(peers, addr)
	}
	return peers
}
