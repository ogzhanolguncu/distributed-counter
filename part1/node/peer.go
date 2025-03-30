package node

import (
	"sync"

	"github.com/ogzhanolguncu/distributed-counter/part1/assertions"
)

type Peer struct {
	peers   []string
	peersMu sync.RWMutex
}

func NewPeer() *Peer {
	return &Peer{
		peers:   make([]string, 0),
		peersMu: sync.RWMutex{},
	}
}

func (p *Peer) SetPeers(peers []string) {
	assertions.Assert(len(peers) > 0, "arg peers cannot be empty")

	p.peersMu.Lock()
	defer p.peersMu.Unlock()
	p.peers = make([]string, len(peers))
	copy(p.peers, peers)

	assertions.AssertEqual(len(p.peers), len(peers), "node's peers should be equal to peers")
}

func (p *Peer) GetPeers() []string {
	p.peersMu.RLock()
	defer p.peersMu.RUnlock()
	peers := make([]string, len(p.peers))
	copy(peers, p.peers)
	return peers
}
