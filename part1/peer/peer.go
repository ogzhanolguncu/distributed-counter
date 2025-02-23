package peer

import "time"

type PeerState struct {
	Addr     string
	Version  uint32
	LastSeen time.Time
	isActive bool
}

type Peer interface {
	GetAddr() string
	GetVersion() uint32
	IsActive() bool
	UpdateVersion(version uint32)
	UpdateLastSeen()
}

func NewPeer(addr string) *PeerState {
	return &PeerState{
		isActive: true,
		LastSeen: time.Now(),
		Addr:     addr,
	}
}

func (p *PeerState) GetAddr() string {
	return p.Addr
}

func (p *PeerState) GetVersion() uint32 {
	return p.Version
}

func (p *PeerState) IsActive() bool {
	return p.isActive && time.Since(p.LastSeen) < time.Minute
}

func (p *PeerState) UpdateVersion(version uint32) {
	p.Version = version
}

func (p *PeerState) UpdateLastSeen() {
	p.LastSeen = time.Now()
}
