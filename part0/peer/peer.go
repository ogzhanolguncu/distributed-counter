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

type peer struct{}
