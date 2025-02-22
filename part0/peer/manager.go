package peer

type Manager interface {
	AddPeer(addr string) Peer
	RemovePeer(addr string)
	GetPeer(addr string) (Peer, bool)
	GetAllPeers() []Peer
	UpdatePeerVersion(addr string, version uint32)
}
