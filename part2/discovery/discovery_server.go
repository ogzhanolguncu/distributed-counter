package discovery

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

type DiscoveryServer struct {
	addr       string
	knownPeers map[string]*PeerInfo
	mu         sync.Mutex
	httpSrv    *http.Server
	done       chan struct{}
}

type PeerInfo struct {
	Addr     string `json:"addr"`
	LastSeen time.Time
}

type RegisterRequest struct {
	Addr string `json:"addr"`
}

func NewDiscoveryServer(addr string, cleanupInterval time.Duration) *DiscoveryServer {
	ds := &DiscoveryServer{
		addr:       addr,
		knownPeers: make(map[string]*PeerInfo),
		mu:         sync.Mutex{},
		done:       make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /register", ds.handleRegister)
	mux.HandleFunc("GET /peers", ds.handlePeers)
	mux.HandleFunc("POST /heartbeat", ds.handleHeartbeat)

	ds.httpSrv = &http.Server{
		Handler:      mux,
		Addr:         addr,
		ReadTimeout:  httpSrvReadTimeout,
		WriteTimeout: httpSrvWriteTimeout,
	}

	go ds.cleanupInactivePeers(cleanupInterval)
	return ds
}

func (ds *DiscoveryServer) Start() error {
	log.Printf("[Discovery Server] started listing at: %s", ds.addr)
	return ds.httpSrv.ListenAndServe()
}

func (ds *DiscoveryServer) Stop() error {
	close(ds.done)
	return ds.httpSrv.Close()
}

func (ds *DiscoveryServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Addr == "" {
		http.Error(w, "Address cannot be empty", http.StatusBadRequest)
		return
	}

	ds.mu.Lock()
	ds.knownPeers[req.Addr] = &PeerInfo{
		Addr:     req.Addr,
		LastSeen: time.Now(),
	}
	ds.mu.Unlock()

	log.Printf("[Discovery Server] Registered new peer: %s", req.Addr)
	w.WriteHeader(http.StatusOK)
}

func (ds *DiscoveryServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Addr == "" {
		http.Error(w, "Address cannot be empty", http.StatusBadRequest)
		return
	}

	ds.mu.Lock()
	if peer, found := ds.knownPeers[req.Addr]; found {
		peer.LastSeen = time.Now()
	}
	ds.mu.Unlock()

	log.Printf("[Discovery Server] Heartbeat received from: %s", req.Addr)
	w.WriteHeader(http.StatusOK)
}

func (ds *DiscoveryServer) handlePeers(w http.ResponseWriter, r *http.Request) {
	ds.mu.Lock()
	peerList := make([]*PeerInfo, 0, len(ds.knownPeers))
	for _, peer := range ds.knownPeers {
		peerList = append(peerList, &PeerInfo{
			Addr: peer.Addr,
		})
	}
	ds.mu.Unlock()

	peers, err := json.Marshal(peerList)
	if err != nil {
		http.Error(w, "Peer list cannot be marshaled", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", mimeJson)
	w.Write(peers)
}

func (ds *DiscoveryServer) cleanupInactivePeers(cleanupInterval time.Duration) {
	ticker := time.NewTicker(cleanupInterval)

	for {
		select {
		case <-ticker.C:
			ds.mu.Lock()
			for _, peer := range ds.knownPeers {
				if time.Since(peer.LastSeen) > cleanupInterval {
					delete(ds.knownPeers, peer.Addr)
					log.Printf("[Discovery Server] Inactive peer removed: %s", peer.Addr)
				}
			}
			ds.mu.Unlock()

		case <-ds.done:
			ticker.Stop()
			return
		}
	}
}
