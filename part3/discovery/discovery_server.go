package discovery

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part3/assertions"
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
	assertions.Assert(addr != "", "discovery server address cannot be empty")
	assertions.Assert(cleanupInterval > 0, "cleanup interval must be positive")

	ds := &DiscoveryServer{
		addr:       addr,
		knownPeers: make(map[string]*PeerInfo),
		mu:         sync.Mutex{},
		done:       make(chan struct{}),
	}

	assertions.AssertNotNil(ds.knownPeers, "peers map must be initialized")
	assertions.AssertNotNil(ds.done, "done channel must be initialized")

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
	assertions.AssertNotNil(ds.httpSrv, "HTTP server cannot be nil")
	assertions.Assert(ds.addr != "", "discovery server address cannot be empty")

	log.Printf("[Discovery Server] started listing at: %s", ds.addr)
	return ds.httpSrv.ListenAndServe()
}

func (ds *DiscoveryServer) Stop() error {
	assertions.AssertNotNil(ds.done, "done channel cannot be nil")
	assertions.AssertNotNil(ds.httpSrv, "HTTP server cannot be nil")

	close(ds.done)
	return ds.httpSrv.Close()
}

func (ds *DiscoveryServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	assertions.AssertNotNil(ds.knownPeers, "peers map cannot be nil")

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

	assertions.AssertNotNil(ds.knownPeers[req.Addr], "peer must be in map after registration")
	ds.mu.Unlock()

	log.Printf("[Discovery Server] Registered new peer: %s", req.Addr)
	w.WriteHeader(http.StatusOK)
}

func (ds *DiscoveryServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	assertions.AssertNotNil(ds.knownPeers, "peers map cannot be nil")

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
		oldTime := peer.LastSeen
		peer.LastSeen = time.Now()

		assertions.Assert(peer.LastSeen.After(oldTime), "last seen time must be updated")
	}
	ds.mu.Unlock()

	log.Printf("[Discovery Server] Heartbeat received from: %s", req.Addr)
	w.WriteHeader(http.StatusOK)
}

func (ds *DiscoveryServer) handlePeers(w http.ResponseWriter, r *http.Request) {
	assertions.AssertNotNil(ds.knownPeers, "peers map cannot be nil")

	ds.mu.Lock()
	peerList := make([]*PeerInfo, 0, len(ds.knownPeers))
	for _, peer := range ds.knownPeers {
		assertions.AssertNotNil(peer, "peer cannot be nil")
		assertions.Assert(peer.Addr != "", "peer address cannot be empty")

		peerList = append(peerList, &PeerInfo{
			Addr: peer.Addr,
		})
	}
	ds.mu.Unlock()

	assertions.AssertEqual(len(peerList), len(ds.knownPeers), "peer list must contain all known peers")

	peers, err := json.Marshal(peerList)
	if err != nil {
		http.Error(w, "Peer list cannot be marshaled", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", mimeJson)
	w.Write(peers)
}

func (ds *DiscoveryServer) cleanupInactivePeers(cleanupInterval time.Duration) {
	assertions.Assert(cleanupInterval > 0, "cleanup interval must be positive")
	assertions.AssertNotNil(ds.knownPeers, "peers map cannot be nil")
	assertions.AssertNotNil(ds.done, "done channel cannot be nil")

	ticker := time.NewTicker(cleanupInterval)
	for {
		select {
		case <-ticker.C:
			ds.mu.Lock()
			initialCount := len(ds.knownPeers)
			removedCount := 0

			for addr, peer := range ds.knownPeers {
				assertions.AssertNotNil(peer, "peer cannot be nil")
				assertions.Assert(addr != "", "peer address cannot be empty")

				if time.Since(peer.LastSeen) > cleanupInterval {
					delete(ds.knownPeers, peer.Addr)
					removedCount++
					log.Printf("[Discovery Server] Inactive peer removed: %s", peer.Addr)
				}
			}

			assertions.AssertEqual(len(ds.knownPeers), initialCount-removedCount,
				"peer count must be reduced by the number of removed peers")

			ds.mu.Unlock()
		case <-ds.done:
			ticker.Stop()
			return
		}
	}
}
