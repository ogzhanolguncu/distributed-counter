package discovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part3/assertions"
	"github.com/ogzhanolguncu/distributed-counter/part3/node"
)

type DiscoveryClient struct {
	srvAddr    string
	httpClient *http.Client
	done       chan struct{}
	node       *node.Node
}

func NewDiscoveryClient(srvAddr string, node *node.Node) *DiscoveryClient {
	assertions.Assert(srvAddr != "", "discovery server address cannot be empty")
	assertions.AssertNotNil(node, "node cannot be nil")

	client := &DiscoveryClient{
		node:    node,
		srvAddr: srvAddr,
		httpClient: &http.Client{
			Timeout: httpTimeout,
		},
		done: make(chan struct{}),
	}

	assertions.AssertNotNil(client.httpClient, "HTTP client must be initialized")
	assertions.AssertNotNil(client.done, "done channel must be initialized")

	return client
}

func (dc *DiscoveryClient) Start(heartbeatInterval time.Duration) error {
	assertions.Assert(heartbeatInterval > 0, "heartbeat interval must be positive")
	assertions.AssertNotNil(dc.node, "node cannot be nil")

	if err := dc.Register(); err != nil {
		return fmt.Errorf("failed to register with discovery server: %w", err)
	}

	dc.discoverPeers()
	dc.StartHeartbeat(heartbeatInterval)
	return nil
}

func (dc *DiscoveryClient) Stop() {
	assertions.AssertNotNil(dc.done, "done channel cannot be nil")
	assertions.AssertNotNil(dc.httpClient, "HTTP client cannot be nil")

	close(dc.done)
	dc.httpClient.CloseIdleConnections()
}

func (dc *DiscoveryClient) Register() error {
	assertions.AssertNotNil(dc.node, "node cannot be nil")
	assertions.AssertNotNil(dc.httpClient, "HTTP client cannot be nil")
	assertions.Assert(dc.srvAddr != "", "discovery server address cannot be empty")

	nodeAddr := dc.node.GetAddr()
	assertions.Assert(nodeAddr != "", "node address cannot be empty")

	var payload RegisterRequest
	payload.Addr = nodeAddr

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal register request: %w", err)
	}

	resp, err := dc.httpClient.Post(fmt.Sprintf("http://%s%s", dc.srvAddr, registerEndpoint), mimeJson, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Errorf("failed to register with discovery server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("discovery server returned status %d for 'Register' ", resp.StatusCode)
	}

	log.Printf("[Node %s] Registered with discovery server %s (counter=%d, version=%d)",
		dc.node.GetAddr(), dc.srvAddr, dc.node.GetCounter(), dc.node.GetVersion())
	return nil
}

func (dc *DiscoveryClient) StartHeartbeat(heartbeatInterval time.Duration) {
	assertions.Assert(heartbeatInterval > 0, "heartbeat interval must be positive")
	assertions.AssertNotNil(dc.node, "node cannot be nil")
	assertions.AssertNotNil(dc.httpClient, "HTTP client cannot be nil")
	assertions.AssertNotNil(dc.done, "done channel cannot be nil")

	ticker := time.NewTicker(heartbeatInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				var payload RegisterRequest
				nodeAddr := dc.node.GetAddr()
				assertions.Assert(nodeAddr != "", "node address cannot be empty for heartbeat")

				payload.Addr = nodeAddr

				jsonPayload, err := json.Marshal(payload)
				if err != nil {
					log.Printf("[Node %s] failed to marshal register payload",
						nodeAddr)
					continue
				}

				resp, err := dc.httpClient.Post(fmt.Sprintf("http://%s%s", dc.srvAddr, heartbeatEndpoint), mimeJson, bytes.NewBuffer(jsonPayload))
				if err != nil {
					log.Printf("[Node %s] failed to send heartbeat: %v",
						nodeAddr, err)
					continue
				}

				func() {
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						log.Printf("[Node %s] received %s status from discovery server",
							nodeAddr, resp.Status)
					} else {
						log.Printf("[Node %s] sent heartbeat to discovery server %s",
							nodeAddr, dc.srvAddr)
					}
				}()
			case <-dc.done:
				ticker.Stop()
				return
			}
		}
	}()
}

func (dc *DiscoveryClient) discoverPeers() {
	assertions.AssertNotNil(dc.node, "node cannot be nil")
	assertions.AssertNotNil(dc.httpClient, "HTTP client cannot be nil")
	assertions.Assert(dc.srvAddr != "", "discovery server address cannot be empty")

	nodeAddr := dc.node.GetAddr()
	assertions.Assert(nodeAddr != "", "node address cannot be empty")

	resp, err := dc.httpClient.Get(fmt.Sprintf("http://%s%s", dc.srvAddr, peersEndpoint))
	if err != nil {
		log.Printf("[Node %s] Failed to get peer list: %v", nodeAddr, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[Node %s] Discovery server returned status: %d",
			nodeAddr, resp.StatusCode)
		return
	}

	var peerList []*PeerInfo
	if err := json.NewDecoder(resp.Body).Decode(&peerList); err != nil {
		log.Printf("[Node %s] Failed to decode peer list: %v",
			nodeAddr, err)
		return
	}

	// Validate peer list
	for _, peer := range peerList {
		assertions.AssertNotNil(peer, "peer info cannot be nil")
		assertions.Assert(peer.Addr != "", "peer address cannot be empty")
	}

	peerManager := dc.node.GetPeerManager()
	assertions.AssertNotNil(peerManager, "peer manager cannot be nil")

	// We always receive fresh peers from discovery server, so we can wipe the old one and continue
	peerManager.ClearPeers()
	for _, peer := range peerList {
		// Discovery server also holds current node's addr so we have to exclude it
		if peer.Addr != nodeAddr {
			peerManager.AddPeer(peer.Addr)
		}
	}

	activePeerCount := len(peerList)
	log.Printf("[Node %s] Updated peers from discovery server - active peers: %d",
		nodeAddr, activePeerCount)
}
