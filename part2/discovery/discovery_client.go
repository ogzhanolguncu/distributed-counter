package discovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part2/node"
)

type DiscoveryClient struct {
	srvAddr    string
	httpClient *http.Client
	done       chan struct{}
	node       *node.Node
}

func NewDiscoveryClient(srvAddr string, node *node.Node) *DiscoveryClient {
	return &DiscoveryClient{
		node:    node,
		srvAddr: srvAddr,
		httpClient: &http.Client{
			Timeout: httpTimeout,
		},
		done: make(chan struct{}),
	}
}

func (dc *DiscoveryClient) Start(discoveryInterval, heartbeatInterval time.Duration) error {
	if err := dc.Register(); err != nil {
		return fmt.Errorf("failed to register with discovery server: %w", err)
	}

	dc.StartDiscovery(discoveryInterval)
	dc.StartHeartbeat(heartbeatInterval)
	return nil
}

func (dc *DiscoveryClient) Stop() {
	close(dc.done)
	dc.httpClient.CloseIdleConnections()
}

func (dc *DiscoveryClient) StartDiscovery(discoveryInterval time.Duration) {
	discoveryTicker := time.NewTicker(discoveryInterval)

	go func() {
		for {
			select {
			case <-discoveryTicker.C:
				dc.discoverPeers()
			case <-dc.done:
				discoveryTicker.Stop()
				return
			}
		}
	}()
}

func (dc *DiscoveryClient) Register() error {
	var payload RegisterRequest
	payload.Addr = dc.node.GetAddr()

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
	ticker := time.NewTicker(heartbeatInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				var payload RegisterRequest
				payload.Addr = dc.node.GetAddr()

				jsonPayload, err := json.Marshal(payload)
				if err != nil {
					log.Printf("[Node %s] failed to marshal register payload",
						dc.node.GetAddr())
					continue
				}

				resp, err := dc.httpClient.Post(fmt.Sprintf("http://%s%s", dc.srvAddr, heartbeatEndpoint), mimeJson, bytes.NewBuffer(jsonPayload))
				if err != nil {
					log.Printf("[Node %s] failed to send heartbeat: %v",
						dc.node.GetAddr(), err)
					continue
				}

				func() {
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						log.Printf("[Node %s] received %s status from discovery server",
							dc.node.GetAddr(), resp.Status)
					} else {
						log.Printf("[Node %s] sent heartbeat to discovery server %s",
							dc.node.GetAddr(), dc.srvAddr)
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
	resp, err := dc.httpClient.Get(fmt.Sprintf("http://%s%s", dc.srvAddr, peersEndpoint))
	if err != nil {
		log.Printf("[Node %s] Failed to get peer list: %v", dc.node.GetAddr(), err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[Node %s] Discovery server returned status: %d",
			dc.node.GetAddr(), resp.StatusCode)
		return
	}

	var peerList []*PeerInfo
	if err := json.NewDecoder(resp.Body).Decode(&peerList); err != nil {
		log.Printf("[Node %s] Failed to decode peer list: %v",
			dc.node.GetAddr(), err)
		return
	}

	// We always receive fresh peers from discovery server, so we can wipe the old one and continue
	dc.node.GetPeerManager().ClearPeers()
	for _, peer := range peerList {
		// Discovery server also holds current node's addr so we have to exclude it
		if peer.Addr != dc.node.GetAddr() {
			dc.node.GetPeerManager().AddPeer(peer.Addr)
		}
	}

	activePeerCount := len(peerList)
	log.Printf("[Node %s] Updated peers from discovery server - active peers: %d",
		dc.node.GetAddr(), activePeerCount)
}
