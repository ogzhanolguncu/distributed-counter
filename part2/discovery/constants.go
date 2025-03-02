package discovery

import "time"

const (
	registerEndpoint  = "/register"
	peersEndpoint     = "/peers"
	heartbeatEndpoint = "/heartbeat"

	mimeJson = "application/json"

	httpTimeout         = time.Second * 5
	httpSrvReadTimeout  = time.Second * 15
	httpSrvWriteTimeout = time.Second * 15
)
