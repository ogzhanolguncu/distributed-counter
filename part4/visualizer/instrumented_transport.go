// part4/visualizer/instrumented_transport.go
package visualizer

import (
	"github.com/ogzhanolguncu/distributed-counter/part4/protocol"
)

// InstrumentedTransport wraps a Transport to record messages
type InstrumentedTransport struct {
	protocol.Transport
	NodeID string
	Vis    *GossipVisualizer
}

func NewInstrumentedTransport(transport protocol.Transport, nodeID string, vis *GossipVisualizer) *InstrumentedTransport {
	return &InstrumentedTransport{
		Transport: transport,
		NodeID:    nodeID,
		Vis:       vis,
	}
}

func (it *InstrumentedTransport) Send(addr string, data []byte) error {
	// Record the message before sending
	msg, err := protocol.Decode(data)
	if err == nil {
		content := map[string]interface{}{
			"type": msg.Type,
		}
		it.Vis.RecordMessage(it.NodeID, addr, protocol.MessageTypeToString(msg.Type), content)
	}

	// Call the original Send method
	return it.Transport.Send(addr, data)
}
