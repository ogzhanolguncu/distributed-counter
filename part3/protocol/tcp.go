package protocol

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part3/assertions"
)

type TCPTransport struct {
	addr     string // This node's listening address
	listener net.Listener
	handler  func(string, []byte) error
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	// Map to keep track of actual peer addresses
	peerAddrs sync.Map // Maps IP -> full listening address
}

func NewTCPTransport(addr string) (*TCPTransport, error) {
	assertions.Assert(addr != "", "transport address cannot be empty")

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	transport := &TCPTransport{
		addr:     addr,
		listener: listener,
		ctx:      ctx,
		cancel:   cancel,
	}

	assertions.AssertNotNil(transport.listener, "listener must be initialized")
	assertions.AssertNotNil(transport.ctx, "context must be initialized")
	assertions.AssertNotNil(transport.cancel, "cancel function must be initialized")

	return transport, nil
}

func (t *TCPTransport) Send(addr string, data []byte) error {
	assertions.Assert(addr != "", "target address cannot be empty")
	assertions.AssertNotNil(data, "data cannot be nil")
	assertions.Assert(len(data) > 0, "data cannot be empty")
	assertions.Assert(t.addr != "", "transport's own address cannot be empty")
	assertions.Assert(t.addr != addr, "transport cannot send data to itself")

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send our address as a prefix (format: "ADDR:127.0.0.1:9000|")
	prefix := "ADDR:" + t.addr + "|"
	prefixBytes := []byte(prefix)

	// First send the prefix
	_, err = conn.Write(prefixBytes)
	if err != nil {
		return err
	}

	// Then send the actual data
	_, err = conn.Write(data)
	return err
}

func (t *TCPTransport) Listen(handler func(string, []byte) error) error {
	assertions.AssertNotNil(handler, "handler function cannot be nil")
	assertions.AssertNotNil(t.listener, "listener cannot be nil")
	assertions.AssertNotNil(t.ctx, "context cannot be nil")

	t.handler = handler
	t.wg.Add(1)

	go func() {
		defer t.wg.Done()
		for {
			select {
			case <-t.ctx.Done():
				return
			default:
				conn, err := t.listener.Accept()
				if err != nil {
					// Only continue if we're not shutting down
					if t.ctx.Err() == nil {
						continue
					}
					return
				}

				assertions.AssertNotNil(conn, "accepted connection cannot be nil")
				go t.handleConn(conn)
			}
		}
	}()

	return nil
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	assertions.AssertNotNil(conn, "connection cannot be nil")

	defer conn.Close()
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	assertions.Assert(n > 0, "must read at least one byte")

	// Extract sender address from message prefix
	data := buf[:n]
	message := string(data)

	// Look for sender address prefix
	var senderAddr string
	remoteAddr := conn.RemoteAddr().String()
	assertions.Assert(remoteAddr != "", "remote address cannot be empty")

	remoteIP := strings.Split(remoteAddr, ":")[0]
	assertions.Assert(remoteIP != "", "remote IP cannot be empty")

	if strings.HasPrefix(message, "ADDR:") {
		// Find the end of the address
		endIndex := strings.Index(message, "|")
		if endIndex > 5 { // "ADDR:" is 5 chars
			senderAddr = message[5:endIndex]
			assertions.Assert(senderAddr != "", "extracted sender address cannot be empty")
			// Store the mapping from IP to listening address
			t.peerAddrs.Store(remoteIP, senderAddr)
			// Remove the prefix from the data
			data = data[endIndex+1:]
		}
	}

	// If we couldn't extract address, try to look it up
	if senderAddr == "" {
		if addr, ok := t.peerAddrs.Load(remoteIP); ok {
			senderAddr = addr.(string)
		} else {
			// Fall back to connection address if we have nothing better
			senderAddr = remoteAddr
		}
	}

	assertions.Assert(senderAddr != "", "sender address cannot be empty")

	if t.handler != nil {
		t.handler(senderAddr, data)
	}
}

func (t *TCPTransport) Close() error {
	assertions.AssertNotNil(t.cancel, "cancel function cannot be nil")
	assertions.AssertNotNil(t.ctx, "context cannot be nil")

	t.cancel()
	if t.listener != nil {
		t.listener.Close()
	}
	t.wg.Wait()
	return nil
}
