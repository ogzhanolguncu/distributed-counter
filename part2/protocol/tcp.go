package protocol

import (
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part1/assertions"
)

const (
	ReadBufferSize = 16 * 1024 // 16KB buffer for reading
	ReadTimeout    = 5 * time.Second
	WriteTimeout   = 5 * time.Second
	maxMessageSize = 10 * 1024 * 1024 // 10MB max message size
)

// Simple header for our messages
// [4 bytes message length][variable length address][1 byte separator][message payload]
// This allows us to properly frame messages without modifying their content

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

	// Establish connection with timeout
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		log.Printf("[TCP Transport] Connection error to %s: %v", addr, err)
		return err
	}
	defer conn.Close()

	// Set write deadline
	if err := conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
		log.Printf("[TCP Transport] Set deadline error: %v", err)
		return err
	}

	// Format: [1 byte address length][N bytes address][payload]
	// Where:
	// - First byte contains length of sender address (0-255)
	// - Next N bytes contain the address (where N equals value from first byte)
	// - Remaining bytes contain the unmodified message payload
	//
	// For example, if sending from "127.0.0.1:9000" with a 32-byte payload:
	// - Byte 0: Value 14 (length of "127.0.0.1:9000")
	// - Bytes 1-14: "127.0.0.1:9000"
	// - Bytes 15+: The 32-byte payload

	// First prepare our address
	addrBytes := []byte(t.addr)
	addrLen := len(addrBytes)

	// Length of the message is: 1 byte (addr length) + address bytes + payload bytes
	totalLen := 1 + addrLen + len(data)

	// Create a single buffer for the complete message to avoid partial writes
	message := make([]byte, totalLen)
	message[0] = byte(addrLen)            // First byte is address length
	copy(message[1:1+addrLen], addrBytes) // Next addrLen bytes are the address
	copy(message[1+addrLen:], data)       // Remaining bytes are the payload

	// Write the entire message
	written, err := conn.Write(message)
	if err != nil {
		log.Printf("[TCP Transport] Write error: %v", err)
		return err
	}

	if written != len(message) {
		log.Printf("[TCP Transport] Warning: Only sent %d of %d bytes to %s", written, len(message), addr)
	}

	return nil
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
				// Set accept deadline to make the listener responsive to context cancellation
				if err := t.listener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second)); err != nil {
					if t.ctx.Err() == nil {
						log.Printf("[TCP Transport] Failed to set accept deadline: %v", err)
					}
				}

				conn, err := t.listener.Accept()
				if err != nil {
					// Check if the error is due to deadline
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue // Just a timeout, try again
					}

					// Only continue if we're not shutting down
					if t.ctx.Err() == nil {
						log.Printf("[TCP Transport] Error accepting connection: %v", err)
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

	// Set read deadline
	if err := conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
		return
	}

	// Create a buffer to read all incoming data
	buf := make([]byte, ReadBufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		if err != io.EOF {
			log.Printf("[TCP Transport] Read error from %s: %v", conn.RemoteAddr(), err)
		}
		return
	}

	if n == 0 {
		return
	}

	// Process the message format:
	// [1 byte address length][N bytes address][payload]
	// Where:
	// - Byte 0: Contains length of sender address (0-255)
	// - Bytes 1 to 1+addrLen-1: Contains the address bytes (total of addrLen bytes)
	// - Bytes 1+addrLen onwards: Contains the message payload
	//
	// The "+1" in the indexing is to skip past the first byte (at index 0)
	// that stores the address length.

	if n < 2 { // Need at least 1 byte for address length + 1 byte of address
		log.Printf("[TCP Transport] Message too short from %s: %d bytes", conn.RemoteAddr(), n)
		return
	}

	addrLen := int(buf[0]) // Get address length from first byte
	if addrLen == 0 || addrLen > 255 || 1+addrLen >= n {
		log.Printf("[TCP Transport] Invalid address length from %s: %d", conn.RemoteAddr(), addrLen)
		return
	}

	senderAddr := string(buf[1 : 1+addrLen]) // Extract address from bytes 1 to 1+addrLen-1
	if len(senderAddr) == 0 {
		log.Printf("[TCP Transport] Empty sender address")
		return
	}

	// Extract payload (everything after the address)
	payload := buf[1+addrLen : n] // Starting at index 1+addrLen
	if len(payload) == 0 {
		return
	}

	// Store mapping from IP to sender address
	remoteIP := strings.Split(conn.RemoteAddr().String(), ":")[0]
	t.peerAddrs.Store(remoteIP, senderAddr)

	// Invoke the handler with the sender address and payload
	if t.handler != nil {
		if err := t.handler(senderAddr, payload); err != nil {
			log.Printf("[TCP Transport] Handler error for message from %s: %v", senderAddr, err)
		}
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
