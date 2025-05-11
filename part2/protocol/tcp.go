package protocol

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ogzhanolguncu/distributed-counter/part2/assertions"
)

const (
	ReadBufferSize = 4 << 12 // 16KB buffer for reading
	ReadTimeout    = 5 * time.Second
	WriteTimeout   = 5 * time.Second
	DialTimeout    = 5 * time.Second
)

type TCPTransport struct {
	addr     string
	listener net.Listener
	handler  func(string, []byte) error
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	logger   *slog.Logger
}

func NewTCPTransport(addr string, logger *slog.Logger) (*TCPTransport, error) {
	assertions.Assert(addr != "", "transport address cannot be empty")

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}

	transportLogger := logger.With("component", "TCPTransport", "addr", addr)

	transport := &TCPTransport{
		addr:     addr,
		listener: listener,
		ctx:      ctx,
		cancel:   cancel,
		logger:   transportLogger,
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
	conn, err := net.DialTimeout("tcp", addr, DialTimeout)
	if err != nil {
		t.logger.Error("connection error", "target", addr, "error", err)
		return err
	}
	defer conn.Close()

	// Set write deadline
	if err := conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
		t.logger.Error("set deadline error", "error", err)
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
		t.logger.Error("write error", "target", addr, "error", err)
		return err
	}

	if written != len(message) {
		t.logger.Warn("partial write", "written", written, "total", len(message), "target", addr)
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
				// Set deadline to make the listener responsive to cancellation
				deadline := time.Now().Add(1 * time.Second)
				if err := t.listener.(*net.TCPListener).SetDeadline(deadline); err != nil && t.ctx.Err() == nil {
					t.logger.Error("failed to set accept deadline", "error", err)
				}

				conn, err := t.listener.Accept()
				if err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue // Just a timeout, try again
					}

					if t.ctx.Err() == nil {
						t.logger.Error("error accepting connection", "error", err)
					}
					continue
				}

				// We still need the concurrent connection handling
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
		t.logger.Error("failed to set read deadline", "remote_addr", conn.RemoteAddr(), "error", err)
		return
	}

	// Create a buffer to read all incoming data
	buf := make([]byte, ReadBufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		if err != io.EOF {
			t.logger.Error("read error", "remote_addr", conn.RemoteAddr(), "error", err)
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
		t.logger.Error("message too short", "remote_addr", conn.RemoteAddr(), "bytes", n)
		return
	}

	addrLen := int(buf[0]) // Get address length from first byte
	if addrLen == 0 || addrLen > 255 || 1+addrLen >= n {
		t.logger.Error("invalid address length", "remote_addr", conn.RemoteAddr(), "addr_len", addrLen)
		return
	}

	senderAddr := string(buf[1 : 1+addrLen]) // Extract address from bytes 1 to 1+addrLen-1
	if len(senderAddr) == 0 {
		t.logger.Error("empty sender address")
		return
	}

	// Extract payload (everything after the address)
	payload := buf[1+addrLen : n] // Starting at index 1+addrLen
	if len(payload) == 0 {
		return
	}

	// Invoke the handler with the sender address and payload
	if t.handler != nil {
		if err := t.handler(senderAddr, payload); err != nil {
			t.logger.Error("handler error", "sender", senderAddr, "error", err)
		}
	}
}

func (t *TCPTransport) Close() error {
	assertions.AssertNotNil(t.cancel, "cancel function cannot be nil")
	assertions.AssertNotNil(t.ctx, "context cannot be nil")

	t.logger.Info("closing transport")
	t.cancel()
	if t.listener != nil {
		t.listener.Close()
	}
	t.wg.Wait()

	return nil
}
