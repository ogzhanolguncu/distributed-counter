package protocol

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type TCPTransport struct {
	addr     string
	listener net.Listener
	handler  func(string, []byte) error
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewTCPTransport() *TCPTransport {
	ctx, cancel := context.WithCancel(context.Background())
	return &TCPTransport{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (t *TCPTransport) Send(addr string, data []byte) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	return retry(t.ctx, 5, 500*time.Millisecond, func() error {
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		_, err = conn.Write(data)
		return err
	})
}

func (t *TCPTransport) Listen(handler func(string, []byte) error) error {
	listener, err := net.Listen("tcp", t.addr)
	if err != nil {
		return err
	}
	t.listener = listener
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
				go t.handleConn(conn)
			}
		}
	}()
	return nil
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	if t.handler != nil {
		t.handler(conn.RemoteAddr().String(), buf[:n])
	}
}

func (t *TCPTransport) Close() error {
	t.cancel()
	if t.listener != nil {
		t.listener.Close()
	}
	t.wg.Wait()
	return nil
}

func retry(ctx context.Context, attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; i < attempts; i++ {
		if i > 0 {
			log.Println("retrying after error:", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleep):
				sleep *= 2
			}
		}
		err = f()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}
