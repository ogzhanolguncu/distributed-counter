package protocol

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTCPTransport_Basic(t *testing.T) {
	receiver := NewTCPTransport()
	receiver.addr = "127.0.0.1:0"

	received := make(chan []byte, 1)

	err := receiver.Listen(func(addr string, data []byte) error {
		received <- data
		return nil
	})
	require.NoError(t, err, "Failed to start receiver")

	actualAddr := receiver.listener.Addr().String()
	sender := NewTCPTransport()
	testData := []byte("hello world")

	err = sender.Send(actualAddr, testData)
	require.NoError(t, err, "Failed to send data")

	select {
	case receivedData := <-received:
		require.Equal(t, testData, receivedData, "Received data mismatch")
	case <-time.After(5 * time.Second):
		require.Fail(t, "Timeout waiting for data")
	}

	require.NoError(t, receiver.Close())
	require.NoError(t, sender.Close())
}

func TestTCPTransport_ConnectionRefused(t *testing.T) {
	sender := NewTCPTransport()

	err := sender.Send("127.0.0.1:44444", []byte("test"))
	require.Error(t, err, "Expected error when sending to non-existent port")

	require.NoError(t, sender.Close())
}

func TestTCPTransport_ConcurrentConnections(t *testing.T) {
	receiver := NewTCPTransport()
	receiver.addr = "127.0.0.1:0"

	receivedCount := 0
	var mu sync.Mutex

	err := receiver.Listen(func(addr string, data []byte) error {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		return nil
	})
	require.NoError(t, err, "Failed to start receiver")

	actualAddr := receiver.listener.Addr().String()
	const numMessages = 10
	var wg sync.WaitGroup

	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sender := NewTCPTransport()
			err := sender.Send(actualAddr, []byte("test"))
			require.NoError(t, err, "Failed to send")
			require.NoError(t, sender.Close())
		}()
	}

	wg.Wait()
	time.Sleep(time.Second)

	mu.Lock()
	require.Equal(t, numMessages, receivedCount, "Message count mismatch")
	mu.Unlock()

	require.NoError(t, receiver.Close())
}

func TestRetry(t *testing.T) {
	attempts := 0
	err := retry(context.Background(), 3, 100*time.Millisecond, func() error {
		attempts++
		if attempts < 2 {
			return fmt.Errorf("simulated error")
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}
