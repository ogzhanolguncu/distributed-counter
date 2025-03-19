package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessages(t *testing.T) {
	tests := []struct {
		name    string
		message Message
	}{
		{
			name: "Basic message with increment values",
			message: Message{
				Type:            MessageTypePull,
				NodeID:          "node1",
				LogicalClock:    1,
				IncrementValues: map[string]uint64{"node1": 5, "node2": 3},
				DecrementValues: map[string]uint64{"node1": 2},
			},
		},
		{
			name: "Message with only decrements",
			message: Message{
				Type:            MessageTypePush,
				NodeID:          "node2",
				LogicalClock:    42,
				IncrementValues: map[string]uint64{},
				DecrementValues: map[string]uint64{"node1": 1, "node2": 2, "node3": 3},
			},
		},
		{
			name: "Message with max values",
			message: Message{
				Type:            MessageTypePull,
				NodeID:          "node3",
				LogicalClock:    18446744073709551615, // Max uint64
				IncrementValues: map[string]uint64{"node1": 18446744073709551615},
				DecrementValues: map[string]uint64{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.message.Encode()

			// We can't assert exact size since it depends on map contents
			assert.Greater(t, len(encoded), 1, "Encoded message should have at least 1 byte")

			decoded, err := DecodeMessage(encoded)
			assert.NoError(t, err)

			// Check each field matches
			assert.Equal(t, tt.message.Type, decoded.Type)
			assert.Equal(t, tt.message.NodeID, decoded.NodeID)
			assert.Equal(t, tt.message.LogicalClock, decoded.LogicalClock)
			assert.Equal(t, tt.message.IncrementValues, decoded.IncrementValues)
			assert.Equal(t, tt.message.DecrementValues, decoded.DecrementValues)
		})
	}
}

func TestDecodeMessageError(t *testing.T) {
	// Test empty message
	_, err := DecodeMessage([]byte{})
	assert.Error(t, err)

	// Test message too short
	_, err = DecodeMessage([]byte{0x01})
	assert.Error(t, err)

	// Test invalid compression flag
	_, err = DecodeMessage([]byte{MessageFlagCompressed, 0x01})
	assert.Error(t, err)
}

func TestCompression(t *testing.T) {
	// Create a message large enough to trigger compression
	largeMessage := Message{
		Type:         MessageTypePush,
		NodeID:       "node1",
		LogicalClock: 1,
		IncrementValues: func() map[string]uint64 {
			m := make(map[string]uint64)
			for i := range 100 {
				m[string(rune('a'+i%26))+string(rune('0'+i%10))] = uint64(i)
			}
			return m
		}(),
		DecrementValues: make(map[string]uint64),
	}

	encoded := largeMessage.Encode()
	// Check if compression flag is set (this message should be large enough)
	assert.Equal(t, uint8(0x80), encoded[0], "Large message should be compressed")

	// Decode and verify
	decoded, err := DecodeMessage(encoded)
	assert.NoError(t, err)
	assert.Equal(t, largeMessage.IncrementValues, decoded.IncrementValues)
}
