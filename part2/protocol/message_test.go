package protocol

import (
	"testing"

	"github.com/ogzhanolguncu/distributed-counter/part2/crdt"
	"github.com/stretchr/testify/require"
)

func TestMessageValidation(t *testing.T) {
	tests := []struct {
		name        string
		message     Message
		expectError bool
		errorType   error
	}{
		{
			name: "Valid push message",
			message: Message{
				Type:            MessageTypePush,
				NodeID:          "node1",
				IncrementValues: crdt.PNMap{"key1": 1},
			},
			expectError: false,
		},
		{
			name: "Invalid message type",
			message: Message{
				Type:   0xFF,
				NodeID: "node1",
			},
			expectError: true,
			errorType:   ErrInvalidType,
		},
		{
			name: "Empty node ID",
			message: Message{
				Type:   MessageTypePush,
				NodeID: "",
			},
			expectError: true,
			errorType:   ErrEmptyNodeID,
		},
		{
			name: "Push message with no values",
			message: Message{
				Type:            MessageTypePush,
				NodeID:          "node1",
				IncrementValues: make(crdt.PNMap),
				DecrementValues: make(crdt.PNMap),
			},
			errorType:   ErrEmptyPNMaps,
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.message.validate()

			if tc.expectError {
				require.Error(t, err)
				if tc.errorType != nil {
					require.ErrorIs(t, err, tc.errorType)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCriticalEdgeCases(t *testing.T) {
	t.Run("Empty data decoding", func(t *testing.T) {
		_, err := Decode([]byte{})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEmptyMessage)
	})

	t.Run("Invalid msgpack data", func(t *testing.T) {
		// Create invalid msgpack data with correct header
		invalidData := []byte{0x00, 0xFF, 0xFF, 0xFF} // First byte is compression flag (none)

		_, err := Decode(invalidData)
		require.Error(t, err)
		require.Contains(t, err.Error(), ErrUnmarshall.Error())
	})
}

func TestCompressionAndEncodeDecode(t *testing.T) {
	// Create a message that should exceed compression threshold
	largeMap := make(crdt.PNMap)
	for i := 0; i < 20; i++ {
		key := "key" + string(rune('A'+i))
		largeMap[key] = uint64(i * 100)
	}

	message := Message{
		Type:            MessageTypePush,
		NodeID:          "compression-test-node",
		IncrementValues: largeMap,
	}

	// Encode and check if compressed
	encoded, err := Encode(message)
	require.NoError(t, err)

	isCompressed := encoded[0] == MessageFlagCompressed
	t.Logf("Message compressed: %v", isCompressed)

	// Decode and verify content matches regardless of compression
	decoded, err := Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, message.Type, decoded.Type)
	require.Equal(t, message.NodeID, decoded.NodeID)
	require.Equal(t, message.IncrementValues, decoded.IncrementValues)
}
