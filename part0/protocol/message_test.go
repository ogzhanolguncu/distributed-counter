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
			name: "Type=0x02,Ver=1,Counter=12",
			message: Message{
				Type:    0x02,
				Version: 1,
				Counter: 12,
			},
		},
		{
			name: "Type=0x01,Ver=2,Counter=0",
			message: Message{
				Type:    0x01,
				Version: 2,
				Counter: 0,
			},
		},
		{
			name: "Type=0x02,Ver=18446744073709551615,Counter=18446744073709551615",
			message: Message{
				Type:    0x02,
				Version: 18446744073709551615,
				Counter: 18446744073709551615,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.message.Encode()
			// We expect: 1 byte (type) + 8 bytes (version) + 8 bytes (counter) = 17 bytes
			assert.Equal(t, MessageSize, len(encoded))

			decoded, err := DecodeMessage(encoded)
			assert.NoError(t, err)
			assert.Equal(t, tt.message.Type, decoded.Type)
			assert.Equal(t, tt.message.Version, decoded.Version)
			assert.Equal(t, tt.message.Counter, decoded.Counter)
		})
	}
}

func TestDecodeMessageError(t *testing.T) {
	_, err := DecodeMessage([]byte{0x01})
	assert.Error(t, err)
}
