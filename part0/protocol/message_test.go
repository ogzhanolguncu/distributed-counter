package protocol

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessages(t *testing.T) {
	messages := []Message{
		{Type: MessageTypePush, Version: 1, Counter: 12},
		{Type: MessageTypePull, Version: 2, Counter: 0},
		{Type: MessageTypePullAck, Version: 1, Counter: 65535},
		{Type: MessageTypePushAck, Version: 0, Counter: 1},
		{Type: MessageTypePush, Version: ^uint32(0), Counter: ^uint64(0)}, // Max values
	}

	for _, msg := range messages {
		t.Run(fmt.Sprintf("Type=0x%02x,Ver=%d,Counter=%d", msg.Type, msg.Version, msg.Counter), func(t *testing.T) {
			encoded := msg.Encode()
			decoded, err := DecodeMessage(encoded)

			require.NoError(t, err)
			require.Equal(t, msg, *decoded)
			require.Equal(t, 13, len(encoded))
		})
	}
}

func TestDecodeMessageErrors(t *testing.T) {
	invalidInputs := []struct {
		input   []byte
		wantErr string
	}{
		{[]byte{}, "message too short: 0 bytes"},
		{[]byte{0x02, 0x00, 0x00, 0x00}, "message too short: 4 bytes"},
	}

	for _, tc := range invalidInputs {
		t.Run(fmt.Sprintf("len=%d", len(tc.input)), func(t *testing.T) {
			_, err := DecodeMessage(tc.input)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
