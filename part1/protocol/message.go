package protocol

// TODO: xxHash or murmur3 for digestion based hashing
import (
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/ogzhanolguncu/distributed-counter/part1/assertions"
	"github.com/ogzhanolguncu/distributed-counter/part1/crdt"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	MessageTypePush       = 0x01
	MessageTypeDigestPull = 0x02 // Request with just a digest
	MessageTypeDigestAck  = 0x02 // Acknowledgment when digests match
	MessageFlagCompressed = 0x80

	CompressionThreshold = 1024 // Only compress message larger than this (Bytes)
	DefaultBufferSize    = 4096
	MaxMessageSize       = 10 * 1024 * 1024 // 10MB max message size
)

var (
	ErrEmptyMessage    = errors.New("protocol: empty message data")
	ErrDecompression   = errors.New("protocol: failed to decompress data")
	ErrUnmarshall      = errors.New("protocol: failed to decode message")
	ErrInvalidType     = errors.New("protocol: invalid message type")
	ErrMessageTooLarge = errors.New("protocol: message exceeds maximum size")
	ErrEmptyNodeID     = errors.New("protocol: empty node ID")
	ErrEmptyPNMaps     = errors.New("protocol: push message must contain increment or decrement values")
)

// Message represents a protocol message for PNCounter CRDT
type Message struct {
	Type            uint8      `msgpack:"t"`
	NodeID          string     `msgpack:"id"`
	IncrementValues crdt.PNMap `msgpack:"inc"`
	DecrementValues crdt.PNMap `msgpack:"dec"`
	Digest          uint64     `msgpack:"dig,omitempty"` // xxHash digest of state
}

// Validate checks if the message is valid
func (m *Message) validate() error {
	assertions.Assert(m != nil, "Message cannot be nil")
	// Check message type is valid
	if m.Type != MessageTypePush && m.Type != MessageTypeDigestAck && m.Type != MessageTypeDigestPull {
		return ErrInvalidType
	}

	// Node ID must not be empty
	if m.NodeID == "" {
		return ErrEmptyNodeID
	}

	// For push messages, either increment or decrement values should be present
	if m.Type == MessageTypePush && len(m.IncrementValues) == 0 && len(m.DecrementValues) == 0 {
		return ErrEmptyPNMaps
	}

	return nil
}

func (m *Message) encode() []byte {
	// Validate the message before encoding
	if err := m.validate(); err != nil {
		return []byte{} // Return empty slice on validation error
	}

	data, err := msgpack.Marshal(m)
	if err != nil {
		return []byte{} // Return empty slice on error
	}

	if len(data) > MaxMessageSize {
		return []byte{} // Message too large
	}

	if len(data) > CompressionThreshold {
		compressedData, err := compressData(data)
		if err == nil && len(compressedData) < len(data) {
			result := make([]byte, len(compressedData)+1) // +1 byte for compression header
			result[0] = MessageFlagCompressed
			copy(result[1:], compressedData)
			return result
		}
	}
	result := make([]byte, len(data)+1) // +1 byte for compression header
	result[0] = 0                       // No compression header
	copy(result[1:], data)
	return result
}

// DecodeMessage deserializes a byte slice into a Message
// decode deserializes a byte slice into this Message
func (m *Message) decode(data []byte) error {
	assertions.AssertNotNil(data, "Data cannot be nil")
	// Check for empty message
	if len(data) == 0 {
		return ErrEmptyMessage
	}

	// Check maximum message size
	if len(data) > MaxMessageSize {
		return ErrMessageTooLarge
	}

	// Process message based on compression flag
	var uncompressedData []byte
	isCompressed := data[0] == MessageFlagCompressed
	if isCompressed {
		decompressed, err := decompressData(data[1:])
		if err != nil {
			return fmt.Errorf("%w: %v", ErrDecompression, err)
		}
		uncompressedData = decompressed
	} else {
		uncompressedData = data[1:]
	}

	// Unmarshal the MessagePack data
	err := msgpack.Unmarshal(uncompressedData, m)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnmarshall, err)
	}

	// Validate the decoded message
	if err := m.validate(); err != nil {
		return err
	}

	return nil
}

func Decode(data []byte) (*Message, error) {
	msg := &Message{}
	err := msg.decode(data)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func Encode(msg Message) ([]byte, error) {
	assertions.Assert(msg.Type != 0, "Message type cannot be zero")
	assertions.Assert(msg.NodeID != "", "Node ID cannot be empty")

	// Validate message type
	assertions.Assert(msg.Type == MessageTypePush || msg.Type == MessageTypeDigestAck || msg.Type == MessageTypeDigestPull,
		"Message type must be either MessageTypePull or MessageTypePush")

	// For push messages, require values
	if msg.Type == MessageTypePush {
		assertions.Assert(len(msg.IncrementValues) > 0 || len(msg.DecrementValues) > 0,
			"Push message must contain increment or decrement values")
	}

	data := msg.encode()
	assertions.Assert(len(data) > 0, "Encoded message cannot be empty")

	return data, nil
}

// COMPRESSION HELPERS
func compressData(data []byte) ([]byte, error) {
	assertions.AssertNotNil(data, "Input data cannot be nil")
	assertions.Assert(len(data) > 0, "Input data cannot be empty")

	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	compressed := encoder.EncodeAll(data, make([]byte, 0))
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return compressed, nil
}

func decompressData(data []byte) ([]byte, error) {
	assertions.AssertNotNil(data, "Compressed data cannot be nil")
	assertions.Assert(len(data) > 0, "Compressed data cannot be empty")

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	output, err := decoder.DecodeAll(data, make([]byte, 0))
	if err != nil {
		return nil, err
	}
	return output, nil
}

// TRANSPORT
// Transport defines the interface for network communication
type Transport interface {
	// Send sends data to the specified address
	Send(addr string, data []byte) error
	// Listen registers a handler for incoming messages
	Listen(handler func(addr string, data []byte) error) error
	// Close closes the transport
	Close() error
}

// Initialize MessagePack decoder for common interfaces
func init() {
	msgpack.GetDecoder().SetCustomStructTag("msgpack")
}
