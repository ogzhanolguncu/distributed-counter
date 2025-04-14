package protocol

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
	MessageTypeDigestAck  = 0x03 // Acknowledgment when digests match
	MessageFlagCompressed = 0x80

	CompressionThreshold = 1 << 10  // Only compress message larger than this (Bytes)
	DefaultBufferSize    = 1 << 12  // 4KB buffer
	MaxMessageSize       = 10 << 20 // 10MB max message size, adjust this if needed
)

var (
	ErrEmptyMessage     = errors.New("protocol: empty message data")
	ErrDecompression    = errors.New("protocol: failed to decompress data")
	ErrUnmarshall       = errors.New("protocol: failed to decode message")
	ErrInvalidType      = errors.New("protocol: invalid message type")
	ErrMessageTooLarge  = errors.New("protocol: message exceeds maximum size")
	ErrEmptyNodeID      = errors.New("protocol: empty node ID")
	ErrEmptyPNMaps      = errors.New("protocol: push message must contain increment or decrement values")
	ErrValidationFailed = errors.New("protocol: message validation failed")
	ErrEncodingFailed   = errors.New("protocol: message encoding failed")
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

// encode validates, marshals, checks size, and optionally compresses the message.
// Returns an error if validation fails, marshalling fails, or message exceeds MaxMessageSize.
func (m *Message) encode() ([]byte, error) { // <<< Changed signature: returns ([]byte, error)
	// Validate the message before encoding
	if err := m.validate(); err != nil {
		// Return validation error explicitly
		return nil, fmt.Errorf("%w: %v", ErrValidationFailed, err)
	}

	data, err := msgpack.Marshal(m)
	if err != nil {
		// Return marshalling error explicitly
		return nil, fmt.Errorf("%w: %v", ErrEncodingFailed, err)
	}

	// Check size *after* marshalling but *before* compression attempt
	if len(data) > MaxMessageSize {
		// Return specific error for oversized messages
		return nil, fmt.Errorf("%w: message size %d exceeds limit %d",
			ErrMessageTooLarge, len(data), MaxMessageSize)
	}

	// Attempt compression only if beneficial and above threshold
	if len(data) > CompressionThreshold {
		compressedData, errCompress := compressData(data)
		// Only use compressed data if compression succeeded AND it's actually smaller
		if errCompress == nil && len(compressedData) < len(data) {
			// Allocate result slice: 1 byte for header + compressed data length
			result := make([]byte, 1+len(compressedData))
			result[0] = MessageFlagCompressed // Set compression flag
			copy(result[1:], compressedData)
			return result, nil
		}
		// If compression failed or wasn't smaller, fall through to send uncompressed
	}

	// Handle uncompressed data (either below threshold or compression not beneficial)
	// Allocate result slice: 1 byte for header + uncompressed data length
	result := make([]byte, 1+len(data))
	result[0] = 0 // No compression flag (explicitly zero)
	copy(result[1:], data)
	return result, nil
}

// decode deserializes a byte slice into this Message
func (m *Message) decode(data []byte) error {
	assertions.AssertNotNil(data, "Data cannot be nil")
	// Check for empty message (at least 1 byte needed for header)
	if len(data) == 0 {
		return ErrEmptyMessage
	}

	// Check maximum message size (applied to the raw incoming data)
	if len(data) > MaxMessageSize {
		// Use existing ErrMessageTooLarge, potentially adding context
		return fmt.Errorf("%w: received message size %d exceeds limit %d",
			ErrMessageTooLarge, len(data), MaxMessageSize)
	}

	// Process message based on compression flag
	var payload []byte
	isCompressed := data[0] == MessageFlagCompressed
	if isCompressed {
		// Ensure there's data *after* the flag
		if len(data) < 2 {
			return fmt.Errorf("%w: compressed message has no payload", ErrEmptyMessage)
		}
		decompressed, err := decompressData(data[1:])
		if err != nil {
			// Wrap ErrDecompression for more context
			return fmt.Errorf("%w: %v", ErrDecompression, err)
		}
		payload = decompressed
	} else {
		// Ensure there's data *after* the flag (or it's just the flag byte)
		if len(data) < 2 {
			// Allow single byte '0x00' if that's a valid (empty) uncompressed message semantic,
			// otherwise return ErrEmptyMessage or similar if payload is always expected.
			// Assuming here payload IS expected after the 0x00 flag.
			return fmt.Errorf("%w: uncompressed message has no payload", ErrEmptyMessage)
		}
		payload = data[1:]
	}

	// Unmarshal the MessagePack data from the payload
	err := msgpack.Unmarshal(payload, m)
	if err != nil {
		// Wrap ErrUnmarshall
		return fmt.Errorf("%w: %v", ErrUnmarshall, err)
	}

	// Validate the logical content of the decoded message
	if err := m.validate(); err != nil {
		// Return validation error directly (e.g., ErrInvalidType, ErrEmptyNodeID)
		return err
	}

	return nil
}

// Decode is a helper function to decode data into a new Message struct
func Decode(data []byte) (*Message, error) {
	msg := &Message{}
	// Use the updated decode method which handles errors
	err := msg.decode(data)
	if err != nil {
		return nil, err // Propagate errors from decode
	}
	return msg, nil
}

// Encode is a helper function to encode a Message struct into bytes
func Encode(msg Message) ([]byte, error) {
	assertions.Assert(msg.Type != 0, "Message type cannot be zero")
	assertions.Assert(msg.NodeID != "", "Node ID cannot be empty")
	assertions.Assert(msg.Type == MessageTypePush || msg.Type == MessageTypeDigestAck || msg.Type == MessageTypeDigestPull,
		"Message type must be valid")
	if msg.Type == MessageTypePush {
		assertions.Assert(len(msg.IncrementValues) > 0 || len(msg.DecrementValues) > 0,
			"Push message must contain increment or decrement values")
	}

	// Call the updated encode method which now returns an error
	data, err := msg.encode()
	if err != nil {
		// Return the error from encode (e.g., validation, marshalling, too large)
		return nil, err
	}
	return data, nil
}

// --- COMPRESSION HELPERS ---
func compressData(data []byte) ([]byte, error) {
	assertions.AssertNotNil(data, "Input data cannot be nil")

	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd writer: %w", err)
	}
	compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)/2)) // Pre-allocate roughly
	errClose := encoder.Close()
	if errClose != nil {
		return nil, fmt.Errorf("failed to close zstd writer: %w", errClose)
	}

	return compressed, nil
}

func decompressData(data []byte) ([]byte, error) {
	assertions.AssertNotNil(data, "Compressed data cannot be nil")
	if len(data) == 0 {
		return []byte{}, nil // Decompressing empty results in empty
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer decoder.Close() // Ensure decoder resources are released

	// DecodeAll handles allocation. Let's estimate output size if possible, otherwise start empty.
	// Rough estimation: assume compression ratio ~3x if known, otherwise start small.
	output, err := decoder.DecodeAll(data, make([]byte, 0, len(data)*2)) // Pre-allocate estimate
	if err != nil {
		return nil, fmt.Errorf("failed to decode zstd data: %w", err)
	}
	return output, nil
}

type Transport interface {
	// Send sends data to the specified address
	Send(addr string, data []byte) error
	// Listen registers a handler for incoming messages
	Listen(handler func(addr string, data []byte) error) error
	// Close closes the transport
	Close() error
}
