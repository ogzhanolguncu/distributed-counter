package protocol

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

// Message types and flags
const (
	MessageTypePush       = 0x01
	MessageTypePull       = 0x02
	MessageFlagCompressed = 0x80 // Flag to indicate compression
)

// Thresholds and sizing constants
const (
	CompressionThreshold = 100  // Only compress messages larger than this (bytes)
	DefaultBufferSize    = 4096 // Default buffer size for message handling
)

// Message represents a protocol message for PNCounter CRDT
type Message struct {
	Type            uint8             `msgpack:"t"`
	NodeID          string            `msgpack:"id"`
	LogicalClock    uint64            `msgpack:"lc"` // Lamport logical clock for causality tracking
	IncrementValues map[string]uint64 `msgpack:"inc"`
	DecrementValues map[string]uint64 `msgpack:"dec"`
}

// Encode serializes a message into a byte slice using MessagePack and compression
func (m *Message) Encode() []byte {
	// Serialize with MessagePack
	data, err := msgpack.Marshal(m)
	if err != nil {
		return []byte{} // Return empty slice on error
	}

	// Only compress if message is large enough to benefit
	if len(data) > CompressionThreshold {
		compressedData, err := compressData(data)
		if err == nil && len(compressedData) < len(data) {
			// Compression was beneficial
			result := make([]byte, len(compressedData)+1)
			result[0] = MessageFlagCompressed
			copy(result[1:], compressedData)
			return result
		}
	}

	// Use uncompressed data
	result := make([]byte, len(data)+1)
	result[0] = 0 // No compression flag
	copy(result[1:], data)
	return result
}

// compressData compresses byte data using gzip
func compressData(data []byte) ([]byte, error) {
	var compressedBuf bytes.Buffer
	compressor := gzip.NewWriter(&compressedBuf)

	_, err := compressor.Write(data)
	if err != nil {
		return nil, err
	}

	err = compressor.Close()
	if err != nil {
		return nil, err
	}

	return compressedBuf.Bytes(), nil
}

// DecodeMessage deserializes a byte slice into a Message
func DecodeMessage(data []byte) (*Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty message data")
	}

	// Process message based on compression flag
	var uncompressedData []byte
	isCompressed := data[0] == MessageFlagCompressed

	if isCompressed {
		decompressed, err := decompressData(data[1:])
		if err != nil {
			return nil, fmt.Errorf("failed to decompress data: %w", err)
		}
		uncompressedData = decompressed
	} else {
		uncompressedData = data[1:]
	}

	// Unmarshal the MessagePack data
	var msg Message
	err := msgpack.Unmarshal(uncompressedData, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed to decode message: %w", err)
	}

	return &msg, nil
}

// decompressData decompresses gzipped data
func decompressData(data []byte) ([]byte, error) {
	gzipReader, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, gzipReader)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

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
