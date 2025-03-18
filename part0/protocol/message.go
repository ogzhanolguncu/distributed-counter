package protocol

import (
	"encoding/binary"
	"fmt"

	"github.com/ogzhanolguncu/distributed-counter/part0/assertions"
)

const (
	MessageTypePull = 0x01
	MessageTypePush = 0x02
)

const (
	TypeSize    = 1
	VersionSize = 8
	CounterSize = 8
	MessageSize = TypeSize + VersionSize + CounterSize
)

type Message struct {
	Type    byte
	Version uint64 // Using uint64 for Version (8 bytes)
	Counter uint64
}

func (m *Message) Encode() []byte {
	buf := make([]byte, MessageSize) // 1 byte type + 8 byte version + 8 byte counter
	buf[0] = m.Type
	binary.BigEndian.PutUint64(buf[1:9], m.Version)
	binary.BigEndian.PutUint64(buf[9:], m.Counter)
	assertions.AssertEqual(MessageSize, len(buf), "encoded message size must match expected size")
	return buf
}

func DecodeMessage(data []byte) (*Message, error) {
	if len(data) < MessageSize {
		return nil, fmt.Errorf("message too short: %d bytes", len(data))
	}
	return &Message{
		Type:    data[0],
		Version: binary.BigEndian.Uint64(data[1:9]),
		Counter: binary.BigEndian.Uint64(data[9:]),
	}, nil
}

type Transport interface {
	Send(addr string, data []byte) error
	Listen(handler func(addr string, data []byte) error) error
	Close() error
}
