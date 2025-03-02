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
	VersionSize = 4
	CounterSize = 8
	MessageSize = TypeSize + VersionSize + CounterSize
)

type Message struct {
	Type    byte
	Version uint32
	Counter uint64
}

func (m *Message) Encode() []byte {
	buf := make([]byte, MessageSize) // 1 byte type + 4 byte version + 8 byte counter
	buf[0] = m.Type
	binary.BigEndian.PutUint32(buf[1:5], m.Version)
	binary.BigEndian.PutUint64(buf[5:], m.Counter)

	assertions.AssertEqual(MessageSize, len(buf), "encoded message size must match expected size")
	return buf
}

func DecodeMessage(data []byte) (*Message, error) {
	if len(data) < MessageSize {
		return nil, fmt.Errorf("message too short: %d bytes", len(data))
	}

	return &Message{
		Type:    data[0],
		Version: binary.BigEndian.Uint32(data[1:5]),
		Counter: binary.BigEndian.Uint64(data[5:]),
	}, nil
}

type Transport interface {
	Send(addr string, data []byte) error
	Listen(handler func(addr string, data []byte) error) error
	Close() error
}
