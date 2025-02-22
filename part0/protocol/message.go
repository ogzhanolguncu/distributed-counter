package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	MessageTypePull = 0x01
	MessageTypePush = 0x02

	// Will later used to ack message for retries
	MessageTypePullAck = 0x03
	MessageTypePushAck = 0x04
)

type Message struct {
	Type    byte
	Version uint32
	Counter uint64
}

func (m *Message) Encode() []byte {
	buf := make([]byte, 13) // 1 byte type + 4 byte version + 8 byte counter
	buf[0] = m.Type
	binary.BigEndian.PutUint32(buf[1:5], m.Version)
	binary.BigEndian.PutUint64(buf[5:], m.Counter)
	return buf
}

func DecodeMessage(data []byte) (*Message, error) {
	if len(data) < 13 {
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
