package message

import (
	"dash/consts"
	"encoding/binary"
	"fmt"
)

// adicionar protobuf depois
type DataMessage struct {
	Operation Operation
	Topic     uint16
	ClientID  uint64
	Name      string
	Payload   string
}

func NewMessage(operation Operation, topic uint16, clientID uint64, name, payload string) *DataMessage {
	return &DataMessage{
		Operation: operation,
		Topic:     topic,
		ClientID:  clientID,
		Name:      name,
		Payload:   payload,
	}
}

func DecodeDataMessage(bytes []byte) (*DataMessage, error) {
	if len(bytes) < consts.HEADERS_SIZE {
		return nil, fmt.Errorf("mensagem de dados incompleta")
	}
	operation, err := NewOperation(bytes[0])
	if err != nil {
		return nil, err
	}

	topic := binary.BigEndian.Uint16(bytes[1:3])
	clientID := binary.BigEndian.Uint64(bytes[3:11])

	nameEnd := consts.HEADERS_SIZE + int(bytes[11])
	if len(bytes) < nameEnd {
		return nil, fmt.Errorf("nome do cliente incompleto na mensagem de dados")
	}
	return NewMessage(operation, topic, clientID, string(bytes[consts.HEADERS_SIZE:nameEnd]), string(bytes[nameEnd:])), nil
}

func (d DataMessage) Encode() []byte {
	nameSize := min(len(d.Name), consts.MAX_CLIENT_NAME_SIZE)
	payloadSize := min(len(d.Payload), consts.MAX_DATA_PAYLOAD_SIZE-nameSize)
	nameEnd := consts.HEADERS_SIZE + nameSize
	bytes := make([]byte, nameEnd+payloadSize)

	bytes[0] = byte(d.Operation)
	binary.BigEndian.PutUint16(bytes[1:3], d.Topic)
	binary.BigEndian.PutUint64(bytes[3:11], d.ClientID)
	bytes[11] = byte(nameSize)
	copy(bytes[consts.HEADERS_SIZE:nameEnd], d.Name)

	// capa no limite do slice bytes, já trunca
	copy(bytes[nameEnd:], d.Payload)

	return bytes
}
