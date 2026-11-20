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
	Name      string // Only carried by FORWARDING; populated from the server's client registry.
	Payload   string
}

func NewMessage(operation Operation, topic uint16, clientID uint64, payload string) *DataMessage {
	return &DataMessage{
		Operation: operation,
		Topic:     topic,
		ClientID:  clientID,
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

	payloadStart := consts.HEADERS_SIZE
	name := ""
	if operation == FORWARDING {
		if len(bytes) < consts.FORWARDING_HEADERS_SIZE {
			return nil, fmt.Errorf("cabeçalho de encaminhamento incompleto")
		}
		payloadStart = consts.FORWARDING_HEADERS_SIZE + int(bytes[consts.HEADERS_SIZE])
		if len(bytes) < payloadStart {
			return nil, fmt.Errorf("nome do cliente incompleto na mensagem de encaminhamento")
		}
		name = string(bytes[consts.FORWARDING_HEADERS_SIZE:payloadStart])
	}
	payloadEnd := min(len(bytes), payloadStart+consts.MAX_DATA_PAYLOAD_SIZE)
	msg := NewMessage(operation, topic, clientID, string(bytes[payloadStart:payloadEnd]))
	msg.Name = name
	return msg, nil
}

func (d DataMessage) Encode() []byte {
	headersSize := consts.HEADERS_SIZE
	nameSize := 0
	if d.Operation == FORWARDING {
		nameSize = min(len(d.Name), consts.MAX_CLIENT_NAME_SIZE)
		headersSize = consts.FORWARDING_HEADERS_SIZE + nameSize
	}
	payloadSize := min(len(d.Payload), consts.MAX_DATA_PAYLOAD_SIZE)
	bytes := make([]byte, headersSize+payloadSize)

	bytes[0] = byte(d.Operation)
	binary.BigEndian.PutUint16(bytes[1:3], d.Topic)
	binary.BigEndian.PutUint64(bytes[3:11], d.ClientID)
	if d.Operation == FORWARDING {
		bytes[consts.HEADERS_SIZE] = byte(nameSize)
		copy(bytes[consts.FORWARDING_HEADERS_SIZE:headersSize], d.Name)
	}

	// capa no limite do slice bytes, já trunca
	copy(bytes[headersSize:], d.Payload)

	return bytes
}
