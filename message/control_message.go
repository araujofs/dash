package message

import (
	"encoding/binary"
	// "errors"
	// "dash/consts"
)

// Ainda falta implementar o tamanho max para payload
type ControlMessage struct{
	Operation Operation
	Topic uint16
	Payload []byte // Debater sobre
}


func NewControlMessage(operation Operation, topic uint16, payload []byte) *ControlMessage{
	return &ControlMessage{
		Operation: operation,
		Topic: topic,
		Payload: payload,
	}
}


// [Tamanho: 4 bytes][Operação: 1 byte][Tópico: 2 bytes][Payload: possivelmente 1017]
func (c ControlMessage) Encode() []byte {
	bodySize := 3 + len(c.Payload)
	bytes := make([]byte, 4 + bodySize)

	binary.BigEndian.PutUint32(bytes[0:4], uint32(bodySize))

	bytes[4] = byte(c.Operation)
	binary.BigEndian.PutUint16(bytes[5:7], c.Topic)

	copy(bytes[7:], c.Payload)

	return bytes
}


func DecodeControlMessage(bytes []byte) (*ControlMessage, error) {

	// if len(bytes) < consts.HEADERS_SIZE{
	// 	return nil, errors.New("")
	// }

	operation, err := NewOperation(bytes[0])
	if err != nil {
		return nil, err
	}
	
	topic := binary.BigEndian.Uint16(bytes[1:3])
	payload := bytes[3:]

	return NewControlMessage(operation, topic, payload), nil
}