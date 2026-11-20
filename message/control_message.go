package message

import (
	"dash/consts"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type ControlMessage struct {
	Operation Operation
	Topic     uint16
	Payload   []byte // Debater sobre
}

// ReadControlMessage reads one complete frame, including fragmented TCP reads.
func ReadControlMessage(reader io.Reader) (*ControlMessage, error) {
	prefix := make([]byte, consts.CONTROL_LENGTH_SIZE)
	if _, err := io.ReadFull(reader, prefix); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(prefix)
	if size < consts.CONTROL_HEADERS_SIZE || size > consts.CONTROL_HEADERS_SIZE+consts.MAX_CONTROL_PAYLOAD_SIZE {
		return nil, fmt.Errorf("tamanho de controle inválido: %d", size)
	}
	frame := make([]byte, len(prefix)+int(size))
	copy(frame, prefix)
	if _, err := io.ReadFull(reader, frame[len(prefix):]); err != nil {
		return nil, err
	}
	return DecodeControlMessage(frame)
}

func WriteControlMessage(writer io.Writer, msg *ControlMessage) error {
	data, err := msg.Encode()
	if err != nil {
		return err
	}
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func NewControlMessage(operation Operation, topic uint16, payload []byte) *ControlMessage {
	return &ControlMessage{
		Operation: operation,
		Topic:     topic,
		Payload:   payload,
	}
}

// [Tamanho: 4 bytes][Operação: 1 byte][Tópico: 2 bytes][Payload: até 256 bytes]
func (c ControlMessage) Encode() ([]byte, error) {
	if len(c.Payload) > consts.MAX_CONTROL_PAYLOAD_SIZE {
		return nil, fmt.Errorf("payload de controle excede %d bytes", consts.MAX_CONTROL_PAYLOAD_SIZE)
	}
	bodySize := consts.CONTROL_HEADERS_SIZE + len(c.Payload)
	headersEnd := consts.CONTROL_LENGTH_SIZE + consts.CONTROL_HEADERS_SIZE
	bytes := make([]byte, consts.CONTROL_LENGTH_SIZE+bodySize)

	binary.BigEndian.PutUint32(bytes[:consts.CONTROL_LENGTH_SIZE], uint32(bodySize))

	bytes[consts.CONTROL_LENGTH_SIZE] = byte(c.Operation)
	binary.BigEndian.PutUint16(bytes[consts.CONTROL_LENGTH_SIZE+1:headersEnd], c.Topic)

	copy(bytes[headersEnd:], c.Payload)

	return bytes, nil
}

func DecodeControlMessage(bytes []byte) (*ControlMessage, error) {

	headersEnd := consts.CONTROL_LENGTH_SIZE + consts.CONTROL_HEADERS_SIZE
	if len(bytes) < headersEnd {
		return nil, errors.New("mensagem de controle incompleta")
	}
	if len(bytes) > consts.MAX_CONTROL_MESSAGE_SIZE {
		return nil, fmt.Errorf("mensagem de controle excede %d bytes", consts.MAX_CONTROL_MESSAGE_SIZE)
	}

	bodySize := binary.BigEndian.Uint32(bytes[:consts.CONTROL_LENGTH_SIZE])
	if bodySize > consts.MAX_CONTROL_MESSAGE_SIZE-consts.CONTROL_LENGTH_SIZE {
		return nil, errors.New("tamanho declarado excede o limite da mensagem de controle")
	}

	if len(bytes)-consts.CONTROL_LENGTH_SIZE != int(bodySize) {
		return nil, errors.New("valor declarado e real de tamanho não são iguais")
	}

	operation, err := NewOperation(bytes[consts.CONTROL_LENGTH_SIZE])
	if err != nil {
		return nil, err
	}

	topic := binary.BigEndian.Uint16(bytes[consts.CONTROL_LENGTH_SIZE+1 : headersEnd])

	return NewControlMessage(operation, topic, bytes[headersEnd:]), nil
}
