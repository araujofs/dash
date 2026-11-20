package message

import (
	"bytes"
	"dash/consts"
	"encoding/binary"
	"testing"
)

func TestControlMessageSizeLimit(t *testing.T) {
	for _, size := range []int{0, 249, 255, 256, 257} {
		payload := bytes.Repeat([]byte{'a'}, size)
		encoded, err := NewControlMessage(IDENTIFY, 0, payload).Encode()
		if size == 257 {
			if err == nil || encoded != nil {
				t.Fatal("expected oversized payload to be rejected without truncation")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) != 7+size || binary.BigEndian.Uint32(encoded[:4]) != uint32(3+size) {
			t.Fatal("packet size or prefix does not match the control layout")
		}
		decoded, err := DecodeControlMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Operation != IDENTIFY || decoded.Topic != 0 || !bytes.Equal(decoded.Payload, payload) {
			t.Fatal("round trip changed the control message")
		}
	}
}

func TestDecodeControlMessageInvalidSize(t *testing.T) {
	for size := range 7 {
		if _, err := DecodeControlMessage(make([]byte, size)); err == nil {
			t.Fatalf("expected an error for %d bytes", size)
		}
	}
	oversized := make([]byte, 264)
	binary.BigEndian.PutUint32(oversized[:4], 260)
	oversized[4] = byte(IDENTIFY)
	declaredOversized := []byte{0, 0, 1, 4, byte(PING), 0, 0}
	for _, packet := range [][]byte{
		oversized,
		declaredOversized,
		{0, 0, 0, 4, byte(PING), 0, 0},
		{0, 0, 0, 2, byte(PING), 0, 0},
	} {
		if _, err := DecodeControlMessage(packet); err == nil {
			t.Fatal("expected an error for an invalid control frame size")
		}
	}
	if consts.MAX_CONTROL_MESSAGE_SIZE != 263 || consts.MAX_CONTROL_PAYLOAD_SIZE != 256 {
		t.Fatal("TCP limits do not match the protocol")
	}
}
