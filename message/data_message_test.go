package message

import (
	"bytes"
	"dash/consts"
	"strings"
	"testing"
)

func TestDataMessageRoundTrip(t *testing.T) {
	for _, operation := range []Operation{SEND, FORWARDING} {
		for _, clientID := range []uint64{0, 1, ^uint64(0)} {
			for _, name := range []string{"", "alice", "João", strings.Repeat("n", consts.MAX_CLIENT_NAME_SIZE)} {
				for _, payload := range []string{"", "hello", "\x03bob\x00data", strings.Repeat("a", 2048), strings.Repeat("a", 2049)} {
					original := NewMessage(operation, 42, clientID, payload)
					original.Name = name
					encoded := original.Encode()
					decoded, err := DecodeDataMessage(encoded)
					if err != nil {
						t.Fatal(err)
					}
					headersSize, wantName := 11, ""
					if operation == FORWARDING {
						headersSize, wantName = 12+len(name), name
					}
					want := payload[:min(len(payload), 2048)]
					if decoded.Operation != operation || decoded.Topic != 42 || decoded.ClientID != clientID || decoded.Name != wantName || decoded.Payload != want {
						t.Fatalf("unexpected decoded message: %+v", decoded)
					}
					if len(encoded) != headersSize+len(want) {
						t.Fatal("encoded packet length does not match headers and payload")
					}
				}
			}
		}
	}
}

func TestDecodeDataMessageTruncatesOversizedPayload(t *testing.T) {
	for _, operation := range []Operation{SEND, FORWARDING} {
		msg := NewMessage(operation, 42, 1, strings.Repeat("x", 2048))
		msg.Name = strings.Repeat("n", 255)
		packet := append(msg.Encode(), []byte("excess")...)
		decoded, err := DecodeDataMessage(packet)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Payload != msg.Payload {
			t.Fatal("decoder did not preserve exactly 2048 payload bytes")
		}
	}
}

func TestDataMessageHeaderLayout(t *testing.T) {
	for _, tt := range []struct {
		operation Operation
		want      []byte
	}{
		{SEND, []byte{byte(SEND), 0x12, 0x34, 1, 2, 3, 4, 5, 6, 7, 8, 'h', 'i'}},
		{FORWARDING, []byte{byte(FORWARDING), 0x12, 0x34, 1, 2, 3, 4, 5, 6, 7, 8, 3, 'b', 'o', 'b', 'h', 'i'}},
	} {
		msg := NewMessage(tt.operation, 0x1234, 0x0102030405060708, "hi")
		msg.Name = "bob"
		if encoded := msg.Encode(); !bytes.Equal(encoded, tt.want) {
			t.Fatalf("encoded bytes = %v, want %v", encoded, tt.want)
		}
	}
}

func TestDecodeIncompleteName(t *testing.T) {
	msg := NewMessage(FORWARDING, 1, 42, "")
	msg.Name = "abc"
	encoded := msg.Encode()
	for size := consts.HEADERS_SIZE; size < len(encoded); size++ {
		if _, err := DecodeDataMessage(encoded[:size]); err == nil {
			t.Fatal("expected an error for an incomplete name header")
		}
	}
}

func TestDecodeShortDataMessage(t *testing.T) {
	for size := range consts.HEADERS_SIZE {
		if _, err := DecodeDataMessage(make([]byte, size)); err == nil {
			t.Fatalf("expected an error for %d bytes", size)
		}
	}
}
