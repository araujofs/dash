package message

import (
	"bytes"
	"dash/consts"
	"strings"
	"testing"
)

func TestDataMessageRoundTrip(t *testing.T) {
	for _, clientID := range []uint64{0, 1, ^uint64(0)} {
		for _, name := range []string{"", "alice", "João", strings.Repeat("n", consts.MAX_CLIENT_NAME_SIZE)} {
			for _, payload := range []string{"", "hello", strings.Repeat("a", consts.MAX_DATA_PAYLOAD_SIZE+10)} {
				original := NewMessage(SEND, 42, clientID, name, payload)
				encoded := original.Encode()
				decoded, err := DecodeDataMessage(encoded)
				if err != nil {
					t.Fatal(err)
				}
				want := payload[:min(len(payload), consts.MAX_DATA_PAYLOAD_SIZE-len(name))]
				if decoded.Operation != SEND || decoded.Topic != 42 || decoded.ClientID != clientID || decoded.Name != name || decoded.Payload != want {
					t.Fatalf("unexpected decoded message: %+v", decoded)
				}
				if len(encoded) != consts.HEADERS_SIZE+len(name)+len(want) {
					t.Fatal("encoded packet length does not match headers and payload")
				}
			}
		}
	}
}

func TestDataMessageHeaderLayout(t *testing.T) {
	encoded := NewMessage(SEND, 0x1234, 0x0102030405060708, "bob", "hi").Encode()
	want := []byte{byte(SEND), 0x12, 0x34, 1, 2, 3, 4, 5, 6, 7, 8, 3, 'b', 'o', 'b', 'h', 'i'}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encoded bytes = %v, want %v", encoded, want)
	}
}

func TestDecodeIncompleteName(t *testing.T) {
	encoded := NewMessage(SEND, 1, 42, "abc", "").Encode()
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
