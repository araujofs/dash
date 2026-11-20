package message

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
)

func TestRouteProtobufGolden(t *testing.T) {
	page := RoutePage{Version: "v", Offset: 3, Data: []byte("ab"), Done: true}
	want := []byte{10, 1, 'v', 16, 3, 26, 2, 'a', 'b', 32, 1}
	if !bytes.Equal(page.Encode(), want) {
		t.Fatalf("protobuf wire mismatch: %v", page.Encode())
	}
	table := RouteTable{Version: "v", Routes: []TopicRoute{{LogicalID: "key", Name: "João", BrokerID: 2, TopicID: 9, TCPAddr: "127.0.0.1:1", UDPAddr: "127.0.0.1:2", OwnerName: "owner", HomeBrokerID: 1}}, Brokers: []BrokerEndpoint{{ID: 2, TCPAddr: "127.0.0.1:1", Available: true}}}
	decoded, err := DecodeRouteTable(table.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Version != table.Version || len(decoded.Routes) != 1 || decoded.Routes[0] != table.Routes[0] || decoded.Brokers[0] != table.Brokers[0] {
		t.Fatal("protobuf route round trip changed data")
	}
	for _, data := range [][]byte{{10, 255}, {0}, {15}, {10, 3, 'a'}} {
		if _, err := DecodeRoutePage(data); err == nil {
			t.Fatal("invalid protobuf accepted")
		}
	}
}

type oneByteReader struct{ reader io.Reader }

func (r oneByteReader) Read(data []byte) (int, error) { return r.reader.Read(data[:min(len(data), 1)]) }
func TestReadControlFrames(t *testing.T) {
	a, _ := NewControlMessage(IDENTIFY, 0, []byte("name")).Encode()
	b, _ := NewControlMessage(PING, 0, nil).Encode()
	reader := oneByteReader{bytes.NewReader(append(a, b...))}
	for _, want := range []Operation{IDENTIFY, PING} {
		msg, err := ReadControlMessage(reader)
		if err != nil || msg.Operation != want {
			t.Fatalf("stream framing: %+v %v", msg, err)
		}
	}
	prefix := binary.BigEndian.AppendUint32(nil, 260)
	if _, err := ReadControlMessage(bytes.NewReader(prefix)); err == nil {
		t.Fatal("oversized body was accepted")
	}
	if _, err := ReadControlMessage(strings.NewReader("short")); err == nil {
		t.Fatal("incomplete body was accepted")
	}
}
