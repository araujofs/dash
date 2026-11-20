package server

import (
	"context"
	"dash/consts"
	"dash/message"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func udpSocket(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func udpServer(t *testing.T) *Server {
	t.Helper()
	s, err := NewServer(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.TcpListener.Close()
		for _, channel := range s.UdpConfig {
			channel.udpConn.Close()
		}
	})
	return s
}

func startUDP(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.ListenUDP(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("ListenUDP returned %v, want cancellation", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("UDP readers or client workers did not stop")
		}
	})
}

func TestListenUDPMultipleChannels(t *testing.T) {
	s := udpServer(t)
	publisher := udpSocket(t)
	alternatePublisher := udpSocket(t)
	// The sender needs no registered UDP address and can publish from either socket.
	publisherClient := &Client{Name: "publisher"}
	if err := s.AddClient(publisherClient); err != nil {
		t.Fatal(err)
	}
	var recipients []*net.UDPConn
	var clients []*Client
	for i := range 2 {
		conn := udpSocket(t)
		client := &Client{Name: fmt.Sprintf("subscriber-%d", i), UdpAddr: conn.LocalAddr().(*net.UDPAddr)}
		if err := s.AddClient(client); err != nil {
			t.Fatal(err)
		}
		recipients = append(recipients, conn)
		clients = append(clients, client)
	}
	if err := s.AddTopic("updates", clients[0]); err != nil {
		t.Fatal(err)
	}
	topic := s.Topics["updates"]
	for _, client := range clients {
		if err := s.AddClientToTopic("updates", client.Name); err != nil {
			t.Fatal(err)
		}
	}
	startUDP(t, s)

	var writers sync.WaitGroup
	const perChannel = 8
	for index, channel := range s.UdpConfig {
		writers.Add(1)
		go func() {
			defer writers.Done()
			// Bad packets must not stop a channel or reach subscribers.
			for _, data := range [][]byte{
				{}, {byte(message.SEND)}, {255, 0, 1},
				{byte(message.SEND), 0, byte(topic.Id), 5, 'a'},
				message.NewMessage(message.PING, topic.Id, publisherClient.Id, "control").Encode(),
				message.NewMessage(message.FORWARDING, topic.Id, publisherClient.Id, "loop").Encode(),
				message.NewMessage(message.SEND, topic.Id+1, publisherClient.Id, "unknown topic").Encode(),
				message.NewMessage(message.SEND, topic.Id, 0, "zero ID").Encode(),
				message.NewMessage(message.SEND, topic.Id, ^uint64(0), "unknown ID").Encode(),
			} {
				if _, err := publisher.WriteToUDP(data, channel.udpAddr); err != nil {
					t.Error(err)
					return
				}
			}
			for i := range perChannel {
				msg := message.NewMessage(message.SEND, topic.Id, publisherClient.Id, fmt.Sprintf("channel-%d/message-%d", index, i))
				socket := publisher
				if i%2 == 1 {
					socket = alternatePublisher
				}
				if _, err := socket.WriteToUDP(msg.Encode(), channel.udpAddr); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	writers.Wait()
	for _, recipient := range recipients {
		if err := recipient.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		seen := make(map[string]bool)
		for range len(s.UdpConfig) * perChannel {
			data := make([]byte, consts.MAX_DATA_MESSAGE_SIZE)
			n, _, err := recipient.ReadFromUDP(data)
			if err != nil {
				t.Fatal(err)
			}
			msg, err := message.DecodeDataMessage(data[:n])
			if err != nil {
				t.Fatal(err)
			}
			if msg.Operation != message.FORWARDING || msg.Topic != topic.Id || msg.ClientID != publisherClient.Id || msg.Name != publisherClient.Name || seen[msg.Payload] {
				t.Fatalf("unexpected or duplicate message: %+v", msg)
			}
			seen[msg.Payload] = true
		}
		for channel := range s.UdpConfig {
			for i := range perChannel {
				if !seen[fmt.Sprintf("channel-%d/message-%d", channel, i)] {
					t.Fatalf("missing message %d from channel %d", i, channel)
				}
			}
		}
	}
	// Removal stops the worker even though the topic still references the client.
	if err := s.RemoveClient(clients[0].Name); err != nil {
		t.Fatal(err)
	}
	clients[0].mu.Lock()
	running := clients[0].udpCancel != nil
	clients[0].mu.Unlock()
	if running {
		t.Fatal("removed client's UDP worker is still running")
	}
}

func TestListenUDPFullPayload(t *testing.T) {
	s := udpServer(t)
	publisher := udpSocket(t)
	recipient := udpSocket(t)
	sender := &Client{Name: strings.Repeat("n", 255)}
	subscriber := &Client{Name: "subscriber", UdpAddr: recipient.LocalAddr().(*net.UDPAddr)}
	for _, client := range []*Client{sender, subscriber} {
		if err := s.AddClient(client); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AddTopic("full", sender); err != nil {
		t.Fatal(err)
	}
	if err := s.AddClientToTopic("full", subscriber.Name); err != nil {
		t.Fatal(err)
	}
	startUDP(t, s)
	payload := strings.Repeat("x", 2048)
	msg := message.NewMessage(message.SEND, s.Topics["full"].Id, sender.Id, payload)
	packet := msg.Encode()
	if len(packet) != 2059 {
		t.Fatalf("SEND size = %d, want 2059", len(packet))
	}
	if _, err := publisher.WriteToUDP(packet, s.UdpConfig[0].udpAddr); err != nil {
		t.Fatal(err)
	}
	if err := recipient.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, consts.MAX_DATA_MESSAGE_SIZE)
	n, _, err := recipient.ReadFromUDP(data)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2315 {
		t.Fatalf("FORWARDING size = %d, want 2315", n)
	}
	forwarded, err := message.DecodeDataMessage(data[:n])
	if err != nil {
		t.Fatal(err)
	}
	if forwarded.Operation != message.FORWARDING || forwarded.ClientID != sender.Id || forwarded.Topic != msg.Topic || forwarded.Name != sender.Name || forwarded.Payload != payload {
		t.Fatal("forwarding changed the full payload or publisher metadata")
	}
}

func TestListenUDPChannelFailure(t *testing.T) {
	s := udpServer(t)
	s.UdpConfig[0].udpConn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.ListenUDP(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ListenUDP = %v; want a socket error and all readers stopped", err)
	}
}
