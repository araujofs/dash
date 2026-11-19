package server

import (
	"context"
	"dash/consts"
	"dash/message"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// ListenUDP blocks until ctx is canceled or a UDP channel fails.
// Each channel reads independently; each recipient has one buffer-draining worker.
func (s *Server) ListenUDP(ctx context.Context) error {
	s.mu.Lock()
	if s.udpListening {
		s.mu.Unlock()
		return fmt.Errorf("servidor já está escutando UDP")
	}
	if len(s.UdpConfig) == 0 {
		s.mu.Unlock()
		return fmt.Errorf("servidor não possui canais UDP")
	}
	for _, channel := range s.UdpConfig {
		if channel == nil || channel.udpConn == nil {
			s.mu.Unlock()
			return fmt.Errorf("canal UDP sem conexão")
		}
	}
	s.udpListening = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.udpListening = false
		s.mu.Unlock()
	}()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var readers, workers sync.WaitGroup
	errorsCh := make(chan error, len(s.UdpConfig))
	for _, channel := range s.UdpConfig {
		readers.Add(1)
		go func(channel *UdpInfo) {
			defer readers.Done()
			err := s.readUDP(runCtx, channel.udpConn, &workers)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				errorsCh <- err
				cancel()
			}
		}(channel)
	}
	readers.Wait()
	cancel()
	workers.Wait()
	select {
	case err := <-errorsCh:
		return err
	default:
		return ctx.Err()
	}
}

// readUDP receives and decodes publications from one socket.
func (s *Server) readUDP(ctx context.Context, conn *net.UDPConn, workers *sync.WaitGroup) error {
	defer conn.SetReadDeadline(time.Time{})
	data := make([]byte, consts.MAX_DATA_MESSAGE_SIZE)
	for ctx.Err() == nil {
		if err := conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			return err
		}
		n, _, err := conn.ReadFromUDP(data)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			return fmt.Errorf("receber mensagem UDP: %w", err)
		}
		msg, err := message.DecodeDataMessage(data[:n])
		if err != nil || msg.Operation != message.SEND {
			continue
		}
		s.forwardUDP(ctx, conn, *msg, workers)
	}
	return ctx.Err()
}

// forwardUDP identifies the publisher and queues a copy for each subscriber.
func (s *Server) forwardUDP(ctx context.Context, conn *net.UDPConn, msg message.DataMessage, workers *sync.WaitGroup) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sender := s.clientsByID[msg.ClientID]
	if sender == nil {
		return
	}
	msg.Operation = message.FORWARDING
	msg.Name = sender.Name
	topic := s.topicsByID[msg.Topic]
	if topic == nil {
		return
	}
	for _, client := range topic.Clients {
		if client == nil || client.UdpAddr == nil || s.Clients[client.Name] != client {
			continue
		}
		if err := client.enqueueMessage(msg); err != nil {
			continue
		}
		client.startUDPWorker(ctx, conn, workers)
	}
}
