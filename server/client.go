package server

import (
	"context"
	"dash/message"
	"dash/server/buffer"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

type Client struct {
	Id           uint64
	Name         string
	UdpAddr      *net.UDPAddr
	TcpAddr      *net.TCPAddr
	TcpConn      *net.TCPConn
	Buffer       *buffer.Buffer[message.DataMessage]
	mu           sync.Mutex
	messageReady chan struct{}
	udpCancel    context.CancelFunc
	udpDone      chan struct{}
}

// enqueueMessage adds a message and wakes the client's sender without blocking.
func (c *Client) enqueueMessage(msg message.DataMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Buffer == nil {
		c.Buffer = &buffer.Buffer[message.DataMessage]{}
	}
	if c.messageReady == nil {
		c.messageReady = make(chan struct{}, 1)
	}
	if err := c.Buffer.Queue(buffer.NewItem(msg)); err != nil {
		return err
	}
	select {
	case c.messageReady <- struct{}{}:
	default:
	}
	return nil
}

// startUDPWorker starts at most one sender for this client.
// The server holds its read lock until this call returns so removal cannot race startup.
func (c *Client) startUDPWorker(ctx context.Context, conn *net.UDPConn, workers *sync.WaitGroup) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.udpCancel != nil {
		return
	}
	clientCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	c.udpCancel, c.udpDone = stop, done
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer stop()
		if err := c.HandleMessages(clientCtx, conn); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("UDP: %v", err)
		}
		c.mu.Lock()
		c.udpCancel, c.udpDone = nil, nil
		close(done)
		c.mu.Unlock()
	}()
}

// HandleMessages drains the client's buffer and sends messages over UDP.
func (c *Client) HandleMessages(ctx context.Context, conn *net.UDPConn) error {
	if conn == nil || c.UdpAddr == nil {
		return fmt.Errorf("conexão ou endereço UDP ausente para cliente %s", c.Name)
	}
	c.mu.Lock()
	if c.Buffer == nil {
		c.Buffer = &buffer.Buffer[message.DataMessage]{}
	}
	if c.messageReady == nil {
		c.messageReady = make(chan struct{}, 1)
	}
	ready := c.messageReady
	c.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		item := c.Buffer.Dequeue()
		c.mu.Unlock()
		if item == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ready:
			}
			continue
		}
		if _, err := conn.WriteToUDP(item.Value().Encode(), c.UdpAddr); err != nil {
			return fmt.Errorf("enviar mensagem para cliente %s: %w", c.Name, err)
		}
	}
}
