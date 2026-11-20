package client

import (
	"context"
	"dash/message"
	"fmt"
	"net"
	"sync"
	"time"
)

type controlConnection struct {
	conn       net.Conn
	clientID   uint64
	address    string
	requestMu  sync.Mutex
	writeMu    sync.Mutex
	mu         sync.Mutex
	pending    chan *message.ControlMessage
	expected   message.Operation
	done       chan struct{}
	once       sync.Once
	onMetadata func(message.RouteTable)
	onFailure  func()
}

func newControlConnection(conn net.Conn, onMetadata func(message.RouteTable), onFailure func()) *controlConnection {
	s := &controlConnection{conn: conn, done: make(chan struct{}), onMetadata: onMetadata, onFailure: onFailure}
	go s.read()
	return s
}
func (s *controlConnection) closed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}
func (s *controlConnection) close() {
	s.once.Do(func() {
		s.conn.Close()
		close(s.done)
		if s.onFailure != nil {
			s.onFailure()
		}
	})
}
func (s *controlConnection) request(ctx context.Context, req *message.ControlMessage) (*message.ControlMessage, error) {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if s.closed() {
		return nil, fmt.Errorf("conexão encerrada")
	}
	replies := make(chan *message.ControlMessage, 1)
	s.mu.Lock()
	s.pending, s.expected = replies, req.Operation
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.pending = nil; s.mu.Unlock() }()
	s.writeMu.Lock()
	if deadline, ok := ctx.Deadline(); ok {
		s.conn.SetWriteDeadline(deadline)
	} else {
		s.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	}
	err := message.WriteControlMessage(s.conn, req)
	s.writeMu.Unlock()
	if err != nil {
		s.close()
		return nil, err
	}
	select {
	case reply := <-replies:
		return reply, nil
	case <-s.done:
		return nil, fmt.Errorf("conexão encerrada")
	case <-ctx.Done():
		s.close()
		return nil, ctx.Err()
	}
}
func (s *controlConnection) deliver(msg *message.ControlMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending != nil && s.expected == msg.Operation {
		select {
		case s.pending <- msg:
		default:
		}
	}
}
func (s *controlConnection) read() {
	defer s.close()
	type assembly struct {
		version string
		data    []byte
	}
	assemblers := make(map[message.Operation]*assembly)
	for {
		msg, err := message.ReadControlMessage(s.conn)
		if err != nil {
			return
		}
		if msg.Operation != message.GET_META && msg.Operation != message.UPD_META {
			s.deliver(msg)
			continue
		}
		page, err := message.DecodeRoutePage(msg.Payload)
		if err != nil {
			return
		}
		if page.Unchanged {
			if msg.Operation != message.GET_META || !page.Done || len(page.Data) != 0 {
				return
			}
			s.deliver(msg)
			continue
		}
		if page.Offset == 0 {
			assemblers[msg.Operation] = &assembly{version: page.Version}
		}
		a := assemblers[msg.Operation]
		if a == nil || a.version != page.Version || page.Offset != uint64(len(a.data)) {
			return
		}
		a.data = append(a.data, page.Data...)
		if page.Done {
			table, err := message.DecodeRouteTable(a.data)
			if err != nil || table.Version != page.Version {
				return
			}
			s.onMetadata(table)
			delete(assemblers, msg.Operation)
			if msg.Operation == message.GET_META {
				s.deliver(msg)
			} else {
				s.writeMu.Lock()
				s.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				err := message.WriteControlMessage(s.conn, message.NewControlMessage(message.UPD_META, 0, []byte{0}))
				s.writeMu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}
}
