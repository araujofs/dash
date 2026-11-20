package server

import (
	"context"
	"dash/consts"
	"dash/message"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"
	"unicode/utf8"
)

func (s *Server) ownsTopicID(id uint16) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topicsByID[id] != nil
}

// Run starts all three services and closes them together on cancellation/failure.
func (s *Server) Run(ctx context.Context, config RPCConfig) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 3)
	go func() { results <- s.ListenTCP(runCtx) }()
	go func() { results <- s.ListenUDP(runCtx) }()
	go func() { results <- s.ListenRPC(runCtx, config) }()
	err := <-results
	cancel()
	s.Close()
	<-results
	<-results
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// ListenTCP serves client sessions; cancellation closes accepted sockets.
func (s *Server) ListenTCP(ctx context.Context) error {
	s.mu.Lock()
	if s.tcpRunning || s.TcpListener == nil {
		s.mu.Unlock()
		return fmt.Errorf("listener TCP ausente ou já em execução")
	}
	s.tcpRunning = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.tcpRunning = false; s.mu.Unlock() }()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	connections := make(map[*net.TCPConn]bool)
	var workers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-runCtx.Done()
		s.TcpListener.Close()
		mu.Lock()
		for conn := range connections {
			conn.Close()
		}
		mu.Unlock()
	}()
	var acceptErr error
	for {
		conn, err := s.TcpListener.AcceptTCP()
		if err != nil {
			if runCtx.Err() == nil {
				acceptErr = err
			}
			break
		}
		mu.Lock()
		if runCtx.Err() != nil {
			mu.Unlock()
			conn.Close()
			break
		}
		connections[conn] = true
		mu.Unlock()
		workers.Add(1)
		go func() {
			defer workers.Done()
			s.handleTCP(runCtx, conn)
			mu.Lock()
			delete(connections, conn)
			mu.Unlock()
		}()
	}
	cancel()
	<-done
	workers.Wait()
	if acceptErr != nil {
		return acceptErr
	}
	return ctx.Err()
}

type controlSession struct {
	conn    *net.TCPConn
	writeMu sync.Mutex
}

func (session *controlSession) send(op message.Operation, topic uint16, payload []byte) error {
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	return session.sendLocked(op, topic, payload)
}
func (session *controlSession) sendLocked(op message.Operation, topic uint16, payload []byte) error {
	if err := session.conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	return message.WriteControlMessage(session.conn, message.NewControlMessage(op, topic, payload))
}
func (session *controlSession) metadata(op message.Operation, table message.RouteTable) error {
	data := table.Encode()
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	for offset := 0; offset < len(data); {
		end := min(len(data), offset+consts.MAX_CONTROL_PAYLOAD_SIZE)
		var encoded []byte
		for {
			encoded = (message.RoutePage{Version: table.Version, Offset: uint64(offset), Data: data[offset:end], Done: end == len(data)}).Encode()
			if len(encoded) <= consts.MAX_CONTROL_PAYLOAD_SIZE {
				break
			}
			end--
		}
		if err := session.sendLocked(op, 0, encoded); err != nil {
			return err
		}
		offset = end
	}
	return nil
}

func (s *Server) handleTCP(ctx context.Context, conn *net.TCPConn) {
	defer conn.Close()
	session := &controlSession{conn: conn}
	clientCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var client *Client
	var notifier sync.WaitGroup
	defer func() {
		cancel()
		notifier.Wait()
		if client != nil {
			s.removeClient(client.Name, client)
		}
	}()
	for {
		req, err := message.ReadControlMessage(conn)
		if err != nil {
			return
		}
		var result []byte
		var operationErr error
		if req.Operation == message.IDENTIFY {
			if client != nil {
				operationErr = fmt.Errorf("cliente já identificado")
			} else if req.Topic != 0 || !utf8.Valid(req.Payload) {
				operationErr = fmt.Errorf("identificação inválida")
			} else {
				candidate := &Client{Name: string(req.Payload), TcpConn: conn, TcpAddr: conn.RemoteAddr().(*net.TCPAddr)}
				operationErr = s.AddClient(candidate)
				if operationErr == nil {
					client = candidate
					result = binary.BigEndian.AppendUint64([]byte{0}, client.Id)
				}
			}
		} else if client == nil {
			operationErr = fmt.Errorf("IDENTIFY necessário antes da operação")
		} else {
			switch req.Operation {
			case message.PING:
				result = []byte{0}
			case message.REGISTER_UDP:
				if len(req.Payload) != 2 || binary.BigEndian.Uint16(req.Payload) == 0 {
					operationErr = fmt.Errorf("porta UDP inválida")
				} else {
					remote := conn.RemoteAddr().(*net.TCPAddr)
					s.mu.Lock()
					client.mu.Lock()
					client.UdpAddr = &net.UDPAddr{IP: slices.Clone(remote.IP), Zone: remote.Zone, Port: int(binary.BigEndian.Uint16(req.Payload))}
					client.mu.Unlock()
					s.mu.Unlock()
					result = []byte{0}
				}
			case message.GET_META:
				table := s.RoutingTable()
				if string(req.Payload) == table.Version {
					if session.send(message.GET_META, 0, (message.RoutePage{Version: table.Version, Done: true, Unchanged: true}).Encode()) != nil {
						return
					}
					continue
				}
				if session.metadata(message.GET_META, table) != nil {
					return
				}
				continue
			case message.UPD_META:
				continue
			case message.ADD:
				if !utf8.Valid(req.Payload) || len(req.Payload) == 0 {
					operationErr = fmt.Errorf("nome do tópico inválido")
				} else {
					operationErr = s.AddTopic(string(req.Payload), client)
				}
				if operationErr == nil {
					s.mu.RLock()
					topic := s.Topics[string(req.Payload)]
					result = binary.BigEndian.AppendUint16([]byte{0}, topic.Id)
					result = append(result, topic.LogicalID...)
					s.mu.RUnlock()
				}
			case message.DEL:
				s.mu.RLock()
				topic := s.topicsByID[req.Topic]
				name := ""
				if topic != nil {
					name = topic.indexName
				}
				s.mu.RUnlock()
				if topic == nil {
					operationErr = fmt.Errorf("tópico inexistente")
				} else {
					operationErr = s.RemoveTopic(name, client)
				}
				result = []byte{0}
			case message.SUB, message.UNSUB:
				operationErr = s.subscriptionByID(req.Topic, client, req.Operation == message.SUB)
				result = []byte{0}
			default:
				operationErr = fmt.Errorf("operação de controle não suportada")
			}
		}
		if operationErr != nil {
			result = append([]byte{1}, []byte(operationErr.Error())...)
			result = result[:min(len(result), consts.MAX_CONTROL_PAYLOAD_SIZE)]
		}
		if err := session.send(req.Operation, req.Topic, result); err != nil {
			return
		}
		if req.Operation == message.IDENTIFY && operationErr == nil {
			notifier.Add(1)
			go func() {
				defer notifier.Done()
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				version := ""
				for {
					select {
					case <-clientCtx.Done():
						return
					case <-ticker.C:
						table := s.RoutingTable()
						if table.Version != version {
							if session.metadata(message.UPD_META, table) != nil {
								conn.Close()
								return
							}
							version = table.Version
						}
					}
				}
			}()
		}
	}
}

func (s *Server) subscriptionByID(id uint16, client *Client, add bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	topic := s.topicsByID[id]
	if topic == nil {
		return fmt.Errorf("tópico inexistente ou transferido")
	}
	if s.Clients[client.Name] != client {
		return fmt.Errorf("cliente desconectado")
	}
	for i, c := range topic.Clients {
		if c == client {
			if add {
				return nil
			}
			topic.Clients = slices.Delete(topic.Clients, i, i+1)
			return nil
		}
	}
	if add {
		topic.Clients = append(topic.Clients, client)
	}
	return nil
}

// Close stops all listeners. Client connections are closed by ListenTCP.
func (s *Server) Close() error {
	s.mu.RLock()
	r := s.rpcState
	s.mu.RUnlock()
	if r != nil {
		r.mu.Lock()
		cancel := r.cancel
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
	var errs []error
	if s.TcpListener != nil {
		if err := s.TcpListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	for _, channel := range s.UdpConfig {
		if channel != nil && channel.udpConn != nil {
			if err := channel.udpConn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}
