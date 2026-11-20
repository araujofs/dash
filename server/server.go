package server

import (
	"dash/consts"
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"
)

type UdpInfo struct {
	udpAddr *net.UDPAddr
	udpConn *net.UDPConn
	clients []*Client
}

type Server struct {
	Id              uint16
	Clients         map[string]*Client
	clientsByID     map[uint64]*Client
	Topics          map[string]*Topic
	topicsByID      map[uint16]*Topic
	topicsByLogical map[string]*Topic
	TcpAddr         *net.TCPAddr
	TcpListener     *net.TCPListener
	TopicSeq        *IDSequence[uint16]
	ClientSeq       *IDSequence[uint64]
	UdpConfig       []*UdpInfo
	mu              sync.RWMutex
	udpListening    bool
	metadata        *MetadataStore
	rpcState        *brokerRuntime
	rpcRunning      bool
}

// NewServer creates a server with a nonzero ID that must be unique in the cluster.
func NewServer(id uint16, tcpPort int) (*Server, error) {
	if id == 0 {
		return nil, fmt.Errorf("id do servidor não pode ser zero")
	}

	udpConfig := [consts.UDP_PORTS]*UdpInfo{}
	ready := false
	defer func() {
		if !ready {
			for _, channel := range udpConfig {
				if channel != nil {
					channel.udpConn.Close()
				}
			}
		}
	}()
	loopback := "127.0.0.1"

	for i := range consts.UDP_PORTS {
		udpAddr, err := net.ResolveUDPAddr("udp", loopback+":0")
		if err != nil {
			return nil, err
		}

		udpConn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			return nil, err
		}

		port := udpConn.LocalAddr().(*net.UDPAddr).Port
		udpAddr.Port = port

		udpConfig[i] = &UdpInfo{
			udpAddr: udpAddr,
			udpConn: udpConn,
			clients: []*Client{},
		}
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", loopback+":"+strconv.Itoa(tcpPort))
	if err != nil {
		return nil, err
	}

	tcpLis, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	if tcpPort == 0 {
		port := tcpLis.Addr().(*net.TCPAddr).Port
		tcpAddr.Port = port
	}

	ready = true
	return &Server{
		Id:              id,
		UdpConfig:       udpConfig[:],
		TcpAddr:         tcpAddr,
		TcpListener:     tcpLis,
		Clients:         map[string]*Client{},
		clientsByID:     map[uint64]*Client{},
		Topics:          map[string]*Topic{},
		topicsByID:      map[uint16]*Topic{},
		topicsByLogical: map[string]*Topic{},
		TopicSeq:        NewIDSequence(uint16(consts.MAX_TOPICS - 1)),
		ClientSeq:       NewIDSequence(^uint64(0)),
	}, nil
}

func (s *Server) AddClient(client *Client) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if client == nil {
		return fmt.Errorf("cliente não pode ser nulo")
	}

	if client.Name == "" {
		return fmt.Errorf("nome do cliente não pode ser vazio")
	}
	if len(client.Name) > consts.MAX_CLIENT_NAME_SIZE {
		return fmt.Errorf("nome do cliente excede %d bytes", consts.MAX_CLIENT_NAME_SIZE)
	}

	if _, exists := s.Clients[client.Name]; exists {
		return fmt.Errorf("cliente de nome %s já existe", client.Name)
	}

	if s.ClientSeq == nil {
		s.ClientSeq = NewIDSequence(^uint64(0))
	}
	id, err := s.ClientSeq.Next()
	if err != nil {
		return err
	}

	if s.Clients == nil {
		s.Clients = make(map[string]*Client)
	}
	if s.clientsByID == nil {
		s.clientsByID = make(map[uint64]*Client)
	}
	client.Id = id
	s.Clients[client.Name] = client
	s.clientsByID[id] = client
	return nil
}

func (s *Server) RemoveClient(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	client, exists := s.Clients[name]
	if !exists {
		return fmt.Errorf("cliente de nome %s não existe", name)
	}

	delete(s.Clients, name)
	delete(s.clientsByID, client.Id)
	s.ClientSeq.Release(client.Id)
	client.mu.Lock()
	cancel, done := client.udpCancel, client.udpDone
	client.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	return nil
}

func (s *Server) AddTopic(name string, client *Client) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.Topics[name]
	if exists {
		return fmt.Errorf("tópico de nome %s já existe", name)
	}

	if s.TopicSeq == nil {
		s.TopicSeq = NewIDSequence(uint16(consts.MAX_TOPICS - 1))
	}
	id, err := s.TopicSeq.Next()
	if err != nil {
		return err
	}

	if s.Topics == nil {
		s.Topics = make(map[string]*Topic)
	}
	if s.topicsByID == nil {
		s.topicsByID = make(map[uint16]*Topic)
	}
	metadata := s.metadataLocked()
	topic := newTopic(id, name, client)
	topic.HomeBrokerID = s.Id
	if s.topicsByLogical == nil {
		s.topicsByLogical = make(map[string]*Topic)
	}
	s.topicsByLogical[topic.LogicalID] = topic
	s.Topics[name] = topic
	s.topicsByID[id] = topic
	metadata.UpsertTopic(topic.metadata())
	s.notifyMetadataLocked()
	return nil
}

func (s *Server) RemoveTopic(name string, client *Client) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	toRemove, exists := s.Topics[name]
	if !exists {
		return fmt.Errorf("tópico de nome %s não existe", name)
	}

	if client != toRemove.Owner && (client == nil || client.Name != toRemove.OwnerName) {
		if client == nil {
			return fmt.Errorf("cliente não informado")
		}
		return fmt.Errorf("tópico de nome %s não pertence ao cliente %s", name, client.Name)
	}

	metadata := s.metadataLocked()
	delete(s.Topics, name)
	delete(s.topicsByID, toRemove.Id)
	delete(s.topicsByLogical, toRemove.LogicalID)
	s.TopicSeq.Release(toRemove.Id)
	metadata.RemoveTopic(toRemove.Id)
	s.notifyMetadataLocked()
	return nil
}

func (s *Server) AddClientToTopic(topicName, clientName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	topic, exists := s.Topics[topicName]
	if !exists {
		return fmt.Errorf("tópico de nome %s não existe", topicName)
	}
	client, exists := s.Clients[clientName]
	if !exists {
		return fmt.Errorf("cliente de nome %s não existe", clientName)
	}
	for _, subscriber := range topic.Clients {
		if subscriber == client {
			return fmt.Errorf("cliente de nome %s já está inscrito no tópico %s", clientName, topicName)
		}
	}
	topic.Clients = append(topic.Clients, client)
	return nil
}

func (s *Server) RemoveClientFromTopic(topicName, clientName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	topic, exists := s.Topics[topicName]
	if !exists {
		return fmt.Errorf("tópico de nome %s não existe", topicName)
	}
	client, exists := s.Clients[clientName]
	if !exists {
		return fmt.Errorf("cliente de nome %s não existe", clientName)
	}
	for i, subscriber := range topic.Clients {
		if subscriber == client {
			topic.Clients = slices.Delete(topic.Clients, i, i+1)
			return nil
		}
	}
	return fmt.Errorf("cliente de nome %s não está inscrito no tópico %s", clientName, topicName)
}
