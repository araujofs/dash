package client

import (
	"context"
	"dash/consts"
	"dash/message"
	"encoding/binary"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"
)

type Broker struct {
	ID          uint16
	EnderecoTCP string
	EnderecoUDP string
}
type broker = Broker
type subscriptionBinding struct {
	session *controlConnection
	topic   uint16
}

// Client tracks logical topics and reconnects directly to their current owner.
// UDP sends are best effort; reconnection never replays publications.
type Client struct {
	Nome          string
	Bootstrap     Broker
	ConexaoUDP    *net.UDPConn
	RetryInterval time.Duration
	Timeout       time.Duration
	mu            sync.RWMutex
	lifecycleMu   sync.Mutex
	closing       bool
	connectMu     sync.Mutex
	reconcileMu   sync.Mutex
	sessions      map[string]*controlConnection
	seeds         map[string]bool
	routes        map[string]message.TopicRoute
	version       string
	desired       map[string]bool
	bindings      map[string]subscriptionBinding
	wake          chan struct{}
	cancel        context.CancelFunc
	done          chan struct{}
}

func NewClient(name string, bootstrap Broker) *Client {
	c := New(name, []string{bootstrap.EnderecoTCP})
	c.Bootstrap = bootstrap
	return c
}
func New(name string, seeds []string) *Client {
	c := &Client{Nome: name, RetryInterval: 250 * time.Millisecond, Timeout: 2 * time.Second, sessions: make(map[string]*controlConnection), seeds: make(map[string]bool), routes: make(map[string]message.TopicRoute), desired: make(map[string]bool), bindings: make(map[string]subscriptionBinding), wake: make(chan struct{}, 1)}
	for _, addr := range seeds {
		if addr != "" {
			c.seeds[addr] = true
		}
	}
	if len(seeds) > 0 {
		c.Bootstrap.EnderecoTCP = seeds[0]
	}
	return c
}
func (c *Client) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Client) orderAddresses(addresses []string) {
	slices.SortFunc(addresses, func(a, b string) int {
		if a == b {
			return 0
		}
		if a == c.Bootstrap.EnderecoTCP {
			return -1
		}
		if b == c.Bootstrap.EnderecoTCP {
			return 1
		}
		if a < b {
			return -1
		}
		return 1
	})
}
func (c *Client) acceptRoutes(table message.RouteTable) {
	c.mu.Lock()
	c.version = table.Version
	c.routes = make(map[string]message.TopicRoute, len(table.Routes))
	for _, r := range table.Routes {
		c.routes[r.LogicalID] = r
		if r.TCPAddr != "" {
			c.seeds[r.TCPAddr] = true
		}
	}
	for _, b := range table.Brokers {
		if b.TCPAddr != "" {
			c.seeds[b.TCPAddr] = true
		}
	}
	c.mu.Unlock()
}
func (c *Client) Connect(ctx context.Context) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		return fmt.Errorf("cliente já conectado")
	}
	c.mu.Unlock()
	udp, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.ConexaoUDP = udp
	c.closing = false
	c.cancel = cancel
	c.done = make(chan struct{})
	done := c.done
	c.mu.Unlock()
	if err := c.Recover(runCtx); err != nil {
		cancel()
		c.closeResources()
		c.mu.Lock()
		c.cancel = nil
		c.mu.Unlock()
		close(done)
		return err
	}
	go func() {
		defer close(done)
		defer c.closeResources()
		interval := c.RetryInterval
		if interval <= 0 {
			interval = 250 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			case <-c.wake:
			}
			if runCtx.Err() == nil {
				_ = c.Recover(runCtx)
			}
		}
	}()
	return nil
}
func (c *Client) Conectar() error { return c.Connect(context.Background()) }
func (c *Client) closeResources() {
	c.mu.Lock()
	c.closing = true
	sessions := c.sessions
	c.sessions = make(map[string]*controlConnection)
	udp := c.ConexaoUDP
	c.ConexaoUDP = nil
	c.mu.Unlock()
	for _, s := range sessions {
		s.close()
	}
	if udp != nil {
		udp.Close()
	}
}
func (c *Client) Close() error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.mu.RLock()
	cancel, done := c.cancel, c.done
	c.mu.RUnlock()
	if cancel != nil {
		cancel()
		c.closeResources()
		<-done
	}
	return nil
}
func (c *Client) connection(ctx context.Context, address string) (*controlConnection, error) {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	c.mu.RLock()
	existing, udp, closing := c.sessions[address], c.ConexaoUDP, c.closing
	c.mu.RUnlock()
	if closing {
		return nil, fmt.Errorf("cliente encerrado")
	}
	if existing != nil && !existing.closed() {
		return existing, nil
	}
	if udp == nil {
		return nil, fmt.Errorf("cliente não conectado")
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(callCtx, "tcp", address)
	if err != nil {
		return nil, err
	}
	s := newControlConnection(conn, c.acceptRoutes, c.notify)
	s.address = address
	reply, err := s.request(callCtx, message.NewControlMessage(message.IDENTIFY, 0, []byte(c.Nome)))
	if err == nil {
		err = responseError(reply)
		if err == nil {
			if len(reply.Payload) != 9 {
				err = fmt.Errorf("resposta IDENTIFY inválida")
			} else {
				s.clientID = binary.BigEndian.Uint64(reply.Payload[1:])
			}
		}
	}
	if err != nil {
		s.close()
		return nil, err
	}
	port := udp.LocalAddr().(*net.UDPAddr).Port
	reply, err = s.request(callCtx, message.NewControlMessage(message.REGISTER_UDP, 0, binary.BigEndian.AppendUint16(nil, uint16(port))))
	if err == nil {
		err = responseError(reply)
	}
	if err != nil {
		s.close()
		return nil, err
	}
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		s.close()
		return nil, fmt.Errorf("cliente encerrado")
	}
	c.sessions[address] = s
	c.mu.Unlock()
	return s, nil
}
func responseError(reply *message.ControlMessage) error {
	if reply == nil || len(reply.Payload) == 0 {
		return fmt.Errorf("resposta vazia")
	}
	if reply.Payload[0] != 0 {
		return fmt.Errorf("%s", reply.Payload[1:])
	}
	return nil
}
func (c *Client) request(ctx context.Context, s *controlConnection, op message.Operation, topic uint16, payload []byte) (*message.ControlMessage, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.request(callCtx, message.NewControlMessage(op, topic, payload))
}
func (c *Client) refresh(ctx context.Context) error {
	c.mu.RLock()
	addresses := make([]string, 0, len(c.seeds))
	for addr := range c.seeds {
		addresses = append(addresses, addr)
	}
	c.mu.RUnlock()
	c.orderAddresses(addresses)
	var last error = fmt.Errorf("nenhum broker configurado")
	for _, addr := range addresses {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s, err := c.connection(ctx, addr)
		if err != nil {
			last = err
			continue
		}
		c.mu.RLock()
		version := c.version
		c.mu.RUnlock()
		if _, err = c.request(ctx, s, message.GET_META, 0, []byte(version)); err != nil {
			last = err
			continue
		}
		return nil
	}
	return last
}

// Recover restores routing and subscriptions using each destination's client ID.
func (c *Client) Recover(ctx context.Context) error {
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	if err := c.refresh(ctx); err != nil {
		return err
	}
	c.mu.RLock()
	keys := make([]string, 0, len(c.desired))
	for key := range c.desired {
		keys = append(keys, key)
	}
	c.mu.RUnlock()
	for _, key := range keys {
		c.mu.RLock()
		r, ok := c.routes[key]
		old, bound := c.bindings[key]
		c.mu.RUnlock()
		if !ok {
			continue
		}
		s, err := c.connection(ctx, r.TCPAddr)
		if err != nil {
			continue
		}
		if bound && old.session == s && old.topic == r.TopicID && !s.closed() {
			continue
		}
		if bound && !old.session.closed() {
			_, _ = c.request(ctx, old.session, message.UNSUB, old.topic, nil)
		}
		reply, err := c.request(ctx, s, message.SUB, r.TopicID, nil)
		if err == nil {
			err = responseError(reply)
		}
		if err != nil {
			continue
		}
		c.mu.Lock()
		if c.desired[key] {
			c.bindings[key] = subscriptionBinding{s, r.TopicID}
		}
		c.mu.Unlock()
	}
	return nil
}
func (c *Client) Resolve(topic string) (message.TopicRoute, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if r, ok := c.routes[topic]; ok {
		return r, nil
	}
	var found message.TopicRoute
	for _, r := range c.routes {
		if r.Name == topic {
			if found.LogicalID != "" {
				return found, fmt.Errorf("nome ambíguo; use LogicalID")
			}
			found = r
		}
	}
	if found.LogicalID == "" {
		return found, fmt.Errorf("rota de tópico indisponível: %s", topic)
	}
	return found, nil
}
func (c *Client) Routes() []message.TopicRoute {
	c.mu.RLock()
	defer c.mu.RUnlock()
	routes := make([]message.TopicRoute, 0, len(c.routes))
	for _, r := range c.routes {
		routes = append(routes, r)
	}
	slices.SortFunc(routes, func(a, b message.TopicRoute) int {
		if a.LogicalID < b.LogicalID {
			return -1
		}
		if a.LogicalID > b.LogicalID {
			return 1
		}
		return 0
	})
	return routes
}
func (c *Client) Subscribe(ctx context.Context, topic string) error {
	if err := c.Recover(ctx); err != nil {
		return err
	}
	r, err := c.Resolve(topic)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.desired[r.LogicalID] = true
	c.mu.Unlock()
	if err := c.Recover(ctx); err != nil {
		return err
	}
	if !c.SubscriptionReady(r.LogicalID) {
		return fmt.Errorf("inscrição pendente de recuperação")
	}
	return nil
}

func (c *Client) SubscriptionReady(topic string) bool {
	r, err := c.Resolve(topic)
	if err != nil {
		return false
	}
	c.mu.RLock()
	binding, ok := c.bindings[r.LogicalID]
	c.mu.RUnlock()
	return ok && !binding.session.closed() && binding.session.address == r.TCPAddr && binding.topic == r.TopicID
}
func (c *Client) Unsubscribe(ctx context.Context, topic string) error {
	key := topic
	if r, err := c.Resolve(topic); err == nil {
		key = r.LogicalID
	}
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	c.mu.Lock()
	delete(c.desired, key)
	binding, ok := c.bindings[key]
	delete(c.bindings, key)
	c.mu.Unlock()
	if ok && !binding.session.closed() {
		reply, err := c.request(ctx, binding.session, message.UNSUB, binding.topic, nil)
		if err != nil {
			return err
		}
		return responseError(reply)
	}
	return nil
}
func (c *Client) Publish(ctx context.Context, topic, payload string) error {
	if err := c.Recover(ctx); err != nil {
		return err
	}
	r, err := c.Resolve(topic)
	if err != nil {
		return err
	}
	s, err := c.connection(ctx, r.TCPAddr)
	if err != nil {
		return err
	}
	addr, err := net.ResolveUDPAddr("udp", r.UDPAddr)
	if err != nil {
		return err
	}
	c.mu.RLock()
	udp := c.ConexaoUDP
	c.mu.RUnlock()
	if udp == nil {
		return fmt.Errorf("UDP não conectado")
	}
	_, err = udp.WriteToUDP(message.NewMessage(message.SEND, r.TopicID, s.clientID, payload).Encode(), addr)
	return err
}
func (c *Client) Receive(ctx context.Context) (*message.DataMessage, error) {
	c.mu.RLock()
	udp := c.ConexaoUDP
	c.mu.RUnlock()
	if udp == nil {
		return nil, fmt.Errorf("UDP não conectado")
	}
	data := make([]byte, consts.MAX_DATA_MESSAGE_SIZE)
	for ctx.Err() == nil {
		if err := udp.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
			return nil, err
		}
		n, _, err := udp.ReadFromUDP(data)
		if err != nil {
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				continue
			}
			return nil, err
		}
		msg, err := message.DecodeDataMessage(data[:n])
		if err == nil && msg.Operation == message.FORWARDING {
			return msg, nil
		}
	}
	return nil, ctx.Err()
}
func (c *Client) CreateTopic(ctx context.Context, name string) (string, error) {
	if err := c.Recover(ctx); err != nil {
		return "", err
	}
	c.mu.RLock()
	addresses := make([]string, 0, len(c.sessions))
	for addr, s := range c.sessions {
		if !s.closed() {
			addresses = append(addresses, addr)
		}
	}
	c.mu.RUnlock()
	c.orderAddresses(addresses)
	if len(addresses) == 0 {
		return "", fmt.Errorf("nenhuma sessão disponível")
	}
	s, err := c.connection(ctx, addresses[0])
	if err != nil {
		return "", err
	}
	reply, err := c.request(ctx, s, message.ADD, 0, []byte(name))
	if err == nil {
		err = responseError(reply)
	}
	if err != nil {
		return "", err
	}
	if len(reply.Payload) < 4 {
		return "", fmt.Errorf("resposta ADD inválida")
	}
	key := string(reply.Payload[3:])
	_ = c.Recover(ctx)
	return key, nil
}
func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	r, err := c.Resolve(topic)
	if err != nil {
		return err
	}
	s, err := c.connection(ctx, r.TCPAddr)
	if err != nil {
		return err
	}
	reply, err := c.request(ctx, s, message.DEL, r.TopicID, nil)
	if err != nil {
		return err
	}
	if err = responseError(reply); err != nil {
		return err
	}
	return c.Recover(ctx)
}
func (c *Client) Inscrever(topic string) error { return c.Subscribe(context.Background(), topic) }
func (c *Client) Publicar(topic, payload string) error {
	return c.Publish(context.Background(), topic, payload)
}
func (c *Client) Publciar(topic, payload string) error { return c.Publicar(topic, payload) }
