package server

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"slices"
	"strconv"
	"sync"
	"time"
)

type PeerConfig struct {
	ID      uint16
	RPCAddr string
}

type RPCConfig struct {
	ListenAddr       string
	AdvertiseAddr    string
	Peers            []PeerConfig
	SyncInterval     time.Duration
	Timeout          time.Duration
	HistoryEvents    int
	HistoryBytes     int
	SnapshotTTL      time.Duration
	FailoverAfter    time.Duration
	FailoverInterval time.Duration
}

type BrokerStatus struct {
	Metadata    BrokerMetadata
	Cursor      MetadataCursor
	Available   bool
	Synced      bool
	LastSuccess time.Time
}

type PeerRequest struct{ BrokerID uint16 }
type BrokerIdentity struct {
	ID       uint16
	Cursor   MetadataCursor
	Received MetadataCursor
}
type ChangesRequest struct {
	BrokerID uint16
	Cursor   MetadataCursor
	Through  uint64
	Offset   int
}
type ApplyChangesRequest struct {
	BrokerID uint16
	Chunk    MetadataChunk
}
type SnapshotInfo struct {
	ID     string
	Cursor MetadataCursor
	Size   int
}
type SnapshotRequest struct {
	BrokerID uint16
	ID       string
	Offset   int
}
type SnapshotChunk struct {
	ID      string
	Offset  int
	Data    []byte
	Done    bool
	Expired bool
}

type rpcPeer struct {
	config            PeerConfig
	replica           *MetadataReplica
	wake              chan struct{}
	mu                sync.Mutex
	available, synced bool
	lastSuccess       time.Time
}

func (p *rpcPeer) status(available, synced bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.available, p.synced = available, synced
	if available {
		p.lastSuccess = time.Now()
	}
}

func (p *rpcPeer) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

type snapshotSession struct {
	info     SnapshotInfo
	data     []byte
	lastRead time.Time
}

type brokerRuntime struct {
	server      *Server
	store       *MetadataStore
	config      RPCConfig
	address     string
	peers       map[uint16]*rpcPeer
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	snapshots   map[uint16]*snapshotSession
	started     time.Time
	cancel      context.CancelFunc
}

// BrokerRPC exposes only broker metadata, never the client's control protocol.
type BrokerRPC struct{ runtime *brokerRuntime }

func (b *BrokerRPC) peer(id uint16) (*rpcPeer, error) {
	p := b.runtime.peers[id]
	if p == nil {
		return nil, fmt.Errorf("broker %d não configurado", id)
	}
	return p, nil
}

func (b *BrokerRPC) Ping(req PeerRequest, reply *BrokerIdentity) error {
	p, err := b.peer(req.BrokerID)
	if err != nil {
		return err
	}
	cursor := b.runtime.store.Cursor()
	received := p.replica.Cursor()
	*reply = BrokerIdentity{ID: b.runtime.server.Id, Cursor: cursor, Received: received}
	return nil
}

func (b *BrokerRPC) GetChanges(req ChangesRequest, reply *MetadataChunk) error {
	if _, err := b.peer(req.BrokerID); err != nil {
		return err
	}
	*reply = b.runtime.store.Changes(req.Cursor, req.Through, req.Offset)
	return nil
}

func (b *BrokerRPC) ApplyChanges(req ApplyChangesRequest, reply *MetadataAck) error {
	p, err := b.peer(req.BrokerID)
	if err != nil {
		return err
	}
	if len(encodeMetadata(req.Chunk)) > MetadataBatchBytes {
		return fmt.Errorf("lote de metadata excede 16 KiB")
	}
	*reply, err = p.replica.Apply(req.Chunk)
	if reply.NeedSnapshot {
		p.notify()
	}
	if err == nil && !reply.NeedSnapshot {
		p.status(true, req.Chunk.Done && !req.Chunk.More)
		if req.Chunk.Done {
			metadata, _ := p.replica.Snapshot()
			b.runtime.server.rememberMetadata(metadata)
		}
	}
	return err
}

func (b *BrokerRPC) BeginSnapshot(req PeerRequest, reply *SnapshotInfo) error {
	if _, err := b.peer(req.BrokerID); err != nil {
		return err
	}
	metadata, cursor := b.runtime.store.Snapshot()
	data := encodeMetadata(metadata)
	info := SnapshotInfo{ID: rand.Text(), Cursor: cursor, Size: len(data)}
	b.runtime.mu.Lock()
	b.runtime.snapshots[req.BrokerID] = &snapshotSession{info: info, data: data, lastRead: time.Now()}
	b.runtime.mu.Unlock()
	*reply = info
	return nil
}

func (b *BrokerRPC) ReadSnapshot(req SnapshotRequest, reply *SnapshotChunk) error {
	if _, err := b.peer(req.BrokerID); err != nil {
		return err
	}
	r := b.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	session := r.snapshots[req.BrokerID]
	if session == nil || session.info.ID != req.ID || time.Since(session.lastRead) > r.config.SnapshotTTL || !r.store.CanReplay(session.info.Cursor) {
		delete(r.snapshots, req.BrokerID)
		*reply = SnapshotChunk{ID: req.ID, Expired: true}
		return nil
	}
	if req.Offset < 0 || req.Offset > len(session.data) {
		return fmt.Errorf("posição inválida no snapshot")
	}
	session.lastRead = time.Now()
	chunk := SnapshotChunk{ID: req.ID, Offset: req.Offset}
	end := min(len(session.data), req.Offset+MetadataBatchBytes)
	for {
		chunk.Data, chunk.Done = session.data[req.Offset:end], end == len(session.data)
		if excess := len(encodeMetadata(chunk)) - MetadataBatchBytes; excess > 0 {
			end -= excess
			continue
		}
		break
	}
	chunk.Data = slices.Clone(chunk.Data)
	*reply = chunk
	// Keep the final page until expiry so a lost response can be retried.
	return nil
}

// metadataLocked is called with the server's write lock held.
func (s *Server) metadataLocked() *MetadataStore {
	if s.metadata == nil {
		broker := BrokerMetadata{ID: s.Id}
		if s.TcpAddr != nil {
			broker.TCPAddr = s.TcpAddr.String()
		}
		for _, channel := range s.UdpConfig {
			if channel != nil && channel.udpAddr != nil {
				broker.UDPAddrs = append(broker.UDPAddrs, channel.udpAddr.String())
			}
		}
		for _, topic := range s.Topics {
			broker.Topics = append(broker.Topics, topic.metadata())
		}
		s.metadata = NewMetadataStore(broker, 0, 0)
	}
	return s.metadata
}

func (s *Server) notifyMetadataLocked() {
	if s.rpcRunning {
		for _, peer := range s.rpcState.peers {
			peer.notify()
		}
	}
}

func (s *Server) RPCAddress() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.rpcRunning {
		return ""
	}
	return s.rpcState.address
}

func (s *Server) ClusterMetadata() []BrokerStatus {
	s.mu.Lock()
	store, runtime, running := s.metadataLocked(), s.rpcState, s.rpcRunning
	s.mu.Unlock()
	local, cursor := store.Snapshot()
	result := []BrokerStatus{{Metadata: local, Cursor: cursor, Available: running, Synced: true}}
	if runtime != nil {
		for _, peer := range runtime.peers {
			metadata, cursor := peer.replica.Snapshot()
			peer.mu.Lock()
			result = append(result, BrokerStatus{Metadata: metadata, Cursor: cursor, Available: peer.available, Synced: peer.synced, LastSuccess: peer.lastSuccess})
			peer.mu.Unlock()
		}
	}
	slices.SortFunc(result, func(a, b BrokerStatus) int { return int(a.Metadata.ID) - int(b.Metadata.ID) })
	return result
}

func normalizeRPCConfig(id uint16, config RPCConfig) (RPCConfig, error) {
	if id == 0 {
		return config, fmt.Errorf("ID do broker não pode ser zero")
	}
	if config.ListenAddr == "" {
		config.ListenAddr = "127.0.0.1:0"
	}
	if config.SyncInterval == 0 {
		config.SyncInterval = 5 * time.Second
	}
	if config.Timeout == 0 {
		config.Timeout = 2 * time.Second
	}
	if config.SnapshotTTL == 0 {
		config.SnapshotTTL = 60 * time.Second
	}
	if config.FailoverAfter == 0 {
		config.FailoverAfter = 3 * config.SyncInterval
	}
	if config.FailoverInterval == 0 {
		config.FailoverInterval = min(config.SyncInterval, time.Second)
	}
	if config.HistoryEvents == 0 {
		config.HistoryEvents = 4096
	}
	if config.HistoryBytes == 0 {
		config.HistoryBytes = 4 * 1024 * 1024
	}
	if config.SyncInterval < 0 || config.Timeout < 0 || config.SnapshotTTL < 0 || config.HistoryEvents < 0 || config.HistoryBytes < 0 || config.FailoverAfter < 0 || config.FailoverInterval < 0 {
		return config, fmt.Errorf("limites RPC devem ser positivos")
	}
	config.Peers = slices.Clone(config.Peers)
	ids, addresses := make(map[uint16]bool), make(map[string]bool)
	for _, peer := range config.Peers {
		if peer.ID == 0 || peer.ID == id || ids[peer.ID] || addresses[peer.RPCAddr] {
			return config, fmt.Errorf("peer inválido ou duplicado: %d", peer.ID)
		}
		host, port, err := net.SplitHostPort(peer.RPCAddr)
		portNumber, portErr := strconv.Atoi(port)
		if err != nil || host == "" || portErr != nil || portNumber < 1 || portNumber > 65535 {
			return config, fmt.Errorf("endereço RPC de peer inválido: %s", peer.RPCAddr)
		}
		ids[peer.ID], addresses[peer.RPCAddr] = true, true
	}
	return config, nil
}

func rpcAdvertiseAddress(listener net.Listener, configured string) (string, error) {
	actual := listener.Addr().(*net.TCPAddr)
	if configured == "" {
		if actual.IP.IsUnspecified() {
			return "", fmt.Errorf("escuta genérica exige AdvertiseAddr explícito")
		}
		return actual.String(), nil
	}
	host, port, err := net.SplitHostPort(configured)
	if err != nil || host == "" {
		return "", fmt.Errorf("AdvertiseAddr inválido")
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return "", fmt.Errorf("AdvertiseAddr não pode ser genérico")
	}
	if port == "0" {
		port = strconv.Itoa(actual.Port)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return "", fmt.Errorf("porta anunciada inválida")
	}
	return net.JoinHostPort(host, port), nil
}

// ListenRPC serves metadata on a dedicated TCP listener, until cancellation.
func (s *Server) ListenRPC(ctx context.Context, config RPCConfig) error {
	config, err := normalizeRPCConfig(s.Id, config)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", config.ListenAddr)
	if err != nil {
		return err
	}
	return s.serveRPC(ctx, listener, config)
}

// serveRPC also permits tests to use already-bound ephemeral endpoints.
func (s *Server) serveRPC(ctx context.Context, listener net.Listener, config RPCConfig) error {
	defer listener.Close()
	var err error
	config, err = normalizeRPCConfig(s.Id, config)
	if err != nil {
		return err
	}
	address, err := rpcAdvertiseAddress(listener, config.AdvertiseAddr)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.mu.Lock()
	if s.rpcRunning {
		s.mu.Unlock()
		return fmt.Errorf("servidor já está escutando RPC")
	}
	store := s.metadataLocked()
	store.mu.Lock()
	store.maxEvents, store.maxBytes = config.HistoryEvents, config.HistoryBytes
	store.mu.Unlock()
	broker, _ := store.Snapshot()
	broker.RPCAddr = address
	store.UpdateBroker(broker)
	r := &brokerRuntime{server: s, store: store, config: config, address: address, peers: make(map[uint16]*rpcPeer), connections: make(map[net.Conn]struct{}), snapshots: make(map[uint16]*snapshotSession)}
	r.started = time.Now()
	r.cancel = cancel
	for _, cfg := range config.Peers {
		r.peers[cfg.ID] = &rpcPeer{config: cfg, replica: NewMetadataReplica(cfg.ID), wake: make(chan struct{}, 1)}
	}
	service := rpc.NewServer()
	if err := service.RegisterName("Broker", &BrokerRPC{runtime: r}); err != nil {
		s.mu.Unlock()
		return err
	}
	s.rpcState, s.rpcRunning = r, true
	s.mu.Unlock()
	defer func() {
		for _, peer := range r.peers {
			peer.status(false, false)
		}
		s.mu.Lock()
		s.rpcRunning = false
		s.mu.Unlock()
	}()
	var workers, handlers sync.WaitGroup
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-runCtx.Done()
		listener.Close()
		r.mu.Lock()
		for conn := range r.connections {
			conn.Close()
		}
		r.mu.Unlock()
	}()
	for _, peer := range r.peers {
		workers.Add(1)
		go func(peer *rpcPeer) { defer workers.Done(); r.syncPeer(runCtx, peer) }(peer)
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(config.FailoverInterval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				r.reconcileTopics()
			}
		}
	}()
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(max(time.Nanosecond, min(config.SnapshotTTL/2, time.Second)))
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				r.mu.Lock()
				for id, session := range r.snapshots {
					if time.Since(session.lastRead) > config.SnapshotTTL {
						delete(r.snapshots, id)
					}
				}
				r.mu.Unlock()
			}
		}
	}()
	var acceptErr error
	for {
		conn, err := listener.Accept()
		if err != nil {
			if runCtx.Err() == nil {
				acceptErr = err
			}
			break
		}
		r.mu.Lock()
		if runCtx.Err() != nil {
			r.mu.Unlock()
			conn.Close()
			break
		}
		r.connections[conn] = struct{}{}
		r.mu.Unlock()
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			service.ServeConn(conn)
			r.mu.Lock()
			delete(r.connections, conn)
			r.mu.Unlock()
		}()
	}
	cancel()
	<-shutdownDone
	workers.Wait()
	handlers.Wait()
	if acceptErr != nil {
		return acceptErr
	}
	return ctx.Err()
}

func (r *brokerRuntime) call(ctx context.Context, peer *rpcPeer, method string, args, reply any) error {
	callCtx, cancel := context.WithTimeout(ctx, r.config.Timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(callCtx, "tcp", peer.config.RPCAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline, _ := callCtx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(callCtx, func() { conn.Close() })
	defer stop()
	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call("Broker."+method, args, reply)
}

func (r *brokerRuntime) syncPeer(ctx context.Context, peer *rpcPeer) {
	ticker := time.NewTicker(r.config.SyncInterval)
	defer ticker.Stop()
	for {
		if err := r.syncRound(ctx, peer); err != nil {
			peer.status(false, false)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-peer.wake:
		}
	}
}

func (r *brokerRuntime) syncRound(ctx context.Context, peer *rpcPeer) error {
	var identity BrokerIdentity
	if err := r.call(ctx, peer, "Ping", PeerRequest{r.server.Id}, &identity); err != nil {
		return err
	}
	if identity.ID != peer.config.ID || identity.Cursor.Generation == "" {
		return fmt.Errorf("identidade RPC divergente")
	}
	if err := r.pullChanges(ctx, peer, identity.Cursor); err != nil {
		return err
	}
	peer.status(true, true)
	remote, _ := peer.replica.Snapshot()
	r.server.rememberMetadata(remote)
	// Ping reports how much of our journal the recipient has already installed.
	cursor, through, offset := identity.Received, uint64(0), 0
	for ctx.Err() == nil {
		chunk := r.store.Changes(cursor, through, offset)
		if !chunk.NeedSnapshot && chunk.Through == chunk.After {
			return nil
		}
		var ack MetadataAck
		if err := r.call(ctx, peer, "ApplyChanges", ApplyChangesRequest{r.server.Id, chunk}, &ack); err != nil {
			return err
		}
		if chunk.NeedSnapshot || ack.NeedSnapshot {
			return nil
		} // recipient wakes its pull worker
		if ack.Cursor.Generation == chunk.Generation && ack.Cursor.Revision >= chunk.Through {
			cursor, through, offset = ack.Cursor, 0, 0
		} else if ack.Offset > offset {
			through, offset = chunk.Through, ack.Offset
		} else {
			return fmt.Errorf("confirmação de metadata sem progresso")
		}
	}
	return ctx.Err()
}

func (r *brokerRuntime) pullChanges(ctx context.Context, peer *rpcPeer, identity MetadataCursor) error {
	through, offset := uint64(0), 0
	restarts := 0
	for ctx.Err() == nil {
		cursor := peer.replica.Cursor()
		var chunk MetadataChunk
		req := ChangesRequest{BrokerID: r.server.Id, Cursor: cursor, Through: through, Offset: offset}
		if err := r.call(ctx, peer, "GetChanges", req, &chunk); err != nil {
			return err
		}
		if chunk.Generation != identity.Generation {
			return fmt.Errorf("geração mudou durante sincronização")
		}
		if len(encodeMetadata(chunk)) > MetadataBatchBytes {
			return fmt.Errorf("lote RPC excede 16 KiB")
		}
		if chunk.NeedSnapshot {
			if err := r.pullSnapshot(ctx, peer, identity.Generation); err != nil {
				if errors.Is(err, errSnapshotExpired) && restarts < 3 {
					restarts++
					through, offset = 0, 0
					continue
				}
				return err
			}
			through, offset = 0, 0
			continue
		}
		if chunk.Through == cursor.Revision {
			return nil
		}
		ack, err := peer.replica.Apply(chunk)
		if err != nil {
			return err
		}
		if ack.NeedSnapshot {
			through, offset = 0, 0
			continue
		}
		if ack.Cursor.Revision >= chunk.Through {
			through, offset = 0, 0
		} else {
			through, offset = chunk.Through, ack.Offset
		}
	}
	return ctx.Err()
}

var errSnapshotExpired = errors.New("snapshot expirou; nova tentativa no próximo ciclo")

func (r *brokerRuntime) pullSnapshot(ctx context.Context, peer *rpcPeer, generation string) error {
	var info SnapshotInfo
	if err := r.call(ctx, peer, "BeginSnapshot", PeerRequest{r.server.Id}, &info); err != nil {
		return err
	}
	if info.Cursor.Generation != generation || info.ID == "" || info.Size < 0 {
		return fmt.Errorf("descritor de snapshot inválido")
	}
	var data []byte
	for ctx.Err() == nil {
		var chunk SnapshotChunk
		if err := r.call(ctx, peer, "ReadSnapshot", SnapshotRequest{r.server.Id, info.ID, len(data)}, &chunk); err != nil {
			return err
		}
		if chunk.Expired {
			return errSnapshotExpired
		}
		if chunk.ID != info.ID || chunk.Offset != len(data) || len(chunk.Data) == 0 || len(encodeMetadata(chunk)) > MetadataBatchBytes {
			return fmt.Errorf("página de snapshot inválida")
		}
		data = append(data, chunk.Data...)
		if len(data) > info.Size {
			return fmt.Errorf("snapshot excede tamanho declarado")
		}
		if chunk.Done {
			if len(data) != info.Size {
				return fmt.Errorf("snapshot incompleto")
			}
			var metadata BrokerMetadata
			if err := decodeMetadata(data, &metadata); err != nil {
				return err
			}
			return peer.replica.InstallSnapshot(metadata, info.Cursor)
		}
	}
	return ctx.Err()
}
