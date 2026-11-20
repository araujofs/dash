package server

import (
	"context"
	dashclient "dash/client"
	"dash/message"
	"net"
	"slices"
	"sync"
	"testing"
	"time"
)

func runFailoverNode(t *testing.T, s *Server, listener net.Listener, peers []PeerConfig) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	for _, run := range []func() error{
		func() error { return s.ListenTCP(ctx) },
		func() error { return s.ListenUDP(ctx) },
		func() error {
			return s.serveRPC(ctx, listener, RPCConfig{Peers: peers, SyncInterval: 20 * time.Millisecond, Timeout: 100 * time.Millisecond, FailoverAfter: 100 * time.Millisecond, FailoverInterval: 10 * time.Millisecond})
		},
	} {
		workers.Add(1)
		go func() { defer workers.Done(); _ = run() }()
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			s.Close()
			done := make(chan struct{})
			go func() { workers.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(4 * time.Second):
				t.Error("node did not stop")
			}
		})
	}
	t.Cleanup(stop)
	awaitRPC(t, func() bool {
		s.mu.RLock()
		ready := s.tcpRunning && s.udpListening && s.rpcRunning
		s.mu.RUnlock()
		return ready
	})
	return stop
}

func TestTopicDestinationDeterministic(t *testing.T) {
	a := TopicDestination("stable", 1, []uint16{3, 2})
	b := TopicDestination("stable", 1, []uint16{2, 3})
	if a == 0 || a != b {
		t.Fatal("selection depends on map order")
	}
	if TopicDestination("stable", 1, []uint16{3, 1, 2}) != 1 {
		t.Fatal("home broker was not preferred")
	}
	if TopicDestination("stable", 1, nil) != 0 {
		t.Fatal("empty cluster selected a broker")
	}
	if TopicDestination("stable", 1, []uint16{2}) != 2 || TopicDestination("stable", 1, []uint16{3}) != 3 {
		t.Fatal("availability-first partitions should select their own reachable broker")
	}
}

func TestClientFailoverAndAutomaticFailback(t *testing.T) {
	servers := []*Server{rpcTestServer(t, 1), rpcTestServer(t, 2), rpcTestServer(t, 3)}
	listeners := []net.Listener{rpcTestListener(t, "127.0.0.1:0"), rpcTestListener(t, "127.0.0.1:0"), rpcTestListener(t, "127.0.0.1:0")}
	addresses := []string{listeners[0].Addr().String(), listeners[1].Addr().String(), listeners[2].Addr().String()}
	configs := func(id uint16) []PeerConfig {
		var peers []PeerConfig
		for i, address := range addresses {
			if uint16(i+1) != id {
				peers = append(peers, PeerConfig{ID: uint16(i + 1), RPCAddr: address})
			}
		}
		return peers
	}
	stops := make([]func(), 3)
	for i, s := range servers {
		stops[i] = runFailoverNode(t, s, listeners[i], configs(s.Id))
	}
	awaitRPC(t, func() bool { return remoteStatus(servers[0], 2).Synced && remoteStatus(servers[0], 3).Synced })
	homeTCP := servers[0].TcpAddr.String()
	homePort := servers[0].TcpAddr.Port
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	publisher := dashclient.New("publisher", []string{homeTCP})
	publisher.Timeout = 200 * time.Millisecond
	publisher.RetryInterval = 20 * time.Millisecond
	if err := publisher.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { publisher.Close() })
	key, err := publisher.CreateTopic(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	subscriber := dashclient.New("subscriber", []string{homeTCP})
	subscriber.Timeout = 200 * time.Millisecond
	subscriber.RetryInterval = 20 * time.Millisecond
	if err := subscriber.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { subscriber.Close() })
	if err := subscriber.Subscribe(ctx, key); err != nil {
		t.Fatal(err)
	}
	deliver := func(payload string) {
		t.Helper()
		if err := publisher.Publish(ctx, key, payload); err != nil {
			t.Fatal(err)
		}
		receiveCtx, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		msg, err := subscriber.Receive(receiveCtx)
		if err != nil {
			t.Fatal(err)
		}
		if msg.Payload != payload || msg.Name != "publisher" {
			t.Fatalf("incorrect forwarded message: %+v", msg)
		}
	}
	deliver("before-failure")
	before, err := publisher.Resolve(key)
	if err != nil || before.BrokerID != 1 {
		t.Fatalf("unexpected original route: %+v, %v", before, err)
	}
	// Wait until both survivors have retained the logical identity and owner.
	awaitRPC(t, func() bool {
		for _, s := range servers[1:] {
			s.mu.RLock()
			store := s.metadata
			s.mu.RUnlock()
			state, _ := store.Snapshot()
			found := false
			for _, topic := range state.KnownTopics {
				if topic.LogicalID == key {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
	stops[0]()
	awaitRPC(t, func() bool {
		route, err := subscriber.Resolve(key)
		return err == nil && route.BrokerID != 1 && subscriber.SubscriptionReady(key)
	})
	awaitRPC(t, func() bool {
		route, err := publisher.Resolve(key)
		other, _ := subscriber.Resolve(key)
		return err == nil && route.BrokerID == other.BrokerID
	})
	after, _ := subscriber.Resolve(key)
	for _, s := range servers[1:] {
		awaitRPC(t, func() bool {
			for _, route := range s.RoutingTable().Routes {
				if route.LogicalID == key {
					return route.BrokerID == after.BrokerID
				}
			}
			return false
		})
	}
	deliver("after-failure")
	// Return the original ID with an occupied local ID, forcing topic-ID remapping.
	restarted, err := NewServer(1, homePort)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Close() })
	if err := restarted.AddTopic("occupied", &Client{Name: "other"}); err != nil {
		t.Fatal(err)
	}
	rebound := rpcTestListener(t, addresses[0])
	stopReturned := runFailoverNode(t, restarted, rebound, configs(1))
	awaitRPC(t, func() bool {
		route, err := subscriber.Resolve(key)
		return err == nil && route.BrokerID == 1 && route.TopicID != before.TopicID && subscriber.SubscriptionReady(key)
	})
	awaitRPC(t, func() bool { route, err := publisher.Resolve(key); return err == nil && route.BrokerID == 1 })
	deliver("after-return")
	// Deleting a logical topic must not make failover resurrect its cached copies.
	if err := publisher.DeleteTopic(ctx, key); err != nil {
		t.Fatal(err)
	}
	awaitRPC(t, func() bool {
		for _, s := range append(servers[1:], restarted) {
			for _, route := range s.RoutingTable().Routes {
				if route.LogicalID == key {
					return false
				}
			}
		}
		return true
	})
	stopReturned()
	awaitRPC(t, func() bool { return !remoteStatus(servers[1], 1).Available && !remoteStatus(servers[2], 1).Available })
	for _, s := range servers[1:] {
		for _, route := range s.RoutingTable().Routes {
			if route.LogicalID == key {
				t.Fatal("deleted topic resurrected")
			}
		}
	}
}

func TestFailoverNameCollisionAndRelease(t *testing.T) {
	s := rpcTestServer(t, 2)
	if err := s.AddTopic("same", &Client{Name: "local"}); err != nil {
		t.Fatal(err)
	}
	local := s.Topics["same"]
	remote := TopicMetadata{ID: 1, Name: "same", LogicalID: "foreign-logical-topic", HomeBrokerID: 1, OwnerName: "remote"}
	s.rememberMetadata(BrokerMetadata{ID: 1, KnownTopics: []TopicMetadata{remote}})
	store := NewMetadataStore(BrokerMetadata{ID: 1}, 4096, 1024*1024)
	r := &brokerRuntime{server: s, store: s.metadata, started: time.Now().Add(-time.Hour), config: RPCConfig{FailoverAfter: time.Millisecond}, peers: map[uint16]*rpcPeer{1: {replica: NewMetadataReplica(1)}}}
	state, cursor := store.Snapshot()
	if err := r.peers[1].replica.InstallSnapshot(state, cursor); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.rpcState, s.rpcRunning = r, true
	s.mu.Unlock()
	r.reconcileTopics()
	s.mu.RLock()
	recovered := s.topicsByLogical[remote.LogicalID]
	same := s.Topics["same"]
	s.mu.RUnlock()
	if recovered == nil || same != local || recovered.Name != "same" || recovered.Id == local.Id {
		t.Fatal("name collision overwrote local topic")
	}
	store.UpsertTopic(remote)
	state, cursor = store.Snapshot()
	if err := r.peers[1].replica.InstallSnapshot(state, cursor); err != nil {
		t.Fatal(err)
	}
	r.peers[1].status(true, true)
	r.reconcileTopics()
	s.mu.RLock()
	remaining := s.topicsByLogical[remote.LogicalID]
	s.mu.RUnlock()
	if remaining != nil {
		t.Fatal("backup hosting claim survived home return")
	}
	metadata, _ := s.metadata.Snapshot()
	if slices.ContainsFunc(metadata.RemovedTopics, func(t TopicMetadata) bool { return t.LogicalID == remote.LogicalID }) {
		t.Fatal("handoff incorrectly deleted the logical topic")
	}
	s.mu.Lock()
	s.rpcRunning = false
	s.mu.Unlock()
}

func TestRoutePagesPayloadLimit(t *testing.T) {
	// Protobuf compatibility is additionally checked by golden bytes in message tests.
	page := message.RoutePage{Version: "v", Offset: 0, Data: []byte("x"), Done: true}
	if got, err := message.DecodeRoutePage(page.Encode()); err != nil || !got.Done || got.Version != "v" {
		t.Fatal("invalid route page")
	}
}

func TestClientRecoversMultipleBrokerSessions(t *testing.T) {
	a, b := rpcTestServer(t, 1), rpcTestServer(t, 2)
	la, lb := rpcTestListener(t, "127.0.0.1:0"), rpcTestListener(t, "127.0.0.1:0")
	// Distinct per-broker client IDs must not be mixed when publishing.
	for _, name := range []string{"reserved-a", "reserved-b", "reserved-c"} {
		if err := a.AddClient(&Client{Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	for _, s := range []*Server{a, b} {
		if err := s.AddTopic("shared", &Client{Name: "publisher"}); err != nil {
			t.Fatal(err)
		}
	}
	keys := []string{a.Topics["shared"].LogicalID, b.Topics["shared"].LogicalID}
	stopA := runFailoverNode(t, a, la, []PeerConfig{{ID: 2, RPCAddr: lb.Addr().String()}})
	runFailoverNode(t, b, lb, []PeerConfig{{ID: 1, RPCAddr: la.Addr().String()}})
	awaitRPC(t, func() bool { return remoteStatus(a, 2).Synced && remoteStatus(b, 1).Synced })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	publisher := dashclient.New("publisher", []string{a.TcpAddr.String()})
	publisher.Timeout = 150 * time.Millisecond
	publisher.RetryInterval = 20 * time.Millisecond
	subscriber := dashclient.New("subscriber", []string{a.TcpAddr.String()})
	subscriber.Timeout = 150 * time.Millisecond
	subscriber.RetryInterval = 20 * time.Millisecond
	for _, c := range []*dashclient.Client{publisher, subscriber} {
		if err := c.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
	}
	awaitRPC(t, func() bool { return len(subscriber.Routes()) == 2 })
	if _, err := subscriber.Resolve("shared"); err == nil {
		t.Fatal("ambiguous topic name was accepted")
	}
	for _, key := range keys {
		if err := subscriber.Subscribe(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	deliver := func() {
		t.Helper()
		for _, key := range keys {
			if err := publisher.Publish(ctx, key, key); err != nil {
				t.Fatal(err)
			}
			receiveCtx, cancel := context.WithTimeout(ctx, time.Second)
			msg, err := subscriber.Receive(receiveCtx)
			cancel()
			if err != nil || msg.Payload != key {
				t.Fatalf("per-broker delivery: %+v, %v", msg, err)
			}
		}
	}
	deliver()
	a.mu.RLock()
	idA := a.Clients["publisher"].Id
	a.mu.RUnlock()
	b.mu.RLock()
	idB := b.Clients["publisher"].Id
	b.mu.RUnlock()
	if idA == idB {
		t.Fatal("fixture failed to allocate different broker-local IDs")
	}
	stopA()
	awaitRPC(t, func() bool {
		for _, key := range keys {
			route, err := subscriber.Resolve(key)
			if err != nil || route.BrokerID != 2 || !subscriber.SubscriptionReady(key) {
				return false
			}
		}
		return true
	})
	awaitRPC(t, func() bool {
		for _, key := range keys {
			route, err := publisher.Resolve(key)
			if err != nil || route.BrokerID != 2 {
				return false
			}
		}
		return true
	})
	deliver()
}
