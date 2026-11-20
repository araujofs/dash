package server

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"testing"
	"time"
)

func rpcTestListener(t *testing.T, address string) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func rpcTestServer(t *testing.T, id uint16) *Server {
	t.Helper()
	s, err := NewServer(id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.TcpListener.Close()
		for _, c := range s.UdpConfig {
			c.udpConn.Close()
		}
	})
	return s
}

func awaitRPC(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("RPC did not converge")
		case <-ticker.C:
		}
	}
}

func startRPC(t *testing.T, s *Server, l net.Listener, peers []PeerConfig, interval time.Duration) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.serveRPC(ctx, l, RPCConfig{Peers: peers, SyncInterval: interval, Timeout: 300 * time.Millisecond})
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("RPC shutdown: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Error("RPC goroutines did not stop")
			}
		})
	}
	t.Cleanup(stop)
	awaitRPC(t, func() bool { return s.RPCAddress() != "" })
	return stop
}

func remoteStatus(s *Server, id uint16) BrokerStatus {
	for _, status := range s.ClusterMetadata() {
		if status.Metadata.ID == id {
			return status
		}
	}
	return BrokerStatus{}
}

func TestBrokerRPCImmediateUpdatesAndShutdown(t *testing.T) {
	a, b := rpcTestServer(t, 1), rpcTestServer(t, 2)
	la, lb := rpcTestListener(t, "127.0.0.1:0"), rpcTestListener(t, "127.0.0.1:0")
	ownerA, ownerB := &Client{Name: "a"}, &Client{Name: "b"}
	if err := a.AddTopic("local-a", ownerA); err != nil {
		t.Fatal(err)
	}
	if err := b.AddTopic("local-b", ownerB); err != nil {
		t.Fatal(err)
	}
	// An hour-long reconciliation interval makes these tests depend on immediate notifications.
	stopA := startRPC(t, a, la, []PeerConfig{{ID: 2, RPCAddr: lb.Addr().String()}}, time.Hour)
	stopB := startRPC(t, b, lb, []PeerConfig{{ID: 1, RPCAddr: la.Addr().String()}}, time.Hour)
	awaitRPC(t, func() bool { return remoteStatus(a, 2).Synced && remoteStatus(b, 1).Synced })
	if remoteStatus(a, 2).Metadata.Topics[0].Name != "local-b" || a.Topics["local-a"].Id != b.Topics["local-b"].Id {
		t.Fatal("broker namespaces were mixed")
	}
	if len(a.Topics) != 1 || len(b.Topics) != 1 {
		t.Fatal("remote topics leaked into local indexes")
	}
	if err := a.AddTopic("new", ownerA); err != nil {
		t.Fatal(err)
	}
	awaitRPC(t, func() bool { return len(remoteStatus(b, 1).Metadata.Topics) == 2 })
	if err := a.RemoveTopic("local-a", ownerA); err != nil {
		t.Fatal(err)
	}
	awaitRPC(t, func() bool {
		state := remoteStatus(b, 1)
		return len(state.Metadata.Topics) == 1 && state.Metadata.Topics[0].Name == "new"
	})
	client, err := rpc.Dial("tcp", la.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var identity BrokerIdentity
	if err := client.Call("Broker.Ping", PeerRequest{BrokerID: 2}, &identity); err != nil {
		t.Fatal(err)
	}
	if identity.ID != 1 || identity.Cursor.Generation == "" {
		t.Fatal("invalid ping identity")
	}
	if err := a.ListenRPC(context.Background(), RPCConfig{}); err == nil {
		t.Fatal("second RPC listener was accepted")
	}
	if err := client.Call("Broker.Ping", PeerRequest{BrokerID: 99}, &identity); err == nil {
		t.Fatal("unknown peer accepted")
	}
	stopA() // An idle accepted RPC connection must not prevent shutdown.
	client.Close()
	stopB()
	if a.RPCAddress() != "" {
		t.Fatal("stopped listener is still advertised as active")
	}
}

func TestBrokerRPCFailureAndRestart(t *testing.T) {
	a, b := rpcTestServer(t, 1), rpcTestServer(t, 2)
	la, lb := rpcTestListener(t, "127.0.0.1:0"), rpcTestListener(t, "127.0.0.1:0")
	aAddr, bAddr := la.Addr().String(), lb.Addr().String()
	owner := &Client{Name: "owner"}
	if err := b.AddTopic("old", owner); err != nil {
		t.Fatal(err)
	}
	startRPC(t, a, la, []PeerConfig{{ID: 2, RPCAddr: bAddr}}, 20*time.Millisecond)
	stopB := startRPC(t, b, lb, []PeerConfig{{ID: 1, RPCAddr: aAddr}}, 20*time.Millisecond)
	awaitRPC(t, func() bool { return remoteStatus(a, 2).Synced })
	oldGeneration := remoteStatus(a, 2).Cursor.Generation
	stopB()
	awaitRPC(t, func() bool { return !remoteStatus(a, 2).Available })
	if len(remoteStatus(a, 2).Metadata.Topics) != 1 {
		t.Fatal("failure discarded last known catalog")
	}
	if err := a.AddTopic("while-offline", owner); err != nil {
		t.Fatal(err)
	}
	restarted := rpcTestServer(t, 2)
	if err := restarted.AddTopic("fresh", owner); err != nil {
		t.Fatal(err)
	}
	newListener := rpcTestListener(t, bAddr)
	startRPC(t, restarted, newListener, []PeerConfig{{ID: 1, RPCAddr: aAddr}}, 20*time.Millisecond)
	awaitRPC(t, func() bool {
		state := remoteStatus(a, 2)
		return state.Synced && state.Cursor.Generation != oldGeneration && len(state.Metadata.Topics) == 1 && state.Metadata.Topics[0].Name == "fresh"
	})
	awaitRPC(t, func() bool { return len(remoteStatus(restarted, 1).Metadata.Topics) == 1 })
}

func TestRPCSnapshotPagesAndConcurrentChanges(t *testing.T) {
	store := metadataFixture(100, 500)
	r := &brokerRuntime{server: &Server{Id: 1}, store: store, config: RPCConfig{SnapshotTTL: time.Minute}, peers: map[uint16]*rpcPeer{2: {replica: NewMetadataReplica(2), wake: make(chan struct{}, 1)}}, snapshots: make(map[uint16]*snapshotSession)}
	service := &BrokerRPC{runtime: r}
	var info SnapshotInfo
	if err := service.BeginSnapshot(PeerRequest{2}, &info); err != nil {
		t.Fatal(err)
	}
	store.UpsertTopic(TopicMetadata{ID: 101, Name: "created-during-snapshot"})
	var data []byte
	pages := 0
	for {
		var chunk SnapshotChunk
		if err := service.ReadSnapshot(SnapshotRequest{2, info.ID, len(data)}, &chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Expired || len(encodeMetadata(chunk)) > MetadataBatchBytes {
			t.Fatal("snapshot page expired or exceeded byte budget")
		}
		data = append(data, chunk.Data...)
		pages++
		if chunk.Done {
			break
		}
	}
	var base BrokerMetadata
	if err := decodeMetadata(data, &base); err != nil {
		t.Fatal(err)
	}
	if pages < 2 || len(base.Topics) != 100 {
		t.Fatal("snapshot was not pinned to one revision")
	}
	replica := NewMetadataReplica(1)
	if err := replica.InstallSnapshot(base, info.Cursor); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Apply(store.Changes(info.Cursor, 0, 0)); err != nil {
		t.Fatal(err)
	}
	final, _ := replica.Snapshot()
	if len(final.Topics) != 101 {
		t.Fatal("changes after snapshot were lost")
	}
	r.snapshots[2].lastRead = time.Now().Add(-2 * time.Minute)
	var expired SnapshotChunk
	if err := service.ReadSnapshot(SnapshotRequest{2, info.ID, 0}, &expired); err != nil || !expired.Expired {
		t.Fatal("idle snapshot was not expired")
	}
	if err := service.BeginSnapshot(PeerRequest{2}, &info); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.maxEvents = 1
	store.mu.Unlock()
	store.RemoveTopic(1)
	store.RemoveTopic(2)
	if err := service.ReadSnapshot(SnapshotRequest{2, info.ID, 0}, &expired); err != nil || !expired.Expired {
		t.Fatal("snapshot with expired follow-up history was not invalidated")
	}
}

func TestRPCConfigurationAndTimeout(t *testing.T) {
	for _, config := range []RPCConfig{
		{Peers: []PeerConfig{{ID: 0, RPCAddr: "127.0.0.1:1"}}},
		{Peers: []PeerConfig{{ID: 1, RPCAddr: "127.0.0.1:1"}}},
		{Peers: []PeerConfig{{ID: 2, RPCAddr: "127.0.0.1:1"}, {ID: 2, RPCAddr: "127.0.0.1:2"}}},
		{Peers: []PeerConfig{{ID: 2, RPCAddr: "127.0.0.1:0"}}},
		{Timeout: -time.Second},
	} {
		if _, err := normalizeRPCConfig(1, config); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	l := rpcTestListener(t, "127.0.0.1:0")
	r := &brokerRuntime{config: RPCConfig{Timeout: 30 * time.Millisecond}}
	peer := &rpcPeer{config: PeerConfig{ID: 2, RPCAddr: l.Addr().String()}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var identity BrokerIdentity
	start := time.Now()
	if err := r.call(ctx, peer, "Ping", PeerRequest{1}, &identity); err == nil {
		t.Fatal("unresponsive RPC server did not time out")
	}
	if time.Since(start) > time.Second {
		t.Fatal("RPC timeout exceeded deadline")
	}
	if _, err := rpcAdvertiseAddress(l, ":1234"); err == nil {
		t.Fatal("empty advertised host accepted")
	}
	if _, err := rpcAdvertiseAddress(l, "0.0.0.0:1234"); err == nil {
		t.Fatal("wildcard advertised host accepted")
	}
	if address, err := rpcAdvertiseAddress(l, "localhost:0"); err != nil || !strings.HasPrefix(address, "localhost:") {
		t.Fatal("ephemeral advertised port not resolved")
	}
}

func TestRPCIdentityMismatch(t *testing.T) {
	server := rpcTestServer(t, 2)
	listener := rpcTestListener(t, "127.0.0.1:0")
	startRPC(t, server, listener, []PeerConfig{{ID: 1, RPCAddr: "127.0.0.1:1"}}, time.Hour)
	local := &Server{Id: 1}
	runtime := &brokerRuntime{server: local, config: RPCConfig{Timeout: time.Second}}
	peer := &rpcPeer{config: PeerConfig{ID: 3, RPCAddr: listener.Addr().String()}, replica: NewMetadataReplica(3)}
	if err := runtime.syncRound(context.Background(), peer); err == nil {
		t.Fatal("unexpected broker identity was accepted")
	}
	if peer.replica.Cursor().Generation != "" {
		t.Fatal("identity mismatch changed remote catalog")
	}
}
