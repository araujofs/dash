package server

import (
	"dash/message"
	"slices"
	"testing"
	"time"
)

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
