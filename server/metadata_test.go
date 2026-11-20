package server

import (
	"fmt"
	"strings"
	"testing"
)

func metadataFixture(count int, nameSize int) *MetadataStore {
	s := NewMetadataStore(BrokerMetadata{ID: 1, RPCAddr: "127.0.0.1:9001"}, 4096, 4*1024*1024)
	for i := 1; i <= count; i++ {
		s.UpsertTopic(TopicMetadata{ID: uint16(i), Name: fmt.Sprintf("topic-%d-", i) + strings.Repeat("x", nameSize)})
	}
	return s
}

func installedReplica(t *testing.T, s *MetadataStore) *MetadataReplica {
	t.Helper()
	r := NewMetadataReplica(1)
	b, cursor := s.Snapshot()
	if err := r.InstallSnapshot(b, cursor); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestMetadataDeltasAndDuplicates(t *testing.T) {
	s := metadataFixture(100, 30)
	r := installedReplica(t, s)
	base := r.Cursor()
	s.UpsertTopic(TopicMetadata{ID: 1, Name: "changed"})
	s.RemoveTopic(2)
	chunk := s.Changes(base, 0, 0)
	if chunk.NeedSnapshot || chunk.After != base.Revision || chunk.Through != base.Revision+2 || !chunk.Done {
		t.Fatalf("expected only two new events: %+v", chunk)
	}
	var events []MetadataEvent
	if err := decodeMetadata(chunk.Data, &events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Kind != TopicUpdated || events[1].Kind != TopicRemoved {
		t.Fatal("incorrect delta history")
	}
	for range 2 {
		ack, err := r.Apply(chunk)
		if err != nil || ack.NeedSnapshot || ack.Cursor.Revision != chunk.Through {
			t.Fatalf("apply: %+v, %v", ack, err)
		}
	}
	b, _ := r.Snapshot()
	if len(b.Topics) != 99 || b.Topics[0].Name != "changed" || b.Topics[1].ID != 3 {
		t.Fatal("incorrect resulting catalog")
	}
	gap := MetadataChunk{Generation: base.Generation, After: chunk.Through + 1, Through: chunk.Through + 2}
	ack, err := r.Apply(gap)
	if err != nil || !ack.NeedSnapshot || r.Cursor().Revision != chunk.Through {
		t.Fatal("gap advanced replica cursor")
	}
	b.Topics[0].Name = "external mutation"
	copy, _ := r.Snapshot()
	if copy.Topics[0].Name != "changed" {
		t.Fatal("catalog accessor leaked mutable state")
	}
}

func TestMetadataFragmentation(t *testing.T) {
	s := metadataFixture(100, 600)
	r := installedReplica(t, s)
	base := r.Cursor()
	large := strings.Repeat("long-topic-", 5000)
	s.UpsertTopic(TopicMetadata{ID: 101, Name: large})
	through, offset, fragments := uint64(0), 0, 0
	for {
		chunk := s.Changes(base, through, offset)
		if chunk.NeedSnapshot || len(encodeMetadata(chunk)) > MetadataBatchBytes {
			t.Fatal("fragment exceeded budget or unexpectedly required a snapshot")
		}
		ack, err := r.Apply(chunk)
		if err != nil || ack.NeedSnapshot {
			t.Fatalf("fragment: %+v, %v", ack, err)
		}
		fragments++
		if chunk.Done {
			break
		}
		if r.Cursor() != base {
			t.Fatal("partial event changed public cursor")
		}
		// A retry of a fragment must not append its bytes twice.
		retry, err := r.Apply(chunk)
		if err != nil || retry.Offset != ack.Offset {
			t.Fatal("fragment retry was not idempotent")
		}
		through, offset = chunk.Through, ack.Offset
	}
	b, cursor := r.Snapshot()
	if fragments < 2 || len(b.Topics) != 101 || b.Topics[100].Name != large || cursor.Revision != base.Revision+1 {
		t.Fatal("fragmented event was not reassembled exactly")
	}
}

func TestMetadataRetentionAndNewGeneration(t *testing.T) {
	s := metadataFixture(10, 10)
	r := installedReplica(t, s)
	oldCursor := r.Cursor()
	s.mu.Lock()
	s.maxEvents = 2
	s.mu.Unlock()
	for i := 1; i <= 4; i++ {
		s.RemoveTopic(uint16(i))
	}
	if !s.Changes(oldCursor, 0, 0).NeedSnapshot {
		t.Fatal("expired history did not require snapshot")
	}
	if len(s.history) > 2 {
		t.Fatal("event retention exceeded limit")
	}
	b, cursor := s.Snapshot()
	if err := r.InstallSnapshot(b, cursor); err != nil {
		t.Fatal(err)
	}
	if len(b.Topics) != 6 {
		t.Fatal("snapshot did not remove stale entries")
	}
	restarted := NewMetadataStore(BrokerMetadata{ID: 1}, 1, 1)
	newState, newCursor := restarted.Snapshot()
	if err := r.InstallSnapshot(newState, newCursor); err != nil {
		t.Fatal(err)
	}
	if err := r.InstallSnapshot(b, cursor); err == nil {
		t.Fatal("retired generation was accepted")
	}
	if len(restarted.history) != 0 || restarted.historySize != 0 {
		t.Fatal("byte retention exceeded limit")
	}
	if !restarted.Changes(MetadataCursor{}, 0, 0).NeedSnapshot {
		t.Fatal("cold replica must receive snapshot")
	}
}

func TestMetadataChoosesSmallerSnapshot(t *testing.T) {
	s := metadataFixture(2, 5)
	r := installedReplica(t, s)
	base := r.Cursor()
	for range 30 {
		s.UpsertTopic(TopicMetadata{ID: 1, Name: "same-final-state"})
	}
	if !s.Changes(base, 0, 0).NeedSnapshot {
		t.Fatal("large backlog should prefer smaller current state")
	}
}
