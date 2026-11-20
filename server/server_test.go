package server

import (
	"dash/consts"
	"strconv"
	"strings"
	"testing"
)

func TestClientManagement(t *testing.T) {
	s := &Server{}
	first := &Client{Id: 99, Name: "first"}
	second := &Client{Id: 99, Name: "second"}
	for _, client := range []*Client{first, second} {
		if err := s.AddClient(client); err != nil {
			t.Fatal(err)
		}
		if s.Clients[client.Name] != client {
			t.Fatalf("client %s was not stored under its name", client.Name)
		}
		if s.clientsByID[client.Id] != client {
			t.Fatalf("client %d was not indexed by ID", client.Id)
		}
	}
	if first.Id != 1 || second.Id != 2 || s.ClientSeq.current != 2 {
		t.Fatal("clients did not receive sequential IDs starting at 1")
	}
	duplicate := &Client{Id: 77, Name: first.Name}
	if err := s.AddClient(duplicate); err == nil {
		t.Fatal("expected an error for a duplicate client name")
	}
	if s.Clients[first.Name] != first || len(s.Clients) != 2 || s.ClientSeq.current != 2 || len(s.ClientSeq.usedIDs) != 2 || duplicate.Id != 77 {
		t.Fatal("duplicate addition changed registered clients")
	}
	if err := s.RemoveClient(first.Name); err != nil {
		t.Fatal(err)
	}
	if _, exists := s.Clients[first.Name]; exists {
		t.Fatal("removed client is still registered")
	}
	if _, exists := s.clientsByID[first.Id]; exists {
		t.Fatal("removed client is still indexed by ID")
	}
	if _, exists := s.ClientSeq.usedIDs[first.Id]; exists {
		t.Fatal("removed client ID is still occupied")
	}
	if err := s.RemoveClient(first.Name); err == nil {
		t.Fatal("expected an error for an unregistered client name")
	}
	if len(s.Clients) != 1 || s.Clients[second.Name] != second {
		t.Fatal("removal changed another client")
	}
	s.ClientSeq.current = ^uint64(0)
	replacement := &Client{Name: first.Name}
	if err := s.AddClient(replacement); err != nil {
		t.Fatal(err)
	}
	if s.Clients[first.Name] != replacement || replacement.Id != 1 {
		t.Fatal("removed client name and ID could not be reused")
	}
	if s.clientsByID[replacement.Id] != replacement {
		t.Fatal("reused client ID still references the removed client")
	}
	for _, client := range s.Clients {
		if _, exists := s.ClientSeq.usedIDs[client.Id]; !exists {
			t.Fatalf("client ID %d was not marked as occupied", client.Id)
		}
	}
}

func TestClientManagementEmptyServer(t *testing.T) {
	s := &Server{}
	if err := s.AddClient(nil); err == nil {
		t.Fatal("expected an error for a nil client")
	}
	if err := s.AddClient(&Client{}); err == nil {
		t.Fatal("expected an error for an empty client name")
	}
	if err := s.RemoveClient("missing"); err == nil {
		t.Fatal("expected an error for an unregistered client name")
	}
	if len(s.Clients) != 0 || s.ClientSeq != nil {
		t.Fatal("failed operations changed registered clients")
	}
}

func TestClientNameFitsDataHeader(t *testing.T) {
	s := &Server{}
	if err := s.AddClient(&Client{Name: strings.Repeat("a", 255)}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddClient(&Client{Name: strings.Repeat("b", 256)}); err == nil {
		t.Fatal("expected an error for a name exceeding the one-byte UDP name length")
	}
	if len(s.Clients) != 1 || s.ClientSeq.current != 1 {
		t.Fatal("rejected name changed registered clients or sequence")
	}
}

func TestClientIDWrap(t *testing.T) {
	s := &Server{}
	for _, name := range []string{"first", "second"} {
		if err := s.AddClient(&Client{Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	s.ClientSeq.current = ^uint64(0) - 1
	last := &Client{Name: "last"}
	if err := s.AddClient(last); err != nil {
		t.Fatal(err)
	}
	if last.Id != ^uint64(0) {
		t.Fatal("last uint64 ID was not assigned")
	}
	wrapped := &Client{Name: "wrapped"}
	if err := s.AddClient(wrapped); err != nil {
		t.Fatal(err)
	}
	if wrapped.Id != 3 || s.ClientSeq.current != 3 {
		t.Fatal("wrap did not skip zero and occupied client IDs")
	}
	if s.TopicSeq != nil {
		t.Fatal("client allocation changed topic allocation state")
	}
}

func TestAddTopicCapacityAndReuse(t *testing.T) {
	owner := &Client{Name: "owner"}
	s := &Server{Topics: make(map[string]*Topic), topicsByID: make(map[uint16]*Topic), TopicSeq: NewIDSequence(uint16(consts.MAX_TOPICS - 1))}
	for id := 1; id < consts.MAX_TOPICS; id++ {
		allocated, err := s.TopicSeq.Next()
		if err != nil {
			t.Fatal(err)
		}
		name := strconv.Itoa(id)
		topic := newTopic(allocated, name, owner)
		s.Topics[name] = topic
		s.topicsByID[allocated] = topic
	}
	if err := s.AddTopic("overflow", owner); err == nil {
		t.Fatal("expected an error when all IDs are occupied")
	}
	if len(s.Topics) != consts.MAX_TOPICS-1 || len(s.TopicSeq.usedIDs) != consts.MAX_TOPICS-1 || s.TopicSeq.current != consts.MAX_TOPICS-1 {
		t.Fatal("failed addition changed topics or sequence")
	}
	if err := s.RemoveTopic("42", owner); err != nil {
		t.Fatal(err)
	}
	if _, exists := s.TopicSeq.usedIDs[42]; exists {
		t.Fatal("removed topic ID is still occupied")
	}
	if _, exists := s.topicsByID[42]; exists {
		t.Fatal("removed topic is still indexed by ID")
	}
	if err := s.AddTopic("replacement", owner); err != nil {
		t.Fatal(err)
	}
	if topic := s.Topics["replacement"]; topic.Id != 42 || topic.Name != "replacement" || topic.Owner != owner || s.topicsByID[42] != topic {
		t.Fatalf("replacement topic = %+v, want ID 42 and original owner", topic)
	}
	if _, exists := s.TopicSeq.usedIDs[42]; !exists {
		t.Fatal("replacement topic ID was not marked as occupied")
	}
	if err := s.AddTopic("replacement", owner); err == nil {
		t.Fatal("expected an error for a duplicate name")
	}
	if s.TopicSeq.current != 42 || len(s.Topics) != consts.MAX_TOPICS-1 || len(s.topicsByID) != consts.MAX_TOPICS-1 || len(s.TopicSeq.usedIDs) != consts.MAX_TOPICS-1 {
		t.Fatal("duplicate addition changed topics or sequence")
	}
}

func TestTopicIDsStayInSync(t *testing.T) {
	owner := &Client{Name: "owner"}
	s := &Server{Topics: make(map[string]*Topic), TopicSeq: NewIDSequence(uint16(consts.MAX_TOPICS - 1))}
	if err := s.AddTopic("first", owner); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTopic("second", owner); err != nil {
		t.Fatal(err)
	}
	if s.Topics["first"].Id != 1 || s.Topics["second"].Id != 2 {
		t.Fatal("topics did not receive distinct sequential IDs")
	}
	if err := s.AddTopic("first", owner); err == nil {
		t.Fatal("expected an error for a duplicate name")
	}
	if err := s.RemoveTopic("first", &Client{Name: "other"}); err == nil {
		t.Fatal("expected an error for a different owner")
	}
	if err := s.RemoveTopic("missing", owner); err == nil {
		t.Fatal("expected an error for a missing topic")
	}
	if len(s.Topics) != 2 || len(s.topicsByID) != 2 || len(s.TopicSeq.usedIDs) != 2 || s.TopicSeq.current != 2 {
		t.Fatal("failed operations changed topics, occupied IDs or sequence")
	}
	for name, topic := range s.Topics {
		if topic.Name != name || s.topicsByID[topic.Id] != topic {
			t.Fatalf("topic %q is inconsistent between name and ID indexes", name)
		}
		if _, exists := s.TopicSeq.usedIDs[topic.Id]; !exists {
			t.Fatalf("topic ID %d was not marked as occupied", topic.Id)
		}
	}
}
