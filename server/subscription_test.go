package server

import "testing"

func TestTopicSubscriptions(t *testing.T) {
	s := &Server{}
	owner := &Client{Name: "owner"}
	first := &Client{Name: "first"}
	second := &Client{Name: "second"}
	for _, client := range []*Client{owner, first, second} {
		if err := s.AddClient(client); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AddTopic("updates", owner); err != nil {
		t.Fatal(err)
	}
	topic := s.Topics["updates"]
	for _, client := range []*Client{owner, first, second} {
		if err := s.AddClientToTopic("updates", client.Name); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AddClientToTopic("updates", first.Name); err == nil {
		t.Fatal("expected an error for a duplicate subscription")
	}
	if len(topic.Clients) != 3 {
		t.Fatal("duplicate subscription changed recipients")
	}
	if err := s.RemoveClientFromTopic("updates", first.Name); err != nil {
		t.Fatal(err)
	}
	if len(topic.Clients) != 2 || topic.Clients[0] != owner || topic.Clients[1] != second {
		t.Fatal("unsubscribe did not preserve the other recipients")
	}
	if err := s.RemoveClientFromTopic("updates", first.Name); err == nil {
		t.Fatal("expected an error for a client who is not subscribed")
	}
	if s.Clients[first.Name] != first {
		t.Fatal("unsubscribe removed the client from the server")
	}
	if err := s.AddClientToTopic("updates", first.Name); err != nil {
		t.Fatal(err)
	}
	for _, client := range []*Client{owner, first, second} {
		if err := s.RemoveClientFromTopic("updates", client.Name); err != nil {
			t.Fatal(err)
		}
	}
	if len(topic.Clients) != 0 || topic.Owner != owner || s.Topics["updates"] != topic {
		t.Fatal("unsubscribe changed topic ownership or left recipients behind")
	}
}

func TestTopicSubscriptionErrors(t *testing.T) {
	s := &Server{}
	client := &Client{Name: "client"}
	if err := s.AddClient(client); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTopic("updates", client); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []struct {
		name string
		fn   func(string, string) error
	}{
		{name: "subscribe", fn: s.AddClientToTopic},
		{name: "unsubscribe", fn: s.RemoveClientFromTopic},
	} {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.fn("missing", client.Name); err == nil {
				t.Fatal("expected an error for a missing topic")
			}
			if err := operation.fn("updates", "missing"); err == nil {
				t.Fatal("expected an error for a missing client")
			}
			if len(s.Topics["updates"].Clients) != 0 {
				t.Fatal("failed operation changed recipients")
			}
		})
	}
}
