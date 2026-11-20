package server

import (
	"bytes"
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"slices"
	"strings"
	"sync"
)

const MetadataBatchBytes = 16 * 1024

type TopicMetadata struct {
	ID           uint16
	Name         string
	LogicalID    string
	HomeBrokerID uint16
	OwnerName    string
}

type BrokerMetadata struct {
	ID            uint16
	RPCAddr       string
	TCPAddr       string
	UDPAddrs      []string
	Topics        []TopicMetadata
	KnownTopics   []TopicMetadata
	RemovedTopics []TopicMetadata
}

// MetadataCursor identifies the last completely applied revision of one origin.
type MetadataCursor struct {
	Generation string
	Revision   uint64
}

type MetadataEventKind uint8

const (
	BrokerUpdated MetadataEventKind = iota + 1
	TopicUpdated
	TopicRemoved
	TopicReleased
	TopicKnown
	TopicDeleted
)

type MetadataEvent struct {
	Revision uint64
	Kind     MetadataEventKind
	Broker   BrokerMetadata
	Topic    TopicMetadata
}

type metadataRecord struct {
	event MetadataEvent
	size  int
}

// MetadataChunk is a fragment of a Gob-encoded []MetadataEvent.
// After/Through pin the batch during fragmentation; Offset counts encoded bytes.
type MetadataChunk struct {
	Generation   string
	After        uint64
	Through      uint64
	Offset       int
	Data         []byte
	Done         bool
	More         bool
	NeedSnapshot bool
}

type MetadataAck struct {
	Cursor       MetadataCursor
	Offset       int
	NeedSnapshot bool
}

// MetadataStore is independent of RPC. Its bounded journal and authoritative
// catalog can also serve a future client metadata adapter.
type MetadataStore struct {
	mu          sync.RWMutex
	generation  string
	revision    uint64
	broker      BrokerMetadata
	topics      map[uint16]TopicMetadata
	known       map[string]TopicMetadata
	removed     map[string]TopicMetadata
	topicSizes  map[uint16]int
	stateSize   int
	history     []metadataRecord
	historySize int
	maxEvents   int
	maxBytes    int
}

func encodeMetadata(value any) []byte {
	var b bytes.Buffer
	// All callers encode fixed, supported metadata types, never arbitrary values.
	if err := gob.NewEncoder(&b).Encode(value); err != nil {
		panic(fmt.Sprintf("encode metadata schema: %v", err))
	}
	return b.Bytes()
}

func decodeMetadata(data []byte, value any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(value)
}

func cloneBroker(b BrokerMetadata) BrokerMetadata {
	b.UDPAddrs = slices.Clone(b.UDPAddrs)
	b.Topics = slices.Clone(b.Topics)
	b.KnownTopics = slices.Clone(b.KnownTopics)
	b.RemovedTopics = slices.Clone(b.RemovedTopics)
	return b
}

func NewMetadataStore(broker BrokerMetadata, maxEvents, maxBytes int) *MetadataStore {
	if maxEvents <= 0 {
		maxEvents = 4096
	}
	if maxBytes <= 0 {
		maxBytes = 4 * 1024 * 1024
	}
	s := &MetadataStore{
		generation: rand.Text(), topics: make(map[uint16]TopicMetadata),
		topicSizes: make(map[uint16]int), maxEvents: maxEvents, maxBytes: maxBytes,
		known: make(map[string]TopicMetadata), removed: make(map[string]TopicMetadata),
	}
	topics := slices.Clone(broker.Topics)
	known, removed := slices.Clone(broker.KnownTopics), slices.Clone(broker.RemovedTopics)
	broker.Topics = nil
	s.UpdateBroker(broker)
	slices.SortFunc(topics, func(a, b TopicMetadata) int { return int(a.ID) - int(b.ID) })
	for _, topic := range topics {
		s.UpsertTopic(topic)
	}
	s.Remember(known, removed)
	return s
}

func (s *MetadataStore) appendLocked(event MetadataEvent) {
	s.revision++
	event.Revision = s.revision
	size := len(encodeMetadata([]MetadataEvent{event}))
	s.history = append(s.history, metadataRecord{event: event, size: size})
	s.historySize += size
	for len(s.history) > s.maxEvents || s.historySize > s.maxBytes {
		s.historySize -= s.history[0].size
		s.history[0] = metadataRecord{}
		s.history = s.history[1:]
	}
}

func (s *MetadataStore) UpdateBroker(broker BrokerMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	broker = cloneBroker(broker)
	broker.Topics = nil
	broker.KnownTopics, broker.RemovedTopics = nil, nil
	if s.revision > 0 {
		s.stateSize -= len(encodeMetadata(s.broker))
	}
	s.broker = broker
	s.stateSize += len(encodeMetadata(broker))
	s.appendLocked(MetadataEvent{Kind: BrokerUpdated, Broker: broker})
}

func (s *MetadataStore) UpsertTopic(topic TopicMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rememberLocked([]TopicMetadata{topic}, nil)
	s.stateSize -= s.topicSizes[topic.ID]
	s.topics[topic.ID] = topic
	s.topicSizes[topic.ID] = len(encodeMetadata(topic))
	s.stateSize += s.topicSizes[topic.ID]
	s.appendLocked(MetadataEvent{Kind: TopicUpdated, Topic: topic})
}

func (s *MetadataStore) RemoveTopic(id uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	topic := s.topics[id]
	s.rememberLocked(nil, []TopicMetadata{topic})
	s.stateSize -= s.topicSizes[id]
	delete(s.topicSizes, id)
	delete(s.topics, id)
	s.appendLocked(MetadataEvent{Kind: TopicRemoved, Topic: topic})
}

// ReleaseTopic removes a hosting claim without deleting the logical topic.
func (s *MetadataStore) ReleaseTopic(id uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	topic := s.topics[id]
	s.stateSize -= s.topicSizes[id]
	delete(s.topicSizes, id)
	delete(s.topics, id)
	s.appendLocked(MetadataEvent{Kind: TopicReleased, Topic: topic})
}
func (s *MetadataStore) Remember(known, removed []TopicMetadata) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rememberLocked(known, removed)
}
func (s *MetadataStore) rememberLocked(known, removed []TopicMetadata) bool {
	changed := false
	for _, topic := range known {
		if topic.LogicalID == "" {
			continue
		}
		if _, ok := s.known[topic.LogicalID]; ok {
			continue
		}
		s.known[topic.LogicalID] = topic
		s.stateSize += len(encodeMetadata(topic))
		s.appendLocked(MetadataEvent{Kind: TopicKnown, Topic: topic})
		changed = true
	}
	for _, topic := range removed {
		if topic.LogicalID == "" {
			continue
		}
		if _, ok := s.removed[topic.LogicalID]; ok {
			continue
		}
		s.removed[topic.LogicalID] = topic
		s.stateSize += len(encodeMetadata(topic))
		s.appendLocked(MetadataEvent{Kind: TopicDeleted, Topic: topic})
		changed = true
	}
	return changed
}

func (s *MetadataStore) Snapshot() (BrokerMetadata, MetadataCursor) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	broker := cloneBroker(s.broker)
	broker.Topics = make([]TopicMetadata, 0, len(s.topics))
	for _, topic := range s.topics {
		broker.Topics = append(broker.Topics, topic)
	}
	for _, topic := range s.known {
		broker.KnownTopics = append(broker.KnownTopics, topic)
	}
	for _, topic := range s.removed {
		broker.RemovedTopics = append(broker.RemovedTopics, topic)
	}
	slices.SortFunc(broker.KnownTopics, func(a, b TopicMetadata) int { return strings.Compare(a.LogicalID, b.LogicalID) })
	slices.SortFunc(broker.RemovedTopics, func(a, b TopicMetadata) int { return strings.Compare(a.LogicalID, b.LogicalID) })
	slices.SortFunc(broker.Topics, func(a, b TopicMetadata) int { return int(a.ID) - int(b.ID) })
	return broker, MetadataCursor{Generation: s.generation, Revision: s.revision}
}

func (s *MetadataStore) Cursor() MetadataCursor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return MetadataCursor{Generation: s.generation, Revision: s.revision}
}

func (s *MetadataStore) canReplayLocked(cursor MetadataCursor) bool {
	return cursor.Generation == s.generation && cursor.Revision <= s.revision &&
		(cursor.Revision == s.revision || len(s.history) > 0 && s.history[0].event.Revision <= cursor.Revision+1)
}

func (s *MetadataStore) CanReplay(cursor MetadataCursor) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.canReplayLocked(cursor)
}

// Changes returns a size-bounded batch; Through and Offset resume a fragment.
func (s *MetadataStore) Changes(cursor MetadataCursor, through uint64, offset int) MetadataChunk {
	s.mu.RLock()
	defer s.mu.RUnlock()
	chunk := MetadataChunk{Generation: s.generation, After: cursor.Revision}
	if !s.canReplayLocked(cursor) || offset < 0 {
		chunk.NeedSnapshot = true
		return chunk
	}
	if cursor.Revision == s.revision {
		chunk.Through, chunk.Done = cursor.Revision, true
		return chunk
	}
	pendingSize := 0
	for _, record := range s.history {
		if record.event.Revision > cursor.Revision {
			pendingSize += record.size
		}
	}
	if through == 0 && pendingSize >= s.stateSize {
		chunk.NeedSnapshot = true
		return chunk
	}
	var events []MetadataEvent
	size := 0
	for _, record := range s.history {
		if record.event.Revision <= cursor.Revision {
			continue
		}
		if through != 0 && record.event.Revision > through {
			break
		}
		if through == 0 && len(events) > 0 && size+record.size > MetadataBatchBytes/2 {
			break
		}
		events = append(events, record.event)
		size += record.size
	}
	if len(events) == 0 || through != 0 && events[len(events)-1].Revision != through {
		chunk.NeedSnapshot = true
		return chunk
	}
	data := encodeMetadata(events)
	if offset > len(data) {
		chunk.NeedSnapshot = true
		return chunk
	}
	chunk.Through, chunk.Offset = events[len(events)-1].Revision, offset
	chunk.More = chunk.Through < s.revision
	end := min(len(data), offset+MetadataBatchBytes)
	for {
		chunk.Data = data[offset:end]
		chunk.Done = end == len(data)
		if excess := len(encodeMetadata(chunk)) - MetadataBatchBytes; excess > 0 {
			end -= excess
			continue
		}
		break
	}
	chunk.Data = slices.Clone(chunk.Data)
	return chunk
}

type metadataPartial struct {
	generation     string
	after, through uint64
	data           []byte
}

// MetadataReplica applies one origin's ordered events or atomically installs a
// complete snapshot. It does not write received events into the local journal.
type MetadataReplica struct {
	mu      sync.RWMutex
	id      uint16
	cursor  MetadataCursor
	broker  BrokerMetadata
	partial metadataPartial
	retired map[string]bool
}

func NewMetadataReplica(id uint16) *MetadataReplica {
	return &MetadataReplica{id: id, broker: BrokerMetadata{ID: id}, retired: make(map[string]bool)}
}

func (r *MetadataReplica) Snapshot() (BrokerMetadata, MetadataCursor) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneBroker(r.broker), r.cursor
}

func (r *MetadataReplica) Cursor() MetadataCursor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cursor
}

func (r *MetadataReplica) Apply(chunk MetadataChunk) (MetadataAck, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ack := MetadataAck{Cursor: r.cursor}
	if chunk.NeedSnapshot {
		ack.NeedSnapshot = true
		return ack, nil
	}
	if chunk.Generation == "" || r.retired[chunk.Generation] {
		return ack, fmt.Errorf("geração de metadata inválida ou substituída")
	}
	if chunk.Generation == r.cursor.Generation && chunk.Through <= r.cursor.Revision {
		return ack, nil
	}
	base := r.cursor.Revision
	if chunk.Generation != r.cursor.Generation {
		base = 0
	}
	if chunk.After != base || chunk.Through <= chunk.After || chunk.Offset < 0 {
		ack.NeedSnapshot = true
		return ack, nil
	}
	if chunk.Offset == 0 {
		r.partial = metadataPartial{generation: chunk.Generation, after: chunk.After, through: chunk.Through}
	}
	p := &r.partial
	if p.generation != chunk.Generation || p.after != chunk.After || p.through != chunk.Through || chunk.Offset > len(p.data) {
		ack.NeedSnapshot = true
		return ack, nil
	}
	if chunk.Offset < len(p.data) {
		ack.Offset = len(p.data)
		return ack, nil
	}
	p.data = append(p.data, chunk.Data...)
	if !chunk.Done {
		ack.Offset = len(p.data)
		return ack, nil
	}
	var events []MetadataEvent
	if err := decodeMetadata(p.data, &events); err != nil {
		r.partial = metadataPartial{}
		return ack, err
	}
	if uint64(len(events)) != chunk.Through-chunk.After {
		return ack, fmt.Errorf("sequência incompleta de eventos")
	}
	broker := cloneBroker(r.broker)
	if chunk.Generation != r.cursor.Generation {
		broker = BrokerMetadata{ID: r.id}
	}
	topics := make(map[uint16]TopicMetadata, len(broker.Topics))
	known, removed := make(map[string]TopicMetadata), make(map[string]TopicMetadata)
	for _, topic := range broker.KnownTopics {
		known[topic.LogicalID] = topic
	}
	for _, topic := range broker.RemovedTopics {
		removed[topic.LogicalID] = topic
	}
	for _, topic := range broker.Topics {
		topics[topic.ID] = topic
	}
	for i, event := range events {
		if event.Revision != chunk.After+uint64(i)+1 {
			return ack, fmt.Errorf("lacuna de revisões")
		}
		switch event.Kind {
		case BrokerUpdated:
			if event.Broker.ID != r.id || len(event.Broker.Topics) != 0 {
				return ack, fmt.Errorf("origem de evento inválida")
			}
			broker = cloneBroker(event.Broker)
		case TopicUpdated:
			if event.Topic.ID == 0 {
				return ack, fmt.Errorf("ID de tópico inválido")
			}
			topics[event.Topic.ID] = event.Topic
		case TopicRemoved, TopicReleased:
			delete(topics, event.Topic.ID)
		case TopicKnown:
			known[event.Topic.LogicalID] = event.Topic
		case TopicDeleted:
			removed[event.Topic.LogicalID] = event.Topic
		default:
			return ack, fmt.Errorf("tipo de evento inválido")
		}
	}
	broker.Topics = make([]TopicMetadata, 0, len(topics))
	broker.KnownTopics, broker.RemovedTopics = nil, nil
	for _, topic := range known {
		broker.KnownTopics = append(broker.KnownTopics, topic)
	}
	for _, topic := range removed {
		broker.RemovedTopics = append(broker.RemovedTopics, topic)
	}
	for _, topic := range topics {
		broker.Topics = append(broker.Topics, topic)
	}
	slices.SortFunc(broker.Topics, func(a, b TopicMetadata) int { return int(a.ID) - int(b.ID) })
	r.installLocked(broker, MetadataCursor{Generation: chunk.Generation, Revision: chunk.Through})
	ack.Cursor = r.cursor
	return ack, nil
}

func (r *MetadataReplica) installLocked(broker BrokerMetadata, cursor MetadataCursor) {
	if r.cursor.Generation != "" && r.cursor.Generation != cursor.Generation {
		r.retired[r.cursor.Generation] = true
	}
	r.broker, r.cursor, r.partial = cloneBroker(broker), cursor, metadataPartial{}
}

func (r *MetadataReplica) InstallSnapshot(broker BrokerMetadata, cursor MetadataCursor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if broker.ID != r.id || cursor.Generation == "" || r.retired[cursor.Generation] {
		return fmt.Errorf("snapshot de origem ou geração inválida")
	}
	if cursor.Generation == r.cursor.Generation && cursor.Revision <= r.cursor.Revision {
		return nil
	}
	seen := make(map[uint16]bool)
	for _, topic := range broker.Topics {
		if topic.ID == 0 || seen[topic.ID] {
			return fmt.Errorf("tópicos inválidos no snapshot")
		}
		seen[topic.ID] = true
	}
	r.installLocked(broker, cursor)
	return nil
}
