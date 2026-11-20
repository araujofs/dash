package server

import (
	"crypto/sha256"
	"dash/message"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"
)

func (s *Server) rememberMetadata(metadata BrokerMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	known := append(slices.Clone(metadata.KnownTopics), metadata.Topics...)
	if s.metadataLocked().Remember(known, metadata.RemovedTopics) {
		s.notifyMetadataLocked()
	}
}

// TopicDestination is shared by all survivors: prefer home, otherwise rendezvous
// hashing over the currently reachable brokers. It is NOT quorum consensus.
func TopicDestination(logicalID string, home uint16, available []uint16) uint16 {
	if slices.Contains(available, home) {
		return home
	}
	var winner uint16
	var best [32]byte
	for _, id := range available {
		if id == 0 {
			continue
		}
		score := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", logicalID, id)))
		if winner == 0 || strings.Compare(string(score[:]), string(best[:])) > 0 || score == best && id < winner {
			winner, best = id, score
		}
	}
	return winner
}

func (r *brokerRuntime) liveBrokers() map[uint16]bool {
	live := map[uint16]bool{r.server.Id: true}
	for id, p := range r.peers {
		p.mu.Lock()
		available, last := p.available && p.synced, p.lastSuccess
		p.mu.Unlock()
		if last.IsZero() {
			last = r.started
		}
		live[id] = available || time.Since(last) < r.config.FailoverAfter
	}
	return live
}

func (r *brokerRuntime) reconcileTopics() {
	live := r.liveBrokers()
	var candidates []uint16
	for id, up := range live {
		if up {
			candidates = append(candidates, id)
		}
	}
	states := r.server.ClusterMetadata()
	known, removed := make(map[string]TopicMetadata), make(map[string]bool)
	hosted := make(map[string]map[uint16]bool)
	for _, state := range states {
		for _, t := range state.Metadata.KnownTopics {
			if t.LogicalID != "" {
				known[t.LogicalID] = t
			}
		}
		for _, t := range state.Metadata.Topics {
			if t.LogicalID == "" {
				continue
			}
			known[t.LogicalID] = t
			if hosted[t.LogicalID] == nil {
				hosted[t.LogicalID] = make(map[uint16]bool)
			}
			hosted[t.LogicalID][state.Metadata.ID] = live[state.Metadata.ID]
		}
		for _, t := range state.Metadata.RemovedTopics {
			removed[t.LogicalID] = true
		}
	}
	keys := make([]string, 0, len(known))
	for key := range known {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	s := r.server
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.rpcRunning || s.rpcState != r {
		return
	}
	changed := false
	for _, key := range keys {
		t := known[key]
		owner := TopicDestination(key, t.HomeBrokerID, candidates)
		local := s.topicsByLogical[key]
		if removed[key] {
			if local != nil {
				s.releaseHostedLocked(local)
				changed = true
			}
			continue
		}
		if owner == s.Id && local == nil {
			if s.TopicSeq == nil {
				continue
			}
			id, err := s.TopicSeq.Next()
			if err != nil {
				continue
			}
			local = newTopic(id, t.Name, s.Clients[t.OwnerName])
			local.LogicalID, local.HomeBrokerID, local.OwnerName = key, t.HomeBrokerID, t.OwnerName
			index := t.Name
			if s.Topics[index] != nil {
				index = t.Name + "@" + key
			}
			local.indexName = index
			if s.topicsByLogical == nil {
				s.topicsByLogical = make(map[string]*Topic)
			}
			s.Topics[index], s.topicsByID[id], s.topicsByLogical[key] = local, local, local
			s.metadata.UpsertTopic(local.metadata())
			changed = true
		} else if owner != s.Id && local != nil && hosted[key][owner] {
			// Keep serving during handoff until the preferred destination advertises it.
			s.releaseHostedLocked(local)
			changed = true
		}
	}
	if changed {
		s.notifyMetadataLocked()
	}
}

func (s *Server) releaseHostedLocked(topic *Topic) {
	delete(s.Topics, topic.indexName)
	delete(s.topicsByID, topic.Id)
	delete(s.topicsByLogical, topic.LogicalID)
	s.TopicSeq.Release(topic.Id)
	s.metadata.ReleaseTopic(topic.Id)
}

// RoutingTable exposes actual hosting claims, with a deterministic preference
// during handoff. Clients always send directly to the selected owner.
func (s *Server) RoutingTable() message.RouteTable {
	states := s.ClusterMetadata()
	s.mu.RLock()
	runtime, running := s.rpcState, s.rpcRunning
	s.mu.RUnlock()
	live := make(map[uint16]bool)
	if runtime != nil && running {
		live = runtime.liveBrokers()
	} else {
		live[s.Id] = true
	}
	var candidates []uint16
	for id, up := range live {
		if up {
			candidates = append(candidates, id)
		}
	}
	removed := make(map[string]bool)
	for _, state := range states {
		for _, t := range state.Metadata.RemovedTopics {
			removed[t.LogicalID] = true
		}
	}
	choices := make(map[string][]message.TopicRoute)
	table := message.RouteTable{}
	for _, state := range states {
		b := state.Metadata
		table.Brokers = append(table.Brokers, message.BrokerEndpoint{ID: b.ID, TCPAddr: b.TCPAddr, Available: live[b.ID]})
		if !live[b.ID] {
			continue
		}
		for _, t := range b.Topics {
			if t.LogicalID == "" || removed[t.LogicalID] {
				continue
			}
			udp := ""
			if len(b.UDPAddrs) > 0 {
				udp = b.UDPAddrs[int(t.ID)%len(b.UDPAddrs)]
			}
			choices[t.LogicalID] = append(choices[t.LogicalID], message.TopicRoute{LogicalID: t.LogicalID, Name: t.Name, BrokerID: b.ID, TopicID: t.ID, TCPAddr: b.TCPAddr, UDPAddr: udp, OwnerName: t.OwnerName, HomeBrokerID: t.HomeBrokerID})
		}
	}
	for key, options := range choices {
		want := TopicDestination(key, options[0].HomeBrokerID, candidates)
		slices.SortFunc(options, func(a, b message.TopicRoute) int {
			if a.BrokerID == b.BrokerID {
				return 0
			}
			if a.BrokerID == want {
				return -1
			}
			if b.BrokerID == want {
				return 1
			}
			return int(a.BrokerID) - int(b.BrokerID)
		})
		table.Routes = append(table.Routes, options[0])
	}
	slices.SortFunc(table.Routes, func(a, b message.TopicRoute) int { return strings.Compare(a.LogicalID, b.LogicalID) })
	slices.SortFunc(table.Brokers, func(a, b message.BrokerEndpoint) int { return int(a.ID) - int(b.ID) })
	hash := sha256.Sum256(table.Encode())
	table.Version = hex.EncodeToString(hash[:8])
	return table
}
