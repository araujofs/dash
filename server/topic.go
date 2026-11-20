package server

import "crypto/rand"

type Topic struct {
	Id           uint16
	Name         string
	Clients      []*Client
	Owner        *Client
	LogicalID    string
	HomeBrokerID uint16
	OwnerName    string
	indexName    string
	// Server  *Server
}

func (t *Topic) metadata() TopicMetadata {
	return TopicMetadata{ID: t.Id, Name: t.Name, LogicalID: t.LogicalID, HomeBrokerID: t.HomeBrokerID, OwnerName: t.OwnerName}
}

func newTopic(id uint16, name string, owner *Client) *Topic {
	return &Topic{
		Id:        id,
		Name:      name,
		LogicalID: rand.Text(),
		OwnerName: func() string {
			if owner != nil {
				return owner.Name
			}
			return ""
		}(),
		indexName: name,
		Clients:   []*Client{},
		Owner:     owner,
		// Server:  server,
	}
}
