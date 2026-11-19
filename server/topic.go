package server

type Topic struct {
	Id      uint16
	Name    string
	Clients []*Client
	Owner   *Client
	// Server  *Server
}

func newTopic(id uint16, name string, owner *Client) *Topic {
	return &Topic{
		Id:      id,
		Name:    name,
		Clients: []*Client{},
		Owner:   owner,
		// Server:  server,
	}
}
