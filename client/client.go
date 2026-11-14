package client

import (
	"net"
	"sync"
)


type Broker struct{
	EnderecoTCP string
	EnderecoUDP string
}

type Client struct {
	Name string
	
	// cache
	Topics map[uint16]uint16
	Brokers map[uint16]Broker
	Subscriptions map[uint16]bool
	bootstrap []Broker

	udpConn *net.UDPConn
	tcpConn net.Conn

	mu sync.Mutex
}

func NewClient(nome string, bootstrap ...Broker, ) *Client {
	return &Client{
		Name: nome,
		bootstrap: bootstrap,
		Topics: map[uint16]uint16{},
		Brokers: map[uint16]Broker{},
		Subscriptions: map[uint16]bool{},
	}
}

func (c *Client) Connect(){

}

