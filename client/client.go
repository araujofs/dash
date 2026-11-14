package client

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"dash/message"
)


type Broker struct{
	TcpAddr string
	UdpAddr string
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

func (c *Client) Connect() error {
	for _, broker := range c.bootstrap {
		connection, err := net.Dial("tcp", broker.TcpAddr)

		if err != nil {
			fmt.Println(err)
			continue
		}

		
		c.tcpConn = connection
		return nil
	}

	return errors.New("impossible to connect to any broker")
}


func (c *Client) Close() error{

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tcpConn != nil {
		err := c.tcpConn.Close()
		c.tcpConn = nil
		return err
	}

	return nil
}


func (c *Client) SendControl(msg *message.ControlMessage) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tcpConn == nil {
		return errors.New("client not connectd")
	}

	encoded := msg.Encode()

	if len(encoded) > 256 {
		return errors.New("max length for a control message is 256 bytes")
	}

	_, err := c.tcpConn.Write(encoded)

	return err
}
