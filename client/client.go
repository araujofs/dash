package client

import (
	"dash/message"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
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


func NewClient(name string, bootstrap ...Broker, ) *Client {
	return &Client{
		Name: name,
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


func (c *Client) ReadControl() (*message.ControlMessage, error){
	if c.tcpConn == nil {
		return nil, errors.New("client not connected")
	}

	sizeBuf := make([]byte,4)

	_, err := io.ReadFull(c.tcpConn, sizeBuf)
	if err != nil {
		return nil,err
	}

	bodySize := binary.BigEndian.Uint32(sizeBuf)

	if (bodySize > 252 || bodySize < 3 ) {
		return nil, errors.New("")
	}

	bodyBuf := make([]byte, bodySize)

	_, err = io.ReadFull(c.tcpConn, bodyBuf)
	if err != nil {
		return nil,err
	}

	return message.DecodeControlMessage(bodyBuf)
}
 