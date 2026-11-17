package client

import (
	"dash/message"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"dash/consts"
	"google.golang.org/protobuf/proto"
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

		udpConn, err := net.ListenUDP("udp",nil)
		if err != nil {
			return  err
		}
		c.udpConn = udpConn


		c.tcpConn = connection
		return nil
	}

	return errors.New("impossible to connect to any broker")
}


func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var TcpErr error 
	var UdpErr error

	if c.tcpConn != nil {
		TcpErr = c.tcpConn.Close()
		c.tcpConn = nil
	}

	if c.udpConn != nil {
		UdpErr = c.udpConn.Close()
		c.udpConn = nil
	}

	return errors.Join(TcpErr, UdpErr)
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
		return nil, errors.New("invalid control message body size")
	}

	bodyBuf := make([]byte, bodySize)

	_, err = io.ReadFull(c.tcpConn, bodyBuf)
	if err != nil {
		return nil,err
	}

	return message.DecodeControlMessage(bodyBuf)
}


func (c *Client) Subscribe (topic uint16) error {
	msg := message.NewControlMessage(message.SUB, topic, []byte(c.Name))

	err := c.SendControl(msg)
	if err != nil {
		return err
	}

	response, err := c.ReadControl()
	if err != nil {
		return err
	}

	if len(response.Payload) > 0 && response.Payload[0] == 0 {
		c.Subscriptions[topic] = true
		return nil
	}

	return errors.New("broker rejected subscription")
}


func (c *Client) Unsubscribe (topic uint16) error {
	msg := message.NewControlMessage(message.UNSUB, topic, nil)

	err := c.SendControl(msg)
	if err != nil {
		return err
	}

	response, err := c.ReadControl()
	if err != nil {
		return err
	}

	if len(response.Payload) > 0 && response.Payload[0] == 0 {
		c.Subscriptions[topic] = false
		return nil
	}

	return errors.New("broker rejected unsubscription")
}


func (c *Client) CreateTopic(topic uint16) error {
	msg := message.NewControlMessage(message.ADD, topic, nil)

	err := c.SendControl(msg)
	if err != nil {
		return err
	}

	response, err := c.ReadControl()
	if err != nil {
		return err
	}

	if len(response.Payload) > 0 && response.Payload[0] == 0 {
		return nil
	}

	return errors.New("broker rejected topic creation")
}


func (c *Client) DeleteTopic(topic uint16) error {
	msg := message.NewControlMessage(message.DEL, topic, nil)

	err := c.SendControl(msg)
	if err != nil {
		return err
	}

	response, err := c.ReadControl()
	if err != nil {
		return err
	}

	if len(response.Payload) > 0 && response.Payload[0] == 0 {
		delete(c.Topics, topic)
		delete(c.Subscriptions, topic)
		return nil
	}

	return errors.New("broker rejected topic deletion")
}


func (c *Client) Ping() error {
	msg := message.NewControlMessage(message.PING, 0, nil)

	err := c.SendControl(msg)
	if err != nil {
		return err
	}

	response, err := c.ReadControl()
	if err != nil {
		return err
	}

	if len(response.Payload) > 0 && response.Payload[0] == 0 {
		return nil
	}

	return errors.New("ping failed: broker rejected or returned error")
}


func (c *Client) SendData(msg *message.DataMessage, addr *net.UDPAddr) error{
	  if c.udpConn == nil {
            return errors.New("client udp not initialized")
        }

        if msg == nil {
            return errors.New("message cannot be nil")
        }

        bytes := msg.Encode()

        _, err := c.udpConn.WriteToUDP(bytes, addr)
        return err
}


func (c *Client) ReadData() (*message.DataMessage, error) {
	if c.udpConn == nil {
		return nil, errors.New("client udp not initialized")
	}

	buf := make([]byte, consts.MAX_DATA_MESSAGE_SIZE)

	n, _, err := c.udpConn.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}

	if n < consts.HEADERS_SIZE {
		return nil, errors.New("data message smaller than header size")
	}

	return message.DecodeDataMessage(buf[:n])
}

func (c *Client) GetMetadata() error {
	msg := message.NewControlMessage(message.GET_META, 0, nil)

	if err := c.SendControl(msg); err != nil {
		return err
	}

	response, err := c.ReadControl()
	if err != nil {
		return err
	}

	var meta message.ClusterMetadata
	if err := proto.Unmarshal(response.Payload, &meta); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, b := range meta.GetBrokers() {
		brokerID := uint16(b.GetId())
		c.Brokers[brokerID] = Broker{
			TcpAddr: b.GetTcpAddr(),
			UdpAddr: b.GetUdpAddr(),
		}

		for _, t := range b.GetTopics() {
			topicID := uint16(t.GetId())
			c.Topics[topicID] = brokerID
		}
	}

	return nil
}
