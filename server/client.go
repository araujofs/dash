package server

import (
	"dash/message"
	"dash/server/buffer"
	"net"
)

type Client struct {
	Id      uint64
	Name    string
	UdpAddr *net.UDPAddr
	TcpAddr *net.TCPAddr
	TcpConn *net.TCPConn
	Buffer  *buffer.Buffer[message.DataMessage]
}
