package server

import (
	"dash/consts"
	"net"
	"strconv"
)

type UdpInfo struct {
	udpAddr *net.UDPAddr
	udpConn *net.UDPConn
	clients []*Client
}

type Topic struct {
	clients []*Client
	server  Server
}

type Server struct {
	Clients     []*Client
	Topics      []*Topic
	TcpAddr     *net.TCPAddr
	TcpListener *net.TCPListener
	TopicSeq    uint64
	UdpConfig   []*UdpInfo
}

/* tcpPort aceita nil, de maneira que se for passado nil a porta será escolhida pelo SO */
func NewServer(tcpPort int) (*Server, error) {
	udpConfig := [consts.UDP_PORTS]*UdpInfo{}

	for i := range consts.UDP_PORTS {
		udpAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
		if err != nil {
			return nil, err
		}

		udpConn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			return nil, err
		}

		port := udpConn.LocalAddr().(*net.UDPAddr).Port
		udpAddr.Port = port

		udpConfig[i] = &UdpInfo{
			udpAddr: udpAddr,
			udpConn: udpConn,
			clients: []*Client{},
		}
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(tcpPort))
	if err != nil {
		return nil, err
	}

	tcpLis, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	if tcpPort == 0 {
		port := tcpLis.Addr().(*net.TCPAddr).Port
		tcpAddr.Port = port
	}

	return &Server{
		UdpConfig:   udpConfig[:],
		TcpAddr:     tcpAddr,
		TcpListener: tcpLis,
		Clients:     []*Client{},
		Topics:      []*Topic{},
	}, nil
}
