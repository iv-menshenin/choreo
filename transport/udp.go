package transport

import (
	"encoding/binary"
	"fmt"
	"net"
)

type (
	Listener struct {
		port uint16
		addr []net.IP
		conn net.PacketConn
	}
	Received struct {
		Addr net.Addr
		Data []byte
	}
)

const DefaultPort = 7999

func NewUDP(port uint16) (*Listener, error) {
	if port == 0 {
		port = DefaultPort
	}
	pc, err := net.ListenPacket("udp4", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	var l = Listener{
		port: port,
		conn: pc,
	}
	if err = l.discoverSubnets(); err != nil {
		return nil, err
	}
	return &l, nil
}

func (l *Listener) Listen(buf []byte) (*Received, error) {
	n, addr, err := l.conn.ReadFrom(buf)
	if err != nil {
		return nil, err
	}
	received := Received{
		Addr: addr,
		Data: buf[:n],
	}
	return &received, nil
}

func (l *Listener) SendAll(data []byte) error {
	for _, addr := range l.addr {
		udp := net.UDPAddr{
			IP:   addr,
			Port: int(l.port),
		}
		if _, err := l.conn.WriteTo(data, &udp); err != nil {
			return err
		}
	}
	return nil
}

func (l *Listener) Send(data []byte, addr net.Addr) error {
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		var err error
		udpAddr, err = net.ResolveUDPAddr(addr.Network(), addr.String())
		if err != nil {
			return err
		}
	}
	if _, err := l.conn.WriteTo(data, udpAddr); err != nil {
		return err
	}
	return nil
}

func (l *Listener) Close() error {
	return l.conn.Close()
}

func (l *Listener) discoverSubnets() error {
	ifaces, err := net.Interfaces()
	if err != nil {
		return err
	}
	l.addr = make([]net.IP, 0, len(ifaces))
	for _, i := range ifaces {
		if i.Name == "lo" {
			continue
		}
		addrs, err := i.Addrs()
		if err != nil {
			return err
		}
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				ipV4 := v.IP.To4()
				if ipV4 == nil {
					continue
				}
				ip := make(net.IP, len(ipV4))
				binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(ipV4)|^binary.BigEndian.Uint32(net.IP(v.Mask).To4()))
				l.addr = append(l.addr, ip)
			}
		}
	}
	return nil
}
