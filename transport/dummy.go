package transport

import (
	"bytes"
	"crypto/rand"
	"log"
	"net"
	"strings"
	"sync"
)

type (
	DummyListener struct {
		mux  sync.Mutex
		id   [16]byte
		ip   net.IP
		port int
		snd  chan Datagram
		rcv  chan Datagram
	}
	DummyNetwork struct {
		mux sync.RWMutex
		lst map[[4]byte]*DummyListener
		net chan Datagram
	}
	Datagram struct {
		from net.IP
		to   net.IP
		data []byte
	}
)

func NewDummy() *DummyNetwork {
	var network = DummyNetwork{
		lst: make(map[[4]byte]*DummyListener),
		net: make(chan Datagram),
	}
	go func() {
		for netPacket := range network.net {
			var msg = netPacket
			log.Printf("ROUTING FROM %s TO %s DATA %s %x %x", msg.from, msg.to, msg.data[:4], msg.data[4:20], msg.data[20:])
			broad := msg.to.Equal(DummyBroadcast)
			network.mux.RLock()
			for _, v := range network.lst {
				var (
					receiverIP    = msg.to.String()
					participantIP = v.ip.String()
				)
				if broad || receiverIP == participantIP {
					var rcv = v.rcv
					go func() {
						rcv <- msg
						log.Printf("DELIVERED TO %s: %s %x", receiverIP, msg.data[:4], msg.data[20:])
					}()
				}
			}
			network.mux.RUnlock()
		}
	}()
	return &network
}

func sendIf(rcv chan<- Datagram, msg Datagram) {
}

func (n *DummyNetwork) NewListener() *DummyListener {
	var rndIP [4]byte
	_, err := rand.Read(rndIP[:])
	if err != nil {
		panic(err)
	}
	var l = DummyListener{
		ip:   rndIP[:],
		port: 0,
		snd:  n.net,
		rcv:  make(chan Datagram),
	}
	n.mux.Lock()
	n.lst[rndIP] = &l
	n.mux.Unlock()
	return &l
}

func (d *DummyListener) SendAll(data []byte) error {
	if bytes.Equal(data[:4], []byte{'K', 'N', 'C', 'K'}) {
		d.mux.Lock()
		copy(d.id[:], data[4:20])
		d.mux.Unlock()
	}
	go func() {
		log.Printf("ATTEMPT to send: %s from %s to all", string(data[:4]), d.ip.String())
		d.snd <- Datagram{
			from: d.ip,
			to:   DummyBroadcast,
			data: data,
		}
	}()
	return nil
}

func (d *DummyListener) Send(data []byte, addr net.Addr) error {
	dg := Datagram{
		from: d.ip,
		to:   net.ParseIP(strings.Split(addr.String(), ":")[0]),
		data: data,
	}
	go func() {
		log.Printf("ATTEMPT to send: %s from %s to %s #%x", string(data[:4]), d.ip.String(), addr.String(), data[20:])
		d.snd <- dg
		log.Printf("SENT: %s from %s to %s", string(data[:4]), d.ip.String(), addr.String())
	}()
	return nil
}

func (d *DummyListener) Listen([]byte) (*Received, error) {
	log.Printf("LISTEN %s", d.ip.String())
	rcv := <-d.rcv
	log.Printf("RECEIVED: %s from %s to %s", rcv.data[:4], rcv.from.String(), d.ip.String())
	return &Received{
		Addr: &net.UDPAddr{
			IP:   rcv.from,
			Port: 0,
		},
		Data: rcv.data,
	}, nil
}

var DummyBroadcast net.IP = []byte{0, 0, 0, 0}
