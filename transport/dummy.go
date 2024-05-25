package transport

import (
	"bytes"
	"crypto/rand"
	"log"
	mr "math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

		started  time.Time
		bytesAll int64
		cntAll   int64
	}
	Datagram struct {
		from net.IP
		to   net.IP
		data []byte
	}
)

var DummyBroadcast net.IP = []byte{0, 0, 0, 0}

func NewDummy() *DummyNetwork {
	var network = DummyNetwork{
		lst:     make(map[[4]byte]*DummyListener),
		net:     make(chan Datagram),
		started: time.Now(),
	}
	go network.processPackets()
	return &network
}

func (n *DummyNetwork) processPackets() {
	for netPacket := range n.net {
		var msg = netPacket
		atomic.AddInt64(&n.bytesAll, int64(len(msg.data)))
		atomic.AddInt64(&n.cntAll, 1)
		log.Printf("ROUTING FROM %s TO %s DATA %s %x %x", msg.from, msg.to, msg.data[:4], msg.data[4:20], msg.data[20:])
		broad := msg.to.Equal(DummyBroadcast)
		n.mux.RLock()
		for _, v := range n.lst {
			var (
				receiverIP    = msg.to.String()
				participantIP = v.ip.String()
			)
			if broad || receiverIP == participantIP {
				go func(rcv chan<- Datagram) {
					<-time.After(time.Duration(mr.Intn(5)+2) * time.Millisecond) //nolint:gosec // network latency
					select {
					case <-time.After(10 * time.Second):
						// packet lost
						log.Printf("DROPPED TO %s: %s %x", receiverIP, msg.data[:4], msg.data[20:])
					case rcv <- msg:
						// received
						log.Printf("DELIVERED TO %s: %s %x", receiverIP, msg.data[:4], msg.data[20:])
					}
				}(v.rcv)
			}
		}
		n.mux.RUnlock()
	}
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

func (n *DummyNetwork) Close() {
	since := time.Since(n.started)
	log.Printf("NETWORK STAT: %v SENT %d MESSAGES with %d KB (%0.3f kbps)", since, n.cntAll, n.bytesAll/1024, float64((1000*n.bytesAll)/since.Milliseconds())/1024)
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

func (d *DummyListener) GetIP() net.IP {
	return d.ip
}
