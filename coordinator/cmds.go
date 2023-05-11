package coordinator

import (
	"errors"
	"net"

	"github.com/iv-menshenin/choreo/transport"
)

func (m *Manager) sendKnockKnock() error {
	var data = make([]byte, 0, 20)
	data = append(data, cmdBroadKnock...)
	data = append(data, m.id[:]...)
	return m.ls.SendAll(data)
}

func (m *Manager) sendWelcome(addr net.Addr) error {
	var data = make([]byte, 0, 20)
	data = append(data, cmdWelcome...)
	data = append(data, m.id[:]...)
	return m.ls.Send(data, addr)
}

func (m *Manager) sendWantKey(key string) error {
	var data = make([]byte, 0, 20+len(key))
	data = append(data, cmdBroadWantKey...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return m.ls.SendAll(data)
}

func (m *Manager) sendThatIsMine(key string) error {
	var data = make([]byte, 0, 20+len(key))
	data = append(data, cmdBroadMine...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return m.ls.SendAll(data)
}

func (m *Manager) sendCandidate(addr net.Addr, owner, key []byte) error {
	var data = make([]byte, 0, 36+len(key))
	data = append(data, cmdCandidate...)
	data = append(data, m.id[:]...)
	data = append(data, owner...)
	data = append(data, key...)
	return m.ls.Send(data, addr)
}

func (m *Manager) sendSaved(addr net.Addr, owner, key []byte) error {
	var data = make([]byte, 0, 36+len(key))
	data = append(data, cmdSaved...)
	data = append(data, m.id[:]...)
	data = append(data, owner...)
	data = append(data, key...)
	return m.ls.Send(data, addr)
}

func (m *Manager) sendRegistered(addr net.Addr, key []byte) error {
	var data = make([]byte, 0, 20+len(key))
	data = append(data, cmdRegistered...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return m.ls.Send(data, addr)
}

func (m *Manager) sendReset(key []byte) error {
	var data = make([]byte, 0, 20+len(key))
	data = append(data, cmdBroadReset...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return m.ls.SendAll(data)
}

func (m *Manager) sendCompareInstances(id []byte) error {
	var data = make([]byte, 0, 36)
	data = append(data, cmdBroadCompare...)
	data = append(data, m.id[:]...)
	data = append(data, id...)
	return m.ls.SendAll(data)
}

func (m *Manager) sendCompared(addr net.Addr, id []byte) error {
	var data = make([]byte, 0, 36)
	data = append(data, cmdCompared...)
	data = append(data, m.id[:]...)
	data = append(data, id...)
	return m.ls.Send(data, addr)
}

var (
	cmdBroadKnock   = []byte("KNCK")
	cmdBroadMine    = []byte("MINE") // TODO address
	cmdBroadWantKey = []byte("WANT")
	cmdBroadReset   = []byte("RSET")
	cmdBroadCompare = []byte("CMPI")

	cmdWelcome    = []byte("WLCM")
	cmdCandidate  = []byte("CAND")
	cmdRegistered = []byte("REGD")
	cmdSaved      = []byte("SAVD")
	cmdCompared   = []byte("CMPO")
)

type Msg struct {
	cmd    []byte
	sender []byte
	data   []byte
	addr   net.Addr
}

func parse(rcv *transport.Received) (Msg, error) {
	if len(rcv.Data) < 20 {
		return Msg{}, errors.New("bad message")
	}
	msg := Msg{
		cmd:    rcv.Data[0:4],
		sender: rcv.Data[4:20],
		data:   rcv.Data[20:],
		addr:   rcv.Addr,
	}
	return msg, nil
}
