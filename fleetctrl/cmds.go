package fleetctrl

import (
	"errors"
	"net"

	"github.com/iv-menshenin/choreo/fleetctrl/internal/id"
	"github.com/iv-menshenin/choreo/transport"
)

func (m *Manager) sendKnockKnock() error {
	var data = make([]byte, 0, 20)
	data = append(data, cmdBroadKnock[:]...)
	data = append(data, m.id[:]...)
	return m.ls.SendAll(data)
}

func (m *Manager) sendWelcome(addr net.Addr) error {
	var data = make([]byte, 0, 20)
	data = append(data, cmdWelcome[:]...)
	data = append(data, m.id[:]...)
	return m.ls.Send(data, addr)
}

func (m *Manager) sendWantKey(key string) error {
	var data = make([]byte, 0, 20+len(key))
	data = append(data, cmdBroadWantKey[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return m.ls.SendAll(data)
}

func (m *Manager) sendThatIsMine(key string) error {
	var data = make([]byte, 0, 20+len(key))
	data = append(data, cmdBroadMine[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return m.ls.SendAll(data)
}

func (m *Manager) sendCandidate(addr net.Addr, owner id.ID, key []byte) error {
	var data = make([]byte, 0, 36+len(key))
	data = append(data, cmdCandidate[:]...)
	data = append(data, m.id[:]...)
	data = append(data, owner[:]...)
	data = append(data, key...)
	return m.ls.Send(data, addr)
}

func (m *Manager) sendSaved(addr net.Addr, owner id.ID, key []byte) error {
	var data = make([]byte, 0, 36+len(key))
	data = append(data, cmdSaved[:]...)
	data = append(data, m.id[:]...)
	data = append(data, owner[:]...)
	data = append(data, key...)
	return m.ls.Send(data, addr)
}

func (m *Manager) sendRegistered(addr net.Addr, key []byte) error {
	var data = make([]byte, 0, 20+len(key))
	data = append(data, cmdRegistered[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return m.ls.Send(data, addr)
}

func (m *Manager) sendReset(key []byte) error {
	var data = make([]byte, 0, 20+len(key))
	data = append(data, cmdBroadReset[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return m.ls.SendAll(data)
}

func (m *Manager) sendCompareInstances(id []byte) error {
	var data = make([]byte, 0, 36)
	data = append(data, cmdBroadCompare[:]...)
	data = append(data, m.id[:]...)
	data = append(data, id...)
	return m.ls.SendAll(data)
}

func (m *Manager) sendCompared(addr net.Addr, id []byte) error {
	var data = make([]byte, 0, 36)
	data = append(data, cmdCompared[:]...)
	data = append(data, m.id[:]...)
	data = append(data, id...)
	return m.ls.Send(data, addr)
}

var (
	cmdBroadKnock   = cmd{'K', 'N', 'C', 'K'}
	cmdBroadMine    = cmd{'M', 'I', 'N', 'E'} // TODO address
	cmdBroadWantKey = cmd{'W', 'A', 'N', 'T'}
	cmdBroadReset   = cmd{'R', 'S', 'E', 'T'}
	cmdBroadCompare = cmd{'C', 'M', 'P', 'I'}

	cmdWelcome    = cmd{'W', 'L', 'C', 'M'}
	cmdCandidate  = cmd{'C', 'A', 'N', 'D'}
	cmdRegistered = cmd{'R', 'E', 'G', 'D'}
	cmdSaved      = cmd{'S', 'A', 'V', 'D'}
	cmdCompared   = cmd{'C', 'M', 'P', 'O'}
)

type (
	message struct {
		cmd    cmd
		sender id.ID
		data   []byte
		addr   net.Addr
	}
	cmd [4]byte
)

func (c cmd) String() string {
	return string(c[:])
}

func parse(rcv *transport.Received) (*message, error) {
	if len(rcv.Data) < 20 {
		return nil, errors.New("bad message")
	}
	var msg = message{
		data: rcv.Data[20:],
		addr: rcv.Addr,
	}
	copy(msg.cmd[:], rcv.Data[0:4])
	copy(msg.sender[:], rcv.Data[4:20])
	return &msg, nil
}
