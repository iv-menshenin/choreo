package fleetctrl

import (
	"fmt"
	"net"

	"github.com/iv-menshenin/choreo/fleetctrl/internal/id"
	"github.com/iv-menshenin/choreo/transport"
)

func (m *Manager) sendKnockKnock() error {
	var data = make([]byte, 0, headSize)
	data = append(data, cmdBroadKnock[:]...)
	data = append(data, m.id[:]...)
	return wrapIfError("can't send KNCK", m.ls.SendAll(data))
}

func (m *Manager) sendWelcome(addr net.Addr) error {
	var data = make([]byte, 0, headSize)
	data = append(data, cmdWelcome[:]...)
	data = append(data, m.id[:]...)
	return wrapIfError("can't send WLCM", m.ls.Send(data, addr))
}

func (m *Manager) sendWantKey(key string) error {
	var data = make([]byte, 0, headSize+len(key))
	data = append(data, cmdBroadWantKey[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return wrapIfError("can't send WANT", m.ls.SendAll(data))
}

func (m *Manager) sendThatIsMine(key string) error {
	var data = make([]byte, 0, headSize+len(key))
	data = append(data, cmdBroadMine[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return wrapIfError("can't send MINE", m.ls.SendAll(data))
}

func (m *Manager) sendThatIsOccupied(addr net.Addr, owner id.ID, key []byte) error {
	var data = make([]byte, 0, headSize+len(key))
	data = append(data, cmdOccupied[:]...)
	data = append(data, owner[:]...)
	data = append(data, key...)
	return wrapIfError("can't send OCPD", m.ls.Send(data, addr))
}

func (m *Manager) sendCandidate(addr net.Addr, owner id.ID, key []byte) error {
	var data = make([]byte, 0, headSize+idSize+len(key))
	data = append(data, cmdCandidate[:]...)
	data = append(data, m.id[:]...)
	data = append(data, owner[:]...)
	data = append(data, key...)
	return wrapIfError("can't send CAND", m.ls.Send(data, addr))
}

func (m *Manager) sendSaved(addr net.Addr, owner id.ID, key []byte) error {
	var data = make([]byte, 0, headSize+idSize+len(key))
	data = append(data, cmdSaved[:]...)
	data = append(data, m.id[:]...)
	data = append(data, owner[:]...)
	data = append(data, key...)
	return wrapIfError("can't send SAVD", m.ls.Send(data, addr))
}

func (m *Manager) sendRegistered(addr net.Addr, key []byte) error {
	var data = make([]byte, 0, headSize+len(key))
	data = append(data, cmdRegistered[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return wrapIfError("can't send REGD", m.ls.Send(data, addr))
}

func (m *Manager) sendReset(key []byte) error {
	var data = make([]byte, 0, headSize+len(key))
	data = append(data, cmdBroadReset[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return wrapIfError("can't send RSET", m.ls.SendAll(data))
}

func (m *Manager) sendCompareInstances(id []byte) error {
	var data = make([]byte, 0, headSize+idSize)
	data = append(data, cmdBroadCompare[:]...)
	data = append(data, m.id[:]...)
	data = append(data, id...)
	return wrapIfError("can't send CMPI", m.ls.SendAll(data))
}

func (m *Manager) sendCompared(addr net.Addr, id []byte) error {
	var data = make([]byte, 0, headSize+idSize)
	data = append(data, cmdCompared[:]...)
	data = append(data, m.id[:]...)
	data = append(data, id...)
	return wrapIfError("can't send CMPO", m.ls.Send(data, addr))
}

func wrapIfError(msg string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", msg, err)
}

//nolint:gochecknoglobals,godox
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
	cmdOccupied   = cmd{'O', 'C', 'P', 'D'}
)

type (
	message struct {
		cmd    cmd
		sender id.ID
		data   []byte
		addr   net.Addr
	}
	cmd [cmdSize]byte
)

func (c cmd) String() string {
	return string(c[:])
}

func parse(rcv *transport.Received) (*message, error) {
	if len(rcv.Data) < headSize {
		return nil, ErrBadMessage
	}

	var msg = message{
		cmd:    cmd(rcv.Data[0:cmdSize]),
		sender: id.ID(rcv.Data[cmdSize:headSize]),
		data:   rcv.Data[headSize:],
		addr:   rcv.Addr,
	}

	return &msg, nil
}

func makeCmdData(blocks ...[]byte) []byte {
	var sz int
	for _, data := range blocks {
		sz += len(data)
	}

	var cmdData = make([]byte, 0, sz)
	for _, data := range blocks {
		cmdData = append(cmdData, data...)
	}

	return cmdData
}
