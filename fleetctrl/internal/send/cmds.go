package send

import (
	"fmt"
	"net"

	"github.com/iv-menshenin/choreo/fleetctrl/id"
	"github.com/iv-menshenin/choreo/transport"
)

type Sender struct {
	id id.ID
	tt Transport
}

type Transport interface {
	SendAll([]byte) error
	Send([]byte, net.Addr) error
	Listen([]byte) (*transport.Received, error)
}

func New(id id.ID, tt Transport) *Sender {
	return &Sender{
		id: id,
		tt: tt,
	}
}

const (
	cmdSize  = 4
	idSize   = 16
	hashSize = 32
	headSize = cmdSize + idSize
)

func (m *Sender) KnockKnock() error {
	var data = make([]byte, 0, headSize)
	data = append(data, CmdBroadKnock[:]...)
	data = append(data, m.id[:]...)
	return wrapIfError("can't send KNCK", m.tt.SendAll(data))
}

func (m *Sender) Welcome(addr net.Addr) error {
	var data = make([]byte, 0, headSize)
	data = append(data, CmdWelcome[:]...)
	data = append(data, m.id[:]...)
	return wrapIfError("can't send WLCM", m.tt.Send(data, addr))
}

func (m *Sender) WantKey(key string) error {
	var data = make([]byte, 0, headSize+len(key))
	data = append(data, CmdBroadWantKey[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return wrapIfError("can't send WANT", m.tt.SendAll(data))
}

func (m *Sender) ThatIsMine(key string) error {
	var data = make([]byte, 0, headSize+len(key))
	data = append(data, CmdBroadMine[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return wrapIfError("can't send MINE", m.tt.SendAll(data))
}

func (m *Sender) ThatIsOccupied(addr net.Addr, owner id.ID, key []byte) error {
	var data = make([]byte, 0, headSize+len(key))
	data = append(data, CmdOccupied[:]...)
	data = append(data, m.id[:]...)
	data = append(data, owner[:]...)
	data = append(data, key...)
	return wrapIfError("can't send OCPD", m.tt.Send(data, addr))
}

func (m *Sender) Candidate(addr net.Addr, owner id.ID, key []byte) error {
	var data = make([]byte, 0, headSize+idSize+len(key))
	data = append(data, CmdCandidate[:]...)
	data = append(data, m.id[:]...)
	data = append(data, owner[:]...)
	data = append(data, key...)
	return wrapIfError("can't send CAND", m.tt.Send(data, addr))
}

func (m *Sender) Saved(addr net.Addr, owner id.ID, key []byte) error {
	var data = make([]byte, 0, headSize+idSize+len(key))
	data = append(data, CmdSaved[:]...)
	data = append(data, m.id[:]...)
	data = append(data, owner[:]...)
	data = append(data, key...)
	return wrapIfError("can't send SAVD", m.tt.Send(data, addr))
}

func (m *Sender) Registered(addr net.Addr, key []byte) error {
	var data = make([]byte, 0, headSize+len(key))
	data = append(data, CmdRegistered[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return wrapIfError("can't send REGD", m.tt.Send(data, addr))
}

func (m *Sender) Reset(key []byte) error {
	var data = make([]byte, 0, headSize+len(key))
	data = append(data, CmdBroadReset[:]...)
	data = append(data, m.id[:]...)
	data = append(data, key...)
	return wrapIfError("can't send RSET", m.tt.SendAll(data))
}

func (m *Sender) CompareInstances(id []byte) error {
	var data = make([]byte, 0, headSize+idSize)
	data = append(data, CmdBroadCompare[:]...)
	data = append(data, m.id[:]...)
	data = append(data, id...)
	return wrapIfError("can't send CMPI", m.tt.SendAll(data))
}

func (m *Sender) Compared(addr net.Addr, hash []byte) error {
	var data = make([]byte, 0, headSize+hashSize)
	data = append(data, CmdCompared[:]...)
	data = append(data, m.id[:]...)
	data = append(data, hash...)
	return wrapIfError("can't send CMPO", m.tt.Send(data, addr))
}

func wrapIfError(msg string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", msg, err)
}

//nolint:gochecknoglobals,godox
var (
	CmdBroadKnock   = Cmd{'K', 'N', 'C', 'K'}
	CmdBroadMine    = Cmd{'M', 'I', 'N', 'E'} // TODO address
	CmdBroadWantKey = Cmd{'W', 'A', 'N', 'T'}
	CmdBroadReset   = Cmd{'R', 'S', 'E', 'T'}
	CmdBroadCompare = Cmd{'C', 'M', 'P', 'I'}

	CmdWelcome    = Cmd{'W', 'L', 'C', 'M'}
	CmdCandidate  = Cmd{'C', 'A', 'N', 'D'}
	CmdRegistered = Cmd{'R', 'E', 'G', 'D'}
	CmdSaved      = Cmd{'S', 'A', 'V', 'D'}
	CmdCompared   = Cmd{'C', 'M', 'P', 'O'}
	CmdOccupied   = Cmd{'O', 'C', 'P', 'D'}
)

type Cmd [cmdSize]byte

func (c Cmd) String() string {
	return string(c[:])
}

func (c Cmd) CmdData(dest id.ID, blocks ...[]byte) []byte {
	var sz = cmdSize + idSize
	for _, data := range blocks {
		sz += len(data)
	}

	var cmdData = make([]byte, 0, sz)
	cmdData = append(cmdData, c[:]...)
	cmdData = append(cmdData, dest[:]...)

	for _, data := range blocks {
		cmdData = append(cmdData, data...)
	}

	return cmdData
}
