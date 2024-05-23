package send

import (
	"errors"
	"net"

	"github.com/iv-menshenin/choreo/fleetctrl/id"
	"github.com/iv-menshenin/choreo/transport"
)

type Message struct {
	Cmd    Cmd
	Sender id.ID
	Data   []byte
	Addr   net.Addr
}

var ErrBadMessage = errors.New("bad message")

func (m *Message) Parse(rcv *transport.Received) error {
	if len(rcv.Data) < headSize {
		return ErrBadMessage
	}

	m.Cmd = Cmd(rcv.Data[0:cmdSize])
	m.Sender = id.ID(rcv.Data[cmdSize:headSize])
	m.Data = rcv.Data[headSize:]
	m.Addr = rcv.Addr

	return nil
}
