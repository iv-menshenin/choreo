package fleetctrl

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/iv-menshenin/choreo/fleetctrl/internal/send"
)

func (m *Manager) readLoop() {
	for {
		select {
		case <-m.done:
			return

		default:
			var buf [1024]byte
			if err := m.readAndProcess(buf[:]); err != nil {
				m.setErr(err)
			}
		}
	}
}

func (m *Manager) readAndProcess(buf []byte) error {
	received, err := m.ls.Listen(buf)
	if err != nil {
		return fmt.Errorf("can't read from listener: %w", err)
	}
	var msg send.Message
	if err = msg.Parse(received); err != nil {
		return err
	}

	go func() {
		if atomic.LoadInt64(&m.state) == StateStop {
			return
		}
		if err = m.process(&msg); err != nil {
			m.setErr(err)
			return
		}
		m.awr.Trigger(received.Data)
	}()
	return nil
}

func (m *Manager) process(msg *send.Message) error {
	if msg.Sender == m.id {
		// skip self owned messages
		return nil
	}
	var err error
	switch msg.Cmd {
	// tell them all that we are online
	case send.CmdBroadKnock:
		err = m.processKnockKnock(msg)

	// register everyone who said hello
	case send.CmdWelcome:
		err = m.processWelcome(msg)

	// confirm the on-line instance list
	case send.CmdBroadCompare:
		err = m.processCompare(msg)

	// someone bragged about a captured key
	case send.CmdBroadMine:
		err = m.processMine(msg)

	case send.CmdBroadWantKey:
		err = m.processWant(msg)

	case send.CmdRegistered:
		err = m.processRegistered(msg)

	case send.CmdOccupied:
		err = m.processOccupied(msg)

	// someone wants to revoke possession of a key because of a conflict
	case send.CmdBroadReset:
		m.keeper.Reset(string(msg.Data))
	}

	return err
}

func (m *Manager) processKnockKnock(msg *send.Message) error {
	if m.keeper.Keep(msg.Sender, msg.Addr) {
		m.debug("REGISTERED: %x %s", msg.Sender, msg.Addr.String())
	}
	return m.sr.Welcome(msg.Addr)
}

func (m *Manager) processWelcome(msg *send.Message) error {
	if m.keeper.Keep(msg.Sender, msg.Addr) {
		m.debug("REGISTERED: %x %s", msg.Sender, msg.Addr.String())
	}
	return nil
}

func (m *Manager) processCompare(msg *send.Message) error {
	h := m.keeper.Hash()
	if bytes.Equal(h[:], msg.Data) {
		return m.sr.Compared(msg.Addr, msg.Data)
	}
	return nil
}

func (m *Manager) processMine(msg *send.Message) error {
	if saveErr := m.keeper.Save(msg.Sender, msg.Addr, string(msg.Data)); saveErr != nil {
		// err = m.sendReset(msg.data) not yours
		m.debug("OWNERSHIP IGNORED %s: %s %+v", msg.Sender, string(msg.Data), saveErr)
	} else {
		m.debug("OWNERSHIP APPROVED %s: %s", msg.Sender, string(msg.Data))
		return m.sr.Saved(msg.Addr, msg.Sender, msg.Data)
	}
	return nil
}

func (m *Manager) processWant(msg *send.Message) error {
	owner, ok := m.keeper.Lookup(string(msg.Data))
	if !ok {
		if m.own.Add(msg.Sender, string(msg.Data)) {
			m.debug("OWNERSHIP CANDIDATE %s: %s", msg.Sender, string(msg.Data))
			return m.sr.Candidate(msg.Addr, msg.Sender, msg.Data)
		}
		// some of the participants have already managed to declare themselves as an owner,
		// so as not to spoil the protocol of quorum gathering, we need to keep silent
		return nil
	}
	if owner.Me() {
		return m.sr.Registered(msg.Addr, msg.Data)
	}
	return m.sr.ThatIsOccupied(msg.Addr, owner.ID, msg.Data)
}

func (m *Manager) processRegistered(msg *send.Message) error {
	if msg.Sender == m.id {
		return nil
	}
	if saveErr := m.keeper.Save(msg.Sender, msg.Addr, string(msg.Data)); saveErr != nil {
		m.warning("OWNERSHIP RESET %s: %s %+v", msg.Sender, string(msg.Data), saveErr)
		return m.sr.Reset(msg.Data) // not yours
	}
	return nil
}

func (m *Manager) processOccupied(msg *send.Message) error {
	if msg.Sender == m.id {
		if m.keeper.Own(string(msg.Data)) {
			return fmt.Errorf("can't own %s", string(msg.Data))
		}
		return nil
	}
	if saveErr := m.keeper.Save(msg.Sender, msg.Addr, string(msg.Data)); saveErr != nil {
		m.warning("OWNERSHIP RESET %s: %s %+v", msg.Sender, string(msg.Data), saveErr)
		return m.sr.Reset(msg.Data) // not yours
	}
	return nil
}

func (m *Manager) subscribeNotifyArmed(ch chan<- struct{}) {
	m.armedCh = append(m.armedCh, ch)
}

func (m *Manager) publicNotifyArmed() {
	for _, v := range m.armedCh {
		close(v)
	}
	m.armedCh = m.armedCh[:0]
}
