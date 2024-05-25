package fleetctrl

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/iv-menshenin/choreo/fleetctrl/id"
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
		return fmt.Errorf("can't parse message: %w", err)
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
		m.debug("REGISTERED: %s %s", msg.Sender, msg.Addr.String())
	}
	err := m.sr.Welcome(msg.Addr)
	if err != nil {
		return fmt.Errorf("greeting error: %w", err)
	}
	return nil
}

func (m *Manager) processWelcome(msg *send.Message) error {
	if m.keeper.Keep(msg.Sender, msg.Addr) {
		m.debug("REGISTERED: %s %s", msg.Sender, msg.Addr.String())
	}
	return nil
}

func (m *Manager) processCompare(msg *send.Message) error {
	h := m.keeper.Hash()
	if bytes.Equal(h[:], msg.Data) {
		err := m.sr.Compared(msg.Addr, msg.Data)
		if err != nil {
			return fmt.Errorf("fleet-hash comparation error: %w", err)
		}
	}
	return nil
}

func (m *Manager) processMine(msg *send.Message) error {
	if saveErr := m.keeper.Save(msg.Sender, msg.Addr, string(msg.Data)); saveErr != nil {
		// err = m.sendReset(msg.data) not yours
		m.debug("OWNERSHIP IGNORED %s: %s %+v", msg.Sender, string(msg.Data), fmt.Errorf("can't save it: %w", saveErr))
	} else {
		m.debug("OWNERSHIP APPROVED %s: %s", msg.Sender, string(msg.Data))
		err := m.sr.Saved(msg.Addr, msg.Sender, msg.Data)
		if err != nil {
			return fmt.Errorf("can't advertise ownership approvement: %w", err)
		}
	}
	return nil
}

func (m *Manager) processWant(msg *send.Message) error {
	owner, ok := m.keeper.Lookup(string(msg.Data))
	if !ok {
		if !m.own.Add(msg.Sender, string(msg.Data)) {
			// some of the participants have already managed to declare themselves as an owner,
			// so as not to spoil the protocol of quorum gathering, we need to keep silent
			return nil
		}
		m.debug("OWNERSHIP CANDIDATE %s: %s", msg.Sender, string(msg.Data))
		err := m.sr.Candidate(msg.Addr, msg.Sender, msg.Data)
		if err != nil {
			return fmt.Errorf("can't advertise approvement: %w", err)
		}
		return nil
	}

	var err error
	if owner.Me() {
		err = m.sr.Registered(msg.Addr, msg.Data)
	} else {
		err = m.sr.ThatIsOccupied(msg.Addr, owner.ID, msg.Data)
	}
	if err != nil {
		return fmt.Errorf("can't advertise rejection: %w", err)
	}
	return nil
}

func (m *Manager) processRegistered(msg *send.Message) error {
	shard, ok := m.keeper.Shard(msg.Sender)
	if !ok {
		return nil
	}
	err := m.keeper.Save(msg.Sender, shard.Addr, string(msg.Data))
	if err != nil {
		m.warning("OWNERSHIP RESET %s: %s %+v", msg.Sender, string(msg.Data), err)
		err = m.sr.Reset(msg.Data) // not yours
		if err != nil {
			return fmt.Errorf("can't fix ownership: %w", err)
		}
	}
	return nil
}

func (m *Manager) processOccupied(msg *send.Message) error {
	owner := id.ID(msg.Data[:id.Size])
	key := string(msg.Data[id.Size:])
	if owner == m.id {
		m.keeper.Own(key)
		return nil
	}
	if shard, ok := m.keeper.Shard(owner); ok {
		err := m.keeper.Save(owner, shard.Addr, key)
		if err != nil {
			m.warning("OWNERSHIP RESET %s: %s+%s %+v", msg.Sender, owner, key, err)
			err = m.sr.Reset([]byte(key)) // not yours
			if err != nil {
				return fmt.Errorf("can't fix ownership: %w", err)
			}
		}
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
