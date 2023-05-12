package fleetctrl

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/election/ownership"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/election/waitfor"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/id"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iv-menshenin/choreo/transport"
)

type (
	Manager struct {
		loglevel LogLevel

		id   id.ID
		port uint16

		ls Transport

		state int64
		armed int64
		stop  int64

		err  error
		once sync.Once
		done chan struct{}

		ins *Instances
		awr *waitfor.Awaiter
		own *ownership.Ownership
	}
	LogLevel uint8
)

const (
	LogLevelError LogLevel = iota
	LogLevelWarning
	LogLevelDebug
)

type Transport interface {
	SendAll([]byte) error
	Send([]byte, net.Addr) error
	Listen([]byte) (*transport.Received, error)
}

const (
	StateCreated int64 = iota
	StateReady
	StateDiscovery
	StateDeactivated
	StateClosed
	StateBroken
)

const (
	UnArmed int64 = iota
	Armed
)

func New(transport Transport) *Manager {
	return &Manager{
		loglevel: LogLevelError,
		id:       id.New(),
		ls:       transport,
		state:    StateCreated,
		armed:    UnArmed,
		done:     make(chan struct{}),
		ins:      newInstances(),
		awr:      waitfor.New(),
		own:      ownership.New(nil),
	}
}

func (m *Manager) SetLogLevel(l LogLevel) {
	m.loglevel = l
}

func (m *Manager) debug(format string, args ...any) {
	if m.loglevel >= LogLevelDebug {
		log.Printf(format, args...)
	}
}

func (m *Manager) warning(format string, args ...any) {
	if m.loglevel >= LogLevelWarning {
		log.Printf(format, args...)
	}
}

func (m *Manager) error(format string, args ...any) {
	log.Printf(format, args...)
}

func (m *Manager) Key() string {
	return fmt.Sprintf("%x", m.id[:])
}

func (m *Manager) Manage() error {
	if !atomic.CompareAndSwapInt64(&m.state, StateCreated, StateDiscovery) {
		return errors.New("wrong state")
	}
	go m.stateLoop()
	go m.readLoop()
	<-m.done
	return m.err
}

func (m *Manager) Stop() {
	atomic.AddInt64(&m.stop, 1)
}

func (m *Manager) stateLoop() {
	var lastTimeDiscovered time.Time
	defer close(m.done)
	for {
		time.Sleep(10 * time.Millisecond)
		if atomic.LoadInt64(&m.stop) > 0 {
			atomic.StoreInt64(&m.state, StateDeactivated)
		}
		switch atomic.LoadInt64(&m.state) {
		case StateCreated:
			continue

		case StateReady:
			if del := m.ins.cleanup(); del > 0 {
				m.warning("UNLINKED: %d", del)
			}
			if time.Since(lastTimeDiscovered).Seconds() >= 1 {
				if err := m.checkArmedStatus(); err != nil {
					m.setErr(err)
					return
				}
				atomic.CompareAndSwapInt64(&m.state, StateReady, StateDiscovery)
			}
			continue

		case StateDiscovery:
			if err := m.sendKnockKnock(); err != nil {
				m.setErr(err)
				return
			}
			atomic.CompareAndSwapInt64(&m.state, StateDiscovery, StateReady)
			lastTimeDiscovered = time.Now()
			continue

		case StateDeactivated:
			atomic.StoreInt64(&m.state, StateClosed)
			fallthrough
		case StateClosed:
			return
		case StateBroken:
			return
		}
	}
}

func (m *Manager) checkArmedStatus() (err error) {
	m.debug("CHECK STATUS")
	var (
		cnt  = 3
		hash = m.ins.hashAllID(m.id)
	)
	for {
		errCh := m.awaitMostOf(cmdCompared, hash)
		if err = m.sendCompareInstances(hash); err != nil {
			return err
		}
		err = <-errCh
		if cnt--; err == nil || cnt < 0 {
			break
		}
	}
	if err != nil {
		if atomic.CompareAndSwapInt64(&m.armed, Armed, UnArmed) {
			m.warning("UNARMED: %+v", err)
		}
		return nil
	}
	if atomic.CompareAndSwapInt64(&m.armed, UnArmed, Armed) {
		m.warning("ARMED")
	}
	return nil
}

func (m *Manager) setErr(err error) {
	m.error("ERROR: %+v", err)
	m.once.Do(func() {
		m.err = err
		atomic.StoreInt64(&m.state, StateBroken)
	})
}

func (m *Manager) readLoop() {
	var buf [1024]byte
	for {
		received, err := m.ls.Listen(buf[:])
		if err != nil {
			m.setErr(err)
			return
		}
		parsed, err := parse(received)
		if err != nil {
			m.error("ERROR: %+v", err)
			continue
		}
		if err = m.process(parsed); err != nil {
			m.setErr(err)
			return
		}
		m.awr.Trigger(received.Data)
	}
}

func (m *Manager) process(msg *message) error {
	if msg.sender == m.id {
		// skip self owned messages
		return nil
	}
	var err error
	// tell them all that we are online
	if msg.cmd == cmdBroadKnock {
		if m.ins.add(msg.sender, msg.addr) {
			m.debug("REGISTERED: %x %s", msg.sender, msg.addr.String())
		}
		err = m.sendWelcome(msg.addr)
	}
	// register everyone who said hello
	if msg.cmd == cmdWelcome {
		if m.ins.add(msg.sender, msg.addr) {
			m.debug("REGISTERED: %x %s", msg.sender, msg.addr.String())
		}
	}
	// confirm the on-line instance list
	if msg.cmd == cmdBroadCompare {
		if bytes.Equal(m.ins.hashAllID(m.id), msg.data) {
			err = m.sendCompared(msg.addr, msg.data)
		}
	}
	// someone bragged about a captured key
	if msg.cmd == cmdBroadMine {
		if saveErr := m.ins.save(msg.sender, string(msg.data), msg.addr); saveErr != nil {
			// err = m.sendReset(msg.data) not yours
			m.debug("OWNERSHIP IGNORED %x: %s %+v", msg.sender, string(msg.data), saveErr)
		} else {
			m.debug("OWNERSHIP APPROVED %x: %s", msg.sender, string(msg.data))
			err = m.sendSaved(msg.addr, msg.sender, msg.data)
		}
	}
	if msg.cmd == cmdBroadWantKey {
		switch m.ins.search(string(msg.data)) {
		case Mine:
			err = m.sendRegistered(msg.addr, msg.data)
		case "":
			if m.own.Add(msg.sender, string(msg.data)) {
				m.debug("OWNERSHIP CANDIDATE %x: %s", msg.sender[:], string(msg.data))
				err = m.sendCandidate(msg.addr, msg.sender, msg.data)
			}
		}
	}
	if msg.cmd == cmdRegistered {
		if saveErr := m.ins.save(msg.sender, string(msg.data), msg.addr); saveErr != nil {
			m.warning("OWNERSHIP RESET %x: %s %+v", msg.sender, string(msg.data), saveErr)
			err = m.sendReset(msg.data) // not yours
		}
	}
	// someone wants to revoke possession of a key because of a conflict
	if msg.cmd == cmdBroadReset {
		m.ins.reset(string(msg.data))
	}

	return err
}
