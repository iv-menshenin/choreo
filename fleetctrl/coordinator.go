package fleetctrl

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl/internal/election/ownership"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/election/waitfor"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/id"
	"github.com/iv-menshenin/choreo/transport"
)

type (
	Manager struct {
		id id.ID
		ll LogLevel
		ls Transport

		state int64
		stop  int64

		err  error
		once sync.Once
		done chan struct{}

		ins *Instances
		awr *waitfor.Awaiter
		own *ownership.Ownership

		armedMux sync.Mutex
		armedFlg bool
		armedCh  []chan<- struct{}

		lastTimeDiscovered time.Time
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

func New(transport Transport) *Manager {
	return &Manager{
		ll:    LogLevelError,
		id:    id.New(),
		ls:    transport,
		state: StateCreated,
		done:  make(chan struct{}),
		ins:   newInstances(),
		awr:   waitfor.New(),
		own:   ownership.New(nil),
	}
}

func (m *Manager) SetLogLevel(l LogLevel) {
	m.ll = l
}

func (m *Manager) debug(format string, args ...any) {
	if m.ll >= LogLevelDebug {
		log.Printf(format, args...)
	}
}

func (m *Manager) warning(format string, args ...any) {
	if m.ll >= LogLevelWarning {
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
		return ErrWrongState
	}
	go m.stateLoop()
	go m.readLoop()
	<-m.done
	return m.err
}

func (m *Manager) Stop() {
	atomic.AddInt64(&m.stop, 1)
}

func (m *Manager) Status() bool {
	m.armedMux.Lock()
	defer m.armedMux.Unlock()
	return m.armedFlg
}

func (m *Manager) Armed() bool {
	m.armedMux.Lock()
	armed := m.armedFlg
	if !armed {
		m.armedFlg = true
		m.warning("ARMED")
		m.publicNotifyArmed()
	}
	m.armedMux.Unlock()
	return !armed
}

func (m *Manager) NotifyArmed() <-chan struct{} {
	var notifyWhenArmed = make(chan struct{})

	m.armedMux.Lock()
	if !m.armedFlg {
		m.subscribeNotifyArmed(notifyWhenArmed)
	} else {
		close(notifyWhenArmed)
	}
	m.armedMux.Unlock()

	return notifyWhenArmed
}

func (m *Manager) Unarmed(err error) bool {
	m.armedMux.Lock()
	armed := m.armedFlg
	if armed {
		m.armedFlg = false
		m.warning("UNARMED: %+v", err)
	}
	m.armedMux.Unlock()

	return armed
}

func (m *Manager) stateLoop() {
	defer close(m.done)

	for state := atomic.LoadInt64(&m.state); state != StateClosed; state = atomic.LoadInt64(&m.state) {

		if atomic.LoadInt64(&m.stop) > 0 {
			atomic.StoreInt64(&m.state, StateDeactivated)
		}
		switch atomic.LoadInt64(&m.state) {
		case StateCreated:

		case StateReady:
			m.readyStateAction()

		case StateDiscovery:
			m.discoveryStateAction()

		case StateBroken:
			atomic.StoreInt64(&m.state, StateDeactivated)
			fallthrough

		case StateDeactivated:
			atomic.StoreInt64(&m.state, StateClosed)
			fallthrough

		case StateClosed:
			return

		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (m *Manager) readyStateAction() {
	if del := m.ins.cleanup(); del > 0 {
		m.warning("UNLINKED: %d", del)
	}
	if time.Since(m.lastTimeDiscovered).Seconds() >= 1 {
		if err := m.checkArmedStatus(); err != nil {
			m.setErr(err)
			return
		}
		atomic.CompareAndSwapInt64(&m.state, StateReady, StateDiscovery)
	}
}

func (m *Manager) discoveryStateAction() {
	if err := m.sendKnockKnock(); err != nil {
		m.setErr(err)
		return
	}
	atomic.CompareAndSwapInt64(&m.state, StateDiscovery, StateReady)
	m.lastTimeDiscovered = time.Now()
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
		m.Unarmed(err)
	} else {
		m.Armed()
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
	for {
		select {
		case <-m.done:
			return

		default:
			if atomic.LoadInt64(&m.state) == StateBroken {
				return
			}
			var buf [1024]byte
			if err := m.readAndProcess(buf[:]); err != nil {
				m.setErr(err)
				return
			}
		}
	}
}

func (m *Manager) readAndProcess(buf []byte) error {
	received, err := m.ls.Listen(buf)
	if err != nil {
		return fmt.Errorf("can't read from listener: %w", err)
	}
	parsed, err := parse(received)
	if err != nil {
		return err
	}
	go func() {
		if err = m.process(parsed); err != nil {
			m.setErr(err)
			return
		}
		m.awr.Trigger(received.Data)
	}()
	return nil
}

func (m *Manager) process(msg *message) error {
	if msg.sender == m.id {
		// skip self owned messages
		return nil
	}
	var err error
	switch msg.cmd {
	// tell them all that we are online
	case cmdBroadKnock:
		err = m.processKnockKnock(msg)

	// register everyone who said hello
	case cmdWelcome:
		err = m.processWelcome(msg)

	// confirm the on-line instance list
	case cmdBroadCompare:
		err = m.processCompare(msg)

	// someone bragged about a captured key
	case cmdBroadMine:
		err = m.processMine(msg)

	case cmdBroadWantKey:
		err = m.processWant(msg)

	case cmdRegistered:
		err = m.processRegistered(msg)

	// someone wants to revoke possession of a key because of a conflict
	case cmdBroadReset:
		m.ins.reset(string(msg.data))
	}

	return err
}

func (m *Manager) processKnockKnock(msg *message) error {
	if m.ins.add(msg.sender, msg.addr) {
		m.debug("REGISTERED: %x %s", msg.sender, msg.addr.String())
	}
	return m.sendWelcome(msg.addr)
}

func (m *Manager) processWelcome(msg *message) error {
	if m.ins.add(msg.sender, msg.addr) {
		m.debug("REGISTERED: %x %s", msg.sender, msg.addr.String())
	}
	return nil
}

func (m *Manager) processCompare(msg *message) error {
	if bytes.Equal(m.ins.hashAllID(m.id), msg.data) {
		return m.sendCompared(msg.addr, msg.data)
	}
	return nil
}

func (m *Manager) processMine(msg *message) error {
	if saveErr := m.ins.save(msg.sender, string(msg.data), msg.addr); saveErr != nil {
		// err = m.sendReset(msg.data) not yours
		m.debug("OWNERSHIP IGNORED %x: %s %+v", msg.sender, string(msg.data), saveErr)
	} else {
		m.debug("OWNERSHIP APPROVED %x: %s", msg.sender, string(msg.data))
		return m.sendSaved(msg.addr, msg.sender, msg.data)
	}
	return nil
}

func (m *Manager) processWant(msg *message) error {
	switch owner := m.ins.search(string(msg.data)); owner {
	case Mine:
		return m.sendRegistered(msg.addr, msg.data)
	case "":
		if m.own.Add(msg.sender, string(msg.data)) {
			m.debug("OWNERSHIP CANDIDATE %x: %s", msg.sender[:], string(msg.data))
			return m.sendCandidate(msg.addr, msg.sender, msg.data)
		}
	default:
		return m.sendThatIsOccupied(msg.addr, m.ins.getInstanceID(owner), msg.data)

	}
	return nil
}

func (m *Manager) processRegistered(msg *message) error {
	if saveErr := m.ins.save(msg.sender, string(msg.data), msg.addr); saveErr != nil {
		m.warning("OWNERSHIP RESET %x: %s %+v", msg.sender, string(msg.data), saveErr)
		return m.sendReset(msg.data) // not yours
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
