package fleetctrl

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl/id"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/election/ownership"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/election/waitfor"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/send"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/shardkeeper"
)

type (
	Manager struct {
		id id.ID
		ll LogLevel
		ls send.Transport
		sr *send.Sender

		state int64
		done  chan struct{}
		err   error
		once  sync.Once

		keeper *shardkeeper.Keeper
		awr    *waitfor.Awaiter
		own    *ownership.Ownership

		stopA int64
		stopX chan struct{}

		armedMux sync.Mutex
		armedFlg bool
		armedCh  []chan<- struct{}
	}
	LogLevel uint8
)

const (
	LogLevelError LogLevel = iota
	LogLevelWarning
	LogLevelDebug
)

const (
	StateCreated int64 = iota
	StateDiscovery
	StateStop
)

func New(id id.ID, transport send.Transport, options *Options) *Manager {
	return &Manager{
		ll:     LogLevelError,
		id:     id,
		ls:     transport,
		sr:     send.New(id, transport),
		state:  StateCreated,
		done:   make(chan struct{}),
		keeper: shardkeeper.New(id, options.shardOptions()),
		awr:    waitfor.New(),
		own:    ownership.New(nil),

		stopX: make(chan struct{}),
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

func (m *Manager) ID() id.ID {
	return m.id
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
	if atomic.CompareAndSwapInt64(&m.stopA, 0, 1) {
		close(m.stopX)
	}
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

type msgID uint8

const (
	evtNothing msgID = iota
	evtCheck
	evtDiscovery
)

func (m *Manager) stateLoop() {
	defer close(m.done)

	var events = make(chan msgID)
	go m.initMessaging(events)

	for event := range events {
		switch event {
		case evtNothing:
			m.warning("%s STATE HASH <<< %x", m.id, m.keeper.Hash())

		case evtCheck:
			m.readyStateAction()

		case evtDiscovery:
			m.warning("DISCOVERY %s", m.id)
			m.discoveryStateAction()
		}
	}
}

func (m *Manager) initMessaging(events chan<- msgID) {
	// first of all we need to start discovery
	events <- evtDiscovery

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		loopMessaging(m.stopX, events, time.Second, evtNothing)
	}()
	go func() {
		defer wg.Done()
		loopMessaging(m.stopX, events, 10*time.Second, evtCheck)
	}()
	go func() {
		defer wg.Done()
		loopMessaging(m.stopX, events, 5*time.Second, evtDiscovery)
	}()

	wg.Wait()
	close(events)
}

func loopMessaging(stopChan <-chan struct{}, sendChan chan<- msgID, duration time.Duration, msgID msgID) {
	for {
		rnd := time.Duration((rand.Float64()+1)*50) * time.Millisecond
		select {
		case <-stopChan:
			return
		case <-time.After(duration + rnd):
			// tick
		}
		select {
		case sendChan <- msgID:
			// sent
		case <-stopChan:
			return
		}
	}
}

func (m *Manager) readyStateAction() {
	del := m.keeper.Cleanup()
	if del > 0 {
		m.warning("UNLINKED: %d", del)
	}
	if err := m.checkArmedStatus(); err != nil {
		m.setErr(err)
		return
	}
}

func (m *Manager) discoveryStateAction() {
	atomic.StoreInt64(&m.state, StateDiscovery)
	if err := m.sr.KnockKnock(); err != nil {
		m.setErr(err)
		return
	}
}

func (m *Manager) checkArmedStatus() (err error) {
	m.debug("CHECK STATUS")
	var (
		cnt  = 3
		hash = m.keeper.Hash()
	)
	for {
		errCh := m.awaitMostOf(send.CmdCompared, hash[:])
		if err = m.sr.CompareInstances(hash[:]); err != nil {
			return fmt.Errorf("can't ARM, checking error: %w", err)
		}
		err = <-errCh
		if cnt--; err == nil || cnt < 0 {
			break
		}
		<-time.After(time.Duration((rand.Float64()+1)*15) * time.Millisecond)
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
		atomic.StoreInt64(&m.state, StateStop)
	})
}
