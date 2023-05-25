package fleetctrl

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl/internal/election/ownership"
)

const (
	// Mine is a special address constant, indicating that the key belongs to the called process itself.
	Mine = "MINE"
	// MaximumKeyLength limits the allowed length of the key.
	MaximumKeyLength = 128
)

var (
	ErrNotReady     = errors.New("not ready")
	ErrKeyTooLong   = errors.New("key-string too long")
	ErrBadMessage   = errors.New("bad message")
	ErrAwaitTimeout = errors.New("timeout")
	ErrWrongState   = errors.New("wrong state")
)

func (m *Manager) CheckKey(ctx context.Context, key string) (string, error) {
	if !m.Status() {
		return "", ErrNotReady
	}
	if len(key) > MaximumKeyLength {
		return "", ErrKeyTooLong
	}

	for {
		if instance := m.ins.search(key); instance != "" {
			return instance, nil
		}
		owned, err := m.tryToOwn(key)
		if err != nil {
			return "", err
		}
		if owned {
			return Mine, nil
		}
		if instance := m.ins.search(key); instance != "" {
			return instance, nil
		}

		repeatAfter := insureDurationGreaterProc(ownership.DefaultTimeout)
		m.warning("CANT OWN (RETRY AFTER %v): %v", repeatAfter, err)
		select {
		case <-ctx.Done():
			m.giveUpOwn(key)
			return "", fmt.Errorf("can't get ownership: %w", ctx.Err())

		case <-time.After(repeatAfter):
			// next trying
			continue
		}
	}
}

func (m *Manager) tryToOwn(key string) (bool, error) {
	if !m.own.Add(m.id, key) {
		return false, nil
	}
	chErr := m.awaitMostOf(cmdCandidate, makeCmdData(m.id[:], []byte(key)))
	if err := m.sendWantKey(key); err != nil {
		return false, err
	}
	if err := <-chErr; err != nil {
		m.warning("OWNERSHIP DENIED %x: %+v", m.id, err)
		return false, nil
	}
	if !m.ins.toOwn(key) {
		m.warning("OWNERSHIP BREAKED %x", m.id)
		return false, nil
	}
	chErr = m.awaitMostOf(cmdSaved, makeCmdData(m.id[:], []byte(key)))
	if err := m.sendThatIsMine(key); err != nil {
		return false, err
	}
	if err := <-chErr; err != nil {
		m.ins.fromOwn(key)
		m.warning("OWNERSHIP CANCELLED %x: %+v", m.id, err)
		return false, nil
	}
	return true, nil
}

func (m *Manager) giveUpOwn(key string) {
	m.own.Del(key)
}

func (m *Manager) awaitMostOf(cmd cmd, data []byte) <-chan error {
	ctx, clear := context.WithTimeout(context.Background(), halfDurationOf(ownership.DefaultTimeout))
	m.debug("AWAITING %x: %s %x", m.id[:], cmd, data)

	var (
		waitAll          sync.WaitGroup
		quorumCounter    = int64(m.ins.getCount()+1) / 2
		awaitingCancel   = make(chan struct{})
		electionComplete = make(chan struct{})
		quorumComplete   = make(chan struct{})
		result           = make(chan error, 1)
		electorate       = m.ins.getAllID()
	)
	waitAll.Add(len(electorate))

	for _, v := range electorate {
		var (
			answered = make(chan struct{})
			retCmd   = makeCmdData(cmd[:], v[:], data)
			awaitID  = m.awr.Add(retCmd, answered)
		)
		go func() {
			defer waitAll.Done()
			defer m.awr.Del(awaitID)
			select {
			case <-answered:
				if cnt := atomic.AddInt64(&quorumCounter, -1); cnt == 0 {
					close(quorumComplete)
				}
			case <-awaitingCancel:
			}
		}()
	}

	go func() {
		defer clear()
		waitAll.Wait()
		close(electionComplete)
	}()
	go waitFor(ctx, quorumComplete, electionComplete, result, awaitingCancel)
	return result
}

func waitFor(ctx context.Context, quo, all <-chan struct{}, result chan<- error, afterAll chan<- struct{}) {
	select {
	case <-ctx.Done():
		result <- ErrAwaitTimeout
	case <-quo:
	case <-all:
	}
	close(result)
	close(afterAll)
}

func (m *Manager) Keys(ctx context.Context) []string {
	return m.ins.getMine()
}
