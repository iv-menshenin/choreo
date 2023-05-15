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
	m.debug("AWAITING %x: %s %x", m.id[:], cmd, data)

	var quorum = int64(m.ins.getCount()+1) / 2
	var waitAll sync.WaitGroup
	var cancel = make(chan struct{})
	var done = make(chan struct{})
	var kvo = make(chan struct{})
	var timeout = make(chan struct{})
	var result = make(chan error, 1)

	go func() {
		defer close(cancel)
		select {
		case <-done:
		case <-kvo:
		case <-time.After(halfDurationOf(ownership.DefaultTimeout)):
			close(timeout)
		}
	}()

	for _, v := range m.ins.getAllID() {
		waitAll.Add(1)

		var (
			answered = make(chan struct{})
			retCmd   = makeCmdData(cmd[:], v[:], data)
			awaitID  = m.awr.Add(retCmd, answered)
		)
		go func() {
			select {
			case <-answered:
				if cnt := atomic.AddInt64(&quorum, -1); cnt == 0 {
					close(kvo)
				}
			case <-cancel:
			}
			m.awr.Del(awaitID)
			waitAll.Done()
		}()
	}

	go func() {
		waitAll.Wait()
		close(done)
	}()
	go func() {
		select {
		case <-timeout:
			result <- ErrAwaitTimeout
		case <-done:
		case <-kvo:
		}
		close(result)
	}()
	return result
}
