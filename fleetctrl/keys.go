package fleetctrl

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl/id"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/election/ownership"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/send"
)

const (
	// MaximumKeyLength limits the allowed length of the key.
	MaximumKeyLength = 128
)

var (
	ErrNotReady     = errors.New("not ready")
	ErrKeyTooLong   = errors.New("key-string too long")
	ErrAwaitTimeout = errors.New("timeout")
	ErrWrongState   = errors.New("wrong state")
)

type Shard interface {
	Me() bool
	Live() bool
	NetAddr() net.Addr
	ShardID() id.ID
}

func (m *Manager) CheckKey(ctx context.Context, key string) (Shard, error) {
	if !m.Status() {
		return nil, ErrNotReady
	}
	if len(key) > MaximumKeyLength {
		return nil, ErrKeyTooLong
	}

	for {
		if instance, ok := m.keeper.Lookup(key); ok {
			return &instance, nil
		}
		owned, err := m.tryToOwn(key)
		if err != nil {
			return nil, err
		}
		if owned {
			return m.keeper.Me(), nil
		}
		if instance, ok := m.keeper.Lookup(key); ok {
			return &instance, nil
		}

		repeatAfter := insureDurationGreaterProc(ownership.DefaultTimeout)
		m.warning("CANT OWN (RETRY AFTER %v): %v", repeatAfter, err)
		select {
		case <-ctx.Done():
			m.giveUpOwn(key)
			return nil, fmt.Errorf("can't get ownership: %w", ctx.Err())

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
	chErr := m.awaitMostOf(send.CmdCandidate, m.id[:], []byte(key))
	if err := m.sr.WantKey(key); err != nil {
		return false, err
	}
	if err := <-chErr; err != nil {
		m.warning("OWNERSHIP DENIED %s: %+v", m.id, err)
		return false, nil
	}
	if !m.keeper.Own(key) {
		m.warning("OWNERSHIP FAIL %s", m.id)
		return false, nil
	}
	chErr = m.awaitMostOf(send.CmdSaved, m.id[:], []byte(key))
	if err := m.sr.ThatIsMine(key); err != nil {
		return false, err
	}
	if err := <-chErr; err != nil {
		m.keeper.Forget(key)
		m.warning("OWNERSHIP CANCELLED %s: %+v", m.id, err)
		return false, nil
	}
	return true, nil
}

func (m *Manager) giveUpOwn(key string) {
	m.own.Del(key)
}

type Cmder interface {
	CmdData(id.ID, ...[]byte) []byte
}

func (m *Manager) awaitMostOf(cmd Cmder, data ...[]byte) <-chan error {
	ctx, clear := context.WithTimeout(context.Background(), halfDurationOf(ownership.DefaultTimeout))
	m.debug("AWAITING %s: %s %x", m.id, cmd, data)

	var (
		waitAll          sync.WaitGroup
		quorumCounter    = int64(m.keeper.Len()+1) / 2
		awaitingCancel   = make(chan struct{})
		electionComplete = make(chan struct{})
		quorumComplete   = make(chan struct{})
		result           = make(chan error, 1)
		electorate       = m.keeper.IDs()
	)
	waitAll.Add(len(electorate))

	for _, v := range electorate {
		var (
			answered = make(chan struct{})
			retCmd   = cmd.CmdData(v, data...)
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

func (m *Manager) Keys(context.Context) []string {
	return m.keeper.Mine()
}
