package fleetctrl

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const Mine = "MINE"

var (
	mux sync.Mutex
	chw = make(map[string]chan struct{})
)

func (m *Manager) CheckKey(ctx context.Context, key string) (string, error) {
	if atomic.LoadInt64(&m.armed) != Armed {
		return "", errors.New("not ready")
	}
	if len(key) > 128 {
		return "", errors.New("too long key")
	}
	var o sync.Once
	var after = func() {}
	defer func() {
		after()
	}()
	for {
		if instance := m.ins.search(key); instance != "" {
			return instance, nil
		}
		o.Do(func() {
			mux.Lock()
			ch, wait := chw[key]
			if !wait {
				ch = make(chan struct{})
				chw[key] = ch
				after = func() {
					close(ch)
					mux.Lock()
					delete(chw, key)
					mux.Unlock()
				}
			}
			mux.Unlock()
			if wait {
				<-ch
			}
		})
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
		select {
		case <-ctx.Done():
			return "", ctx.Err()

		case <-time.After(time.Duration(rand.Intn(50)+50) * time.Millisecond):
			// next trying
		}
	}
}

func (m *Manager) tryToOwn(key string) (bool, error) {
	if !m.own.Add(m.id, key) {
		return false, nil
	}
	chErr := m.awaitMostOf(cmdCandidate, append(append(make([]byte, 0, 16+len(key)), m.id[:]...), []byte(key)...))
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
	chErr = m.awaitMostOf(cmdSaved, append(append(make([]byte, 0, 16+len(key)), m.id[:]...), []byte(key)...))
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

var defaultAwaitTimeout = 50 * time.Millisecond

func (m *Manager) awaitMostOf(cmd cmd, data []byte) <-chan error {
	m.debug("AWAITING %x: %s %x", m.id, cmd, data)

	var quorum = int64(m.ins.getCount()+1) / 2
	var wg sync.WaitGroup
	var cancel = make(chan struct{})
	var done = make(chan struct{})
	var kvo = make(chan struct{})
	var timeout = make(chan struct{})
	var result = make(chan error, 1)

	go func() {
		select {
		case <-done:
		case <-kvo:
		case <-time.After(defaultAwaitTimeout):
		}
		close(timeout)
		close(cancel)
	}()
	for _, v := range m.ins.getAllID() {
		wg.Add(1)
		var ch = make(chan struct{})
		go func(id int64) {
			select {
			case <-ch:
				if cnt := atomic.AddInt64(&quorum, -1); cnt == 0 {
					close(kvo)
				}
			case <-cancel:
			}
			m.awr.Del(id)
			wg.Done()
		}(m.awr.Add(append(append(append(make([]byte, 0, 20+len(data)), cmd[:]...), v[:]...), data...), ch))
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	go func() {
		select {
		case <-timeout:
			result <- errors.New("timeout")
		case <-done:
		case <-kvo:
		}
		close(result)
	}()
	return result
}
