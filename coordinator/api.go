package coordinator

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
