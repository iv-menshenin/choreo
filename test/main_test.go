package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl"
	"github.com/iv-menshenin/choreo/fleetctrl/id"
	"github.com/iv-menshenin/choreo/transport"
)

var (
	persistentKeys = fleetctrl.Options{
		PersistentKeys: true,
	}
)

type (
	Service interface {
		ID() id.ID
		CheckKey(ctx context.Context, key string) (fleetctrl.Shard, error)
		Keys(ctx context.Context) []string
		NotifyArmed() <-chan struct{}
		Stop()
	}
	TestFleet struct {
		ctx context.Context
		t   *testing.T

		mux   sync.Mutex
		fleet map[string]Service
		done  chan struct{}
		close chan struct{}

		wg  sync.WaitGroup
		net interface {
			NewListener() *transport.DummyListener
			Close()
		}
	}
)

func newTestFleet(ctx context.Context, t *testing.T, cnt int) *TestFleet {
	var test = TestFleet{
		ctx: ctx,
		t:   t,

		fleet: make(map[string]Service),
		done:  make(chan struct{}),
		close: make(chan struct{}),

		net: transport.NewDummy(),
	}

	var wg sync.WaitGroup
	wg.Add(cnt)
	for n := 0; n < cnt; n++ {
		go func() {
			defer wg.Done()
			test.Grow()
		}()
	}
	wg.Wait()

	go func() {
		test.wg.Wait()
		test.net.Close()
		time.Sleep(time.Second)

		close(test.done)
	}()
	for _, v := range test.fleet {
		select {
		case <-ctx.Done():
			// bad
		case <-v.NotifyArmed():
			// ok
		}
	}
	return &test
}

func (s *TestFleet) ReturnBack(missedID id.ID) string {
	s.wg.Add(1)
	listener := s.net.NewListener()
	c := fleetctrl.New(
		missedID,
		listener,
		&persistentKeys,
	)
	c.SetLogLevel(fleetctrl.LogLevelDebug)
	addr := listener.GetIP().String()
	s.registerAndListen(addr, c)
	return addr
}

func (s *TestFleet) Grow() {
	s.wg.Add(1)
	listener := s.net.NewListener()
	c := fleetctrl.New(
		id.New(),
		listener,
		&persistentKeys,
	)
	c.SetLogLevel(fleetctrl.LogLevelDebug)
	s.registerAndListen(listener.GetIP().String(), c)
}

func (s *TestFleet) registerAndListen(key string, c *fleetctrl.Manager) {
	s.mux.Lock()
	s.fleet[key] = c
	s.mux.Unlock()

	go func() {
		select {
		case <-s.ctx.Done():
		case <-s.close:
		}
		c.Stop()
	}()
	go func() {
		defer s.wg.Done()
		if err := c.Manage(); err != nil {
			s.t.Errorf("Manager stopped with %+v", err)
		}
	}()
	select {
	case <-s.ctx.Done():
		// bad
	case <-c.NotifyArmed():
		// good
	}
}

func (s *TestFleet) StopOne() (string, Service) {
	var k string
	var v Service
	for k, v = range s.fleet {
		break
	}
	delete(s.fleet, k)
	v.Stop()
	return k, v
}

func (s *TestFleet) getService() (string, Service) {
	for k, v := range s.fleet {
		return k, v
	}
	return "", nil
}

func (s *TestFleet) waitStop(ctx context.Context) error {
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *TestFleet) Close() {
	close(s.close)
}
