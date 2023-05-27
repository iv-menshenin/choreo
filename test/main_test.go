package test

import (
	"context"
	"github.com/iv-menshenin/choreo/fleetctrl"
	"sync"
	"testing"

	"github.com/iv-menshenin/choreo/transport"
)

type (
	Service interface {
		CheckKey(ctx context.Context, key string) (string, error)
		Keys(ctx context.Context) []string
		NotifyArmed() <-chan struct{}
		Stop()
	}
	TestFleet struct {
		ctx context.Context
		t   *testing.T

		fleet map[string]Service
		done  chan struct{}
		close chan struct{}

		wg  sync.WaitGroup
		net interface {
			NewListener() *transport.DummyListener
		}
	}
)

func NewFleet(t *testing.T, ctx context.Context, cnt int) *TestFleet {
	var test = TestFleet{
		ctx: ctx,
		t:   t,

		fleet: make(map[string]Service),
		done:  make(chan struct{}),
		close: make(chan struct{}),

		net: transport.NewDummy(),
	}
	for n := 0; n < cnt; n++ {
		test.Grow()
	}
	go func() {
		test.wg.Wait()
		close(test.done)
	}()
	for _, v := range test.fleet {
		<-v.NotifyArmed()
	}
	return &test
}

func (s *TestFleet) Grow() {
	s.wg.Add(1)
	listener := s.net.NewListener()
	c := fleetctrl.New(
		listener,
	)
	c.SetLogLevel(fleetctrl.LogLevelDebug)
	s.fleet[listener.GetIP().String()] = c
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
	<-c.NotifyArmed()
}

func (s *TestFleet) StopOne() []string {
	var k string
	var v Service
	for k, v = range s.fleet {
		break
	}
	delete(s.fleet, k)
	var keys = v.Keys(context.Background())
	v.Stop()
	return keys
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
