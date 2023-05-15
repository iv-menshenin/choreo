package test

import (
	"context"
	"github.com/iv-menshenin/choreo/fleetctrl"
	"github.com/iv-menshenin/choreo/transport"
	"sync"
	"testing"
)

type (
	Service interface {
		CheckKey(ctx context.Context, key string) (string, error)
		NotifyArmed() <-chan struct{}
	}
	TestFleet struct {
		fleet map[string]Service
		done  chan struct{}
		close chan struct{}
	}
)

func NewFleet(t *testing.T, ctx context.Context, cnt int) *TestFleet {
	var network = transport.NewDummy()
	var test = TestFleet{
		fleet: make(map[string]Service),
		done:  make(chan struct{}),
		close: make(chan struct{}),
	}
	var wg sync.WaitGroup
	for n := 0; n < cnt; n++ {
		wg.Add(1)
		listener := network.NewListener()
		c := fleetctrl.New(
			listener,
		)
		test.fleet[listener.GetIP().String()] = c
		go func() {
			select {
			case <-ctx.Done():
			case <-test.close:
			}
			c.Stop()
		}()
		go func() {
			defer wg.Done()
			if err := c.Manage(); err != nil {
				t.Errorf("Manager stopped with %+v", err)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(test.done)
	}()
	for _, v := range test.fleet {
		<-v.NotifyArmed()
	}
	return &test
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
