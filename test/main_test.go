package test

import (
	"context"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl"
	"github.com/iv-menshenin/choreo/transport"
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
	for n := 0; n < cnt; n++ {
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
			defer close(test.done)
			if err := c.Manage(); err != nil {
				t.Errorf("Manager stopped with %+v", err)
			}
		}()
	}
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

func Test_Discovery(t *testing.T) {
	const (
		nodesCount = 10
		initTime   = 15 * time.Second
		msgCount   = 1000
	)
	log.SetFlags(log.Lmicroseconds)
	ctx, cancel := context.WithTimeout(context.Background(), initTime)
	defer cancel()

	fleet := NewFleet(t, ctx, nodesCount)

	select {
	case <-ctx.Done():
		t.Error("TIMEOUT ERROR")

	default:
		// start test
	}

	var (
		keeper  string
		counter int64
		init    = make(chan struct{})
	)
	for n := 0; n < msgCount; n++ {
		idx, svc := fleet.getService()
		iterNum := atomic.AddInt64(&counter, 1)
		if iterNum > 1 {
			<-init
		}
		go func() {
			v, err := svc.CheckKey(context.Background(), "test")
			if err != nil {
				t.Errorf("CheckKey error: %+v", err)
			}
			if iterNum == 1 {
				keeper = idx
				close(init)
				return
			}
			v = strings.Split(v, ":")[0] // remove port
			if idx == keeper {
				if v != fleetctrl.Mine {
					t.Errorf("expected %+v, got %+v", fleetctrl.Mine, v)
				}
				return
			}
			if keeper != v {
				t.Errorf("expected %+v, got %+v", keeper, v)
			}
		}()
	}
	fleet.Close()
	if err := fleet.waitStop(ctx); err != nil {
		t.Errorf("ERROR AT STOPPING: %v", err)
	}
}
