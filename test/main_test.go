package test

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
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

func Test_Discovery(t *testing.T) {
	const (
		nodesCount = 9
		initTime   = 15 * time.Second
		msgCount   = 100
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
	)
	for n := 0; n < msgCount; n++ {
		idx, svc := fleet.getService()
		iterNum := atomic.AddInt64(&counter, 1)
		v, err := svc.CheckKey(context.Background(), t.Name())
		if err != nil {
			t.Errorf("CheckKey error: %+v", err)
		}
		if iterNum == 1 {
			keeper = idx
			continue
		}
		v = strings.Split(v, ":")[0] // remove port
		if idx == keeper {
			if v != fleetctrl.Mine {
				t.Errorf("expected %+v, got %+v", fleetctrl.Mine, v)
			}
			continue
		}
		if keeper != v {
			t.Errorf("expected %+v, got %+v", keeper, v)
		}
	}
	fleet.Close()
	if err := fleet.waitStop(ctx); err != nil {
		t.Errorf("ERROR AT STOPPING: %v", err)
	}
}

func Test_Performance(t *testing.T) {
	const (
		nodesCount = 5
		initTime   = 15 * time.Second
		msgCount   = 10000
		msgClass   = 16
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
		keeper  [msgClass]string
		muxes   [msgClass]sync.Mutex
		counter int64
		wg      sync.WaitGroup
	)
	wg.Add(msgCount)
	for n := 0; n < msgCount; n++ {
		go func() {
			defer wg.Done()
			iterNum := atomic.AddInt64(&counter, 1)
			cc := iterNum % msgClass
			mx := &muxes[cc]
			idx, svc := fleet.getService()
			v, err := svc.CheckKey(context.Background(), fmt.Sprintf("%s-%d", t.Name(), cc))
			if err != nil {
				t.Errorf("CheckKey error: %+v", err)
			}
			if v == fleetctrl.Mine {
				return
			}
			mx.Lock()
			keeper[cc] = idx
			mx.Unlock()
		}()
	}
	var done = make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-done:
	}
	fleet.Close()
	if err := fleet.waitStop(ctx); err != nil {
		t.Errorf("ERROR AT STOPPING: %v", err)
	}
}
