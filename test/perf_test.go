package test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl/id"
)

func TestPerformance(t *testing.T) {
	var out = bytes.NewBuffer(nil)
	log.SetOutput(out)
	defer func() {
		if t.Failed() {
			fmt.Print(out.String())
		}
	}()

	const (
		nodesCount = 5
		initTime   = 3 * time.Minute
		msgCount   = 1000000
		msgClass   = 37
		parallels  = 64
	)
	log.SetFlags(log.Lmicroseconds)
	ctx, cancel := context.WithTimeout(context.Background(), initTime)
	defer cancel()

	fleet := newTestFleet(ctx, t, nodesCount)

	select {
	case <-ctx.Done():
		t.Error("TIMEOUT ERROR")

	default:
		// start test
	}

	var (
		band    = make(chan struct{}, parallels)
		keeper  [msgClass]id.ID
		muxes   [msgClass]sync.Mutex
		counter int64
		wg      sync.WaitGroup
	)
	wg.Add(msgCount)
	for n := 0; n < msgCount; n++ {
		go func() {
			defer wg.Done()
			band <- struct{}{}
			defer func() {
				<-band
			}()

			iterNum := atomic.AddInt64(&counter, 1)
			cc := iterNum % msgClass

			_, svc := fleet.getService()
			key := fmt.Sprintf("%s-%d", t.Name(), cc)
			v, err := svc.CheckKey(context.Background(), key)
			if err != nil {
				t.Errorf("CheckKey error: %+v", err)
			}

			mx := &muxes[cc]
			mx.Lock()
			var x = v.ShardID()
			if v.Me() {
				x = svc.ID()
			}
			if keeper[cc] == id.NullID {
				keeper[cc] = x
				mx.Unlock()
				return
			}
			if keeper[cc] != x {
				t.Errorf("expected %q, got %q", keeper[cc], v.ShardID())
			}
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
