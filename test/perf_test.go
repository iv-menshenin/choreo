package test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl"
)

func Test_Performance(t *testing.T) {
	t.Parallel()

	var out = bytes.NewBuffer(nil)
	log.SetOutput(out)
	defer func() {
		if t.Failed() {
			fmt.Print(out.String())
		}
	}()

	const (
		nodesCount = 5
		initTime   = 30 * time.Second
		msgCount   = 100000
		msgClass   = 16
		parallels  = 64
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
		band    = make(chan struct{}, parallels)
		keeper  [msgClass]string
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
			mx := &muxes[cc]
			idx, svc := fleet.getService()
			v, err := svc.CheckKey(context.Background(), fmt.Sprintf("%s-%d", t.Name(), cc))
			if err != nil {
				t.Errorf("CheckKey error: %+v", err)
			}
			if v == fleetctrl.Mine {
				if keeper[cc] == "" || keeper[cc] == idx {
					keeper[cc] = idx
				}
				return
			}
			v = strings.Split(v, ":")[0] // remove port
			mx.Lock()
			if kcc := keeper[cc]; kcc != "" && keeper[cc] != v {
				t.Errorf("expected %s, got: %s", kcc, v)
			}
			keeper[cc] = v
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
