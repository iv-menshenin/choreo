package test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl"
)

func Test_Discovery(t *testing.T) {
	var out = bytes.NewBuffer(nil)
	log.SetOutput(out)
	defer func() {
		if t.Failed() {
			fmt.Print(out.String())
		}
	}()

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
