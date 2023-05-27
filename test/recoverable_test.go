package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl"
)

func Test_Recoverability(t *testing.T) {
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

	var keeper [msgClass]string
	for iterNum := 0; iterNum < msgCount; iterNum++ {
		cc := iterNum % msgClass
		idx, svc := fleet.getService()
		v, err := svc.CheckKey(context.Background(), fmt.Sprintf("%s-%d", t.Name(), cc))
		if errors.Is(err, fleetctrl.ErrNotReady) {
			<-time.After(time.Second)
			continue
		}
		if v == fleetctrl.Mine {
			if keeper[cc] == "" || keeper[cc] == idx {
				keeper[cc] = idx
			}
			continue
		}
		v = strings.Split(v, ":")[0] // remove port
		if kcc := keeper[cc]; kcc != "" && keeper[cc] != v {
			t.Errorf("expected %s, got: %s", kcc, v)
		}
		keeper[cc] = v
	}

	var missedKeys = fleet.StopOne()
	fmt.Printf("%+v\n", missedKeys)
	var rebased = make(map[string]struct{})
	for _, k := range missedKeys {
		rebased[k] = struct{}{}
	}

	fleet.Grow()
	for iterNum := 0; iterNum < msgCount; iterNum++ {
		cc := iterNum % msgClass
		idx, svc := fleet.getService()
		msg := fmt.Sprintf("%s-%d", t.Name(), cc)
		v, err := svc.CheckKey(context.Background(), msg)
		if errors.Is(err, fleetctrl.ErrNotReady) {
			<-time.After(time.Second)
			continue
		}
		if v == fleetctrl.Mine && keeper[cc] == idx {
			continue
		}
		v = strings.Split(v, ":")[0] // remove port
		if kcc := keeper[cc]; kcc != "" && keeper[cc] != v {
			if _, ok := rebased[msg]; !ok {
				t.Errorf("expected %s, got: %s", kcc, v)
			}
		}
	}

	fleet.Close()
	if err := fleet.waitStop(ctx); err != nil {
		t.Errorf("ERROR AT STOPPING: %v", err)
	}
}
