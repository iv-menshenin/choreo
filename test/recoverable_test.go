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

func TestRecoverability(t *testing.T) {
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

	fleet := newTestFleet(ctx, t, nodesCount)

	select {
	case <-ctx.Done():
		t.Fatal("TIMEOUT ERROR")

	default:
		// start test
	}

	var keeper [msgClass]string
	for iterNum := 0; iterNum < msgCount; iterNum++ {
		var host string
		cc := iterNum % msgClass
		idx, svc := fleet.getService()
		msg := fmt.Sprintf("%s-%d", t.Name(), cc)
		for {
			v, err := svc.CheckKey(context.Background(), msg)
			if errors.Is(err, fleetctrl.ErrNotReady) {
				<-time.After(50 * time.Millisecond)
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			host = idx
			if !v.Me() {
				host = strings.Split(v.NetAddr().String(), ":")[0]
			}
			break
		}
		if keeper[cc] == "" {
			keeper[cc] = idx
			continue
		}
		if keeper[cc] != host {
			t.Errorf("expected %s, got: %s", keeper[cc], host)
		}
	}

	lostIP, stopped := fleet.StopOne()
	missedKeys := stopped.Keys(context.Background())
	missedID := stopped.ID()

	fmt.Printf("%+v\n", missedKeys)
	var rebased = make(map[string]struct{})
	for _, k := range missedKeys {
		rebased[k] = struct{}{}
	}

	recovered := fleet.ReturnBack(missedID)
	for iterNum := 0; iterNum < msgCount; iterNum++ {
		var host string
		cc := iterNum % msgClass
		idx, svc := fleet.getService()
		msg := fmt.Sprintf("%s-%d", t.Name(), cc)
		for {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			default:
				// go ahead
			}
			v, err := svc.CheckKey(ctx, msg)
			if errors.Is(err, fleetctrl.ErrNotReady) {
				<-time.After(50 * time.Millisecond)
				continue
			}
			if err != nil {
				t.Error(err)
				continue
			}
			if strings.Split(host, ":")[0] == lostIP {
				<-time.After(250 * time.Millisecond)
				continue
			}
			host = idx
			if !v.Me() {
				host = strings.Split(v.NetAddr().String(), ":")[0]
			}
			break
		}
		if keeper[cc] == idx {
			continue
		}
		if _, ok := rebased[msg]; ok {
			if host == lostIP {
				<-time.After(250 * time.Millisecond)
				continue
			}
			if host != recovered {
				t.Errorf("recovered %s, but refered to: %s (%s)", recovered, host, lostIP)
			}
			continue
		}
		if kcc := keeper[cc]; kcc != "" && keeper[cc] != host {
			t.Errorf("expected %s, got: %s", kcc, host)
		}
	}

	fleet.Close()
	if err := fleet.waitStop(ctx); err != nil {
		t.Errorf("ERROR AT STOPPING: %v", err)
	}
}
