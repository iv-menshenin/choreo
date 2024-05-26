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
)

func TestClusterActivation(t *testing.T) {
	t.Parallel()

	var out = bytes.NewBuffer(nil)
	log.SetOutput(out)
	defer func() {
		if t.Failed() {
			fmt.Print(out.String())
		}
	}()

	const (
		nodesCount = 18
		initTime   = 30 * time.Second
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

	fleet.Close()
}

func TestDiscoveryCluster(t *testing.T) {
	t.Parallel()

	var out = bytes.NewBuffer(nil)
	log.SetOutput(out)
	defer func() {
		if t.Failed() {
			fmt.Print(out.String())
		}
	}()

	const (
		nodesCount = 9
		initTime   = 30 * time.Second
		msgCount   = 100000
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
		keeper  string
		counter int64
	)
	for n := 0; n < msgCount; n++ {
		host, svc := fleet.getService()
		iterNum := atomic.AddInt64(&counter, 1)
		v, err := svc.CheckKey(context.Background(), t.Name())
		if err != nil {
			t.Errorf("CheckKey error: %+v", err)
			break
		}
		if iterNum == 1 {
			// used just one key, so all shards will answer the same
			keeper = host
			continue
		}
		var gotHost = host
		if !v.Me() {
			gotHost = strings.Split(v.NetAddr().String(), ":")[0] // remove port
		}
		if gotHost != keeper {
			t.Errorf("expected %q, got %q", keeper, gotHost)
		}
	}
	fleet.Close()
	if err := fleet.waitStop(ctx); err != nil {
		t.Errorf("ERROR AT STOPPING: %v", err)
	}
}
