package test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl"
	"github.com/iv-menshenin/choreo/transport"
)

type (
	Mr interface {
		CheckKey(ctx context.Context, key string) (string, error)
		NotifyArmed() <-chan struct{}
	}
)

func Test_Discovery(t *testing.T) {
	const (
		nodesCount = 10
		initTime   = 15 * time.Second
		msgCount   = 1000
	)
	var (
		done     = make(chan struct{})
		network  = transport.NewDummy()
		balancer = make(map[int]Mr)
	)
	log.SetFlags(log.Lmicroseconds)

	for n := 0; n < nodesCount; n++ {
		c := fleetctrl.New(
			network.NewListener(),
		)
		balancer[n] = c
		go func() {
			<-done
			c.Stop()
		}()
		go func() {
			if err := c.Manage(); err != nil {
				t.Errorf("Manager stopped with %+v", err)
			}
		}()
	}
	select {
	case <-time.After(initTime):
		t.Error("timeout exceeded")
		return

	case <-balancer[0].NotifyArmed():
		// inititalization OK
	}
	time.Sleep(initTime)
	_, err := balancer[0].CheckKey(context.Background(), "test")
	if err != nil {
		t.Errorf("First CheckKey invocation %+v", err)
	}
	var keeper string
	for n := 0; n < msgCount; n++ {
		for idx, b := range balancer {
			v, err := b.CheckKey(context.Background(), "test")
			if err != nil {
				t.Errorf("CheckKey error: %+v", err)
			}
			switch idx {
			case 0:
				if v != "MINE" {
					t.Errorf("expected MINE, got %+v", v)
				}
			default:
				if keeper == "" {
					keeper = v
				}
				if keeper != v {
					t.Errorf("expected %+v, got %+v", v, keeper)
				}
			}
			break
		}
	}
	close(done)
}
