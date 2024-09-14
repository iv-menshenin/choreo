package client

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl/internal/send"
	"github.com/iv-menshenin/choreo/transport"
)

type Options struct {
	DiscoveryPort     uint16
	DiscoveryInterval time.Duration
	Transport         send.Transport
	Logger            Printfer
}

type Printfer interface {
	Printf(string, ...any)
}

func (o *Options) normalization() error {
	if o.DiscoveryPort == 0 {
		return fmt.Errorf("discovery port must not be zero")
	}
	if o.DiscoveryInterval == 0 {
		o.DiscoveryInterval = 5 * time.Second
	}
	if o.Transport == nil {
		var err error
		o.Transport, err = transport.NewUDP(o.DiscoveryPort)
		if err != nil {
			return fmt.Errorf("can't make transport: %v", err)
		}
	}
	if o.Logger == nil {
		o.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	return nil
}
