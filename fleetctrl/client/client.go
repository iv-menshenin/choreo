package client

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl/id"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/send"
)

type Client struct {
	options *Options

	transport send.Transport
	sender    *send.Sender

	closed  chan struct{}
	allDone sync.WaitGroup

	mux  sync.Mutex
	addr map[string]Addr
}

type Addr struct {
	Active time.Time
	Addr   net.Addr
}

func New(o *Options) (*Client, error) {
	if o == nil {
		return nil, fmt.Errorf("options must not be omitted")
	}
	if err := o.normalization(); err != nil {
		return nil, err
	}

	var c = Client{
		options:   o,
		transport: o.Transport,
		sender:    send.New(id.New(), o.Transport),
		closed:    make(chan struct{}),
		addr:      make(map[string]Addr),
	}

	c.allDone.Add(2)
	go c.discoveryCycle()
	go c.readAnswersCycle()

	return &c, nil
}

func (c *Client) Close() error {
	close(c.closed)
	err := c.transport.Close()
	c.allDone.Wait()

	if err != nil {
		return fmt.Errorf("can' close: %v", err)
	}
	return nil
}

func (c *Client) discoveryCycle() {
	ticker := time.NewTicker(c.options.DiscoveryInterval)
	defer ticker.Stop()
	defer c.allDone.Done()

	for {
		err := c.sender.WhoIsHere()
		if err != nil {
			c.reportErr(err)
		}

		select {
		case <-c.closed:
			return
		case <-ticker.C:
			c.cleanOld()
			continue
		}
	}
}

func (c *Client) readAnswersCycle() {
	defer c.allDone.Done()

	for {
		select {
		case <-c.closed:
			return
		default:
			// go ahead
		}

		var buf [1024]byte
		rcv, err := c.transport.Listen(buf[:0])
		if err != nil {
			c.reportErr(err)
		}

		var msg send.Message
		if err = msg.Parse(rcv); err != nil {
			c.reportErr(err)
		}
		if msg.Cmd == send.ResponseItIsMe {
			c.register(msg.Addr)
		}
	}
}

func (c *Client) reportErr(err error) {
	c.options.Logger.Printf("discovery error: %v", err)
}
