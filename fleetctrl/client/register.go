package client

import (
	"net"
	"time"
)

func (c *Client) register(addr net.Addr) {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.addr[addr.String()] = Addr{
		Active: time.Now(),
		Addr:   addr,
	}
}

func (c *Client) cleanOld() {
	c.mux.Lock()
	defer c.mux.Unlock()

	var lat = 3 * c.options.DiscoveryInterval
	var old = make([]string, 0, len(c.addr))
	for k, v := range c.addr {
		if time.Since(v.Active) < lat {
			continue
		}
		old = append(old, k)
	}

	for _, k := range old {
		delete(c.addr, k)
	}
}

func (c *Client) Get() *Addr {
	c.mux.Lock()
	defer c.mux.Unlock()

	for _, v := range c.addr {
		return &v
	}
	return nil
}

func (c *Client) All() []Addr {
	c.mux.Lock()
	defer c.mux.Unlock()

	var all = make([]Addr, 0, len(c.addr))
	for _, v := range c.addr {
		all = append(all, v)
	}
	return all
}
