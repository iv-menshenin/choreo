package ownership

import (
	"sync"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl/internal/id"
)

type (
	Ownership struct {
		mux        sync.Mutex
		candidates map[string]candidate
		options    Options
		done       chan struct{}
	}
	candidate struct {
		key string
		exp time.Time
		id  id.ID
	}
	Options struct {
		Timeout time.Duration
		Window  time.Duration
	}
)

const (
	DefaultTimeout = 100 * time.Millisecond
	DefaultWindow  = 10 * time.Millisecond
)

func New(options *Options) *Ownership {
	if options == nil {
		options = &Options{}
	}
	if options.Window == 0 {
		options.Window = DefaultWindow
	}
	if options.Timeout == 0 {
		options.Timeout = DefaultTimeout
	}
	var o = Ownership{
		candidates: make(map[string]candidate),
		options:    *options,
		done:       make(chan struct{}),
	}
	go o.background()
	return &o
}

func (o *Ownership) background() {
	var toDelete []string
	for {
		select {
		case <-o.done:
			return

		case <-time.After(o.options.Window):
			o.mux.Lock()
			toDelete = o.garbage(toDelete)
			o.deleteCandidates(toDelete)
			o.mux.Unlock()
			toDelete = toDelete[:0]
		}
	}
}

func (o *Ownership) garbage(toDelete []string) []string {
	for k, v := range o.candidates {
		if time.Now().After(v.exp) {
			toDelete = append(toDelete, k)
		}
	}
	return toDelete
}

func (o *Ownership) deleteCandidates(toDelete []string) {
	for _, key := range toDelete {
		delete(o.candidates, key)
	}
}

func (o *Ownership) Add(ID id.ID, key string) bool {
	o.mux.Lock()
	if cand, ok := o.candidates[key]; ok && cand.id != ID {
		o.mux.Unlock()
		return false
	}
	o.candidates[key] = candidate{
		key: key,
		exp: time.Now().UTC().Add(o.options.Timeout),
		id:  ID,
	}
	o.mux.Unlock()
	return true
}

func (o *Ownership) Del(key string) {
	o.mux.Lock()
	delete(o.candidates, key)
	o.mux.Unlock()
}

func (o *Ownership) Close() {
	close(o.done)
}
