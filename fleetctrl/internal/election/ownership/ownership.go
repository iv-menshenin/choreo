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
		options = &Options{
			Timeout: DefaultTimeout,
			Window:  DefaultWindow,
		}
	}

	if options.Window == 0 {
		options.Window = DefaultWindow
	}
	if options.Timeout == 0 {
		options.Timeout = DefaultTimeout
	}

	var ownership = &Ownership{
		candidates: make(map[string]candidate),
		options:    *options,
		done:       make(chan struct{}),
	}
	go ownership.background()
	return ownership
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

func (o *Ownership) Add(ownerID id.ID, key string) bool {
	o.mux.Lock()
	if exists, ok := o.candidates[key]; ok && exists.id != ownerID {
		o.mux.Unlock()
		return false
	}
	o.candidates[key] = candidate{
		key: key,
		exp: time.Now().UTC().Add(o.options.Timeout),
		id:  ownerID,
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
