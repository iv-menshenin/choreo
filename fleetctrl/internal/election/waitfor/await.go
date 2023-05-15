package waitfor

import (
	"bytes"
	"sync"
	"sync/atomic"
)

type (
	Awaiter struct {
		mux sync.RWMutex
		num int64
		wai []wait
	}
	wait struct {
		num  int64
		trg  int64
		data []byte
		done chan<- struct{}
	}
)

func New() *Awaiter {
	return &Awaiter{}
}

func (a *Awaiter) Trigger(data []byte) {
	a.mux.RLock()
	for n := range a.wai {
		waiter := &a.wai[n]
		if atomic.LoadInt64(&waiter.trg) > 0 {
			continue
		}

		if bytes.Equal(data, waiter.data) {
			if atomic.CompareAndSwapInt64(&waiter.trg, 0, 1) {
				close(waiter.done)
			}
		}
	}
	a.mux.RUnlock()
}

func (a *Awaiter) Add(data []byte, done chan<- struct{}) int64 {
	var newAwaiter = wait{
		num:  atomic.AddInt64(&a.num, 1),
		data: data,
		done: done,
	}
	a.mux.Lock()
	a.wai = append(a.wai, newAwaiter)
	a.mux.Unlock()
	return newAwaiter.num
}

func (a *Awaiter) Del(num int64) {
	a.mux.Lock()
	var n int
	for n = 0; n < len(a.wai) && a.wai[n].num != num; {
		n++
	}

	if n < len(a.wai) {
		a.wai = append(a.wai[:n], a.wai[n+1:]...)
	}

	if len(a.wai) > 16 && cap(a.wai) > len(a.wai)*4 {
		// reduce if needed
		var buf = a.wai
		a.wai = make([]wait, len(buf), len(buf)+len(buf)/2)
		copy(a.wai, buf)
	}
	a.mux.Unlock()
}
