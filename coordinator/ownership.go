package coordinator

import (
	"sync"
	"time"
)

func (m *Manager) tryToOwn(key string) (bool, error) {
	if !m.own.add(m.id, key) {
		return false, nil
	}
	chErr := m.awaitMostOf(cmdCandidate, append(append(make([]byte, 0, 16+len(key)), m.id[:]...), []byte(key)...))
	if err := m.sendWantKey(key); err != nil {
		return false, err
	}
	if err := <-chErr; err != nil {
		m.warning("OWNERSHIP DENIED %x: %+v", m.id, err)
		return false, nil
	}
	if !m.ins.toOwn(key) {
		m.warning("OWNERSHIP BREAKED %x", m.id)
		return false, nil
	}
	chErr = m.awaitMostOf(cmdSaved, append(append(make([]byte, 0, 16+len(key)), m.id[:]...), []byte(key)...))
	if err := m.sendThatIsMine(key); err != nil {
		return false, err
	}
	if err := <-chErr; err != nil {
		m.ins.fromOwn(key)
		m.warning("OWNERSHIP CANCELLED %x: %+v", m.id, err)
		return false, nil
	}
	return true, nil
}

type Ownership struct {
	mux        sync.Mutex
	candidates map[string]candidate
}

func newOwnership() *Ownership {
	var o = Ownership{
		candidates: make(map[string]candidate),
	}
	go o.background()
	return &o
}

type candidate struct {
	key string
	exp time.Time
	id  [16]byte
}

func (o *Ownership) background() {
	var toDelete []string
	for {
		<-time.After(10 * time.Millisecond)
		o.mux.Lock()
		for k, v := range o.candidates {
			if time.Now().After(v.exp) {
				toDelete = append(toDelete, k)
			}
		}
		for _, key := range toDelete {
			delete(o.candidates, key)
		}
		o.mux.Unlock()
		toDelete = toDelete[:0]
	}
}

func (o *Ownership) add(ID [16]byte, key string) bool {
	o.mux.Lock()
	if cand, ok := o.candidates[key]; ok && cand.id != ID {
		o.mux.Unlock()
		return false
	}
	cand := candidate{
		key: key,
		exp: time.Now().UTC().Add(100 * time.Millisecond),
		id:  ID,
	}
	o.candidates[key] = cand
	o.mux.Unlock()
	return true
}

func (o *Ownership) del(key string) {
	o.mux.Lock()
	delete(o.candidates, key)
	o.mux.Unlock()
}
