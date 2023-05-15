package fleetctrl

import (
	"errors"
	"fmt"
	"github.com/iv-menshenin/choreo/fleetctrl/internal/id"
	"net"
	"sync"
	"time"
)

type (
	Instances struct {
		mux       sync.RWMutex
		instances map[string]Instance
		mineKeys  map[string]struct{}
	}
	Instance struct {
		ID   id.ID
		tm   time.Time
		addr net.Addr
		keys map[string]struct{} // TODO performance
	}
)

func newInstances() *Instances {
	return &Instances{
		instances: make(map[string]Instance),
		mineKeys:  make(map[string]struct{}),
	}
}

func (i *Instances) getAllID() [][16]byte {
	var ids = make([][16]byte, 0, len(i.instances))
	i.mux.RLock()
	for _, i := range i.instances {
		ids = append(ids, i.ID)
	}
	i.mux.RUnlock()
	return ids
}

func (i *Instances) hashAllID(self [16]byte) []byte {
	var ids = make([][16]byte, 0, len(i.instances))
	i.mux.RLock()
	for _, i := range i.instances {
		ids = append(ids, i.ID)
	}
	i.mux.RUnlock()
	var hash [16]byte
	copy(hash[:], self[:])
	for _, id := range ids {
		for n := 0; n < 16; n++ {
			hash[n] ^= id[n]
		}
	}
	return hash[:]
}

func (i *Instances) getCount() int {
	i.mux.RLock()
	cnt := len(i.instances)
	i.mux.RUnlock()
	return cnt
}

func (i *Instances) add(id id.ID, addr net.Addr) bool {
	var isNew bool
	i.mux.Lock()
	instance, ok := i.instances[addr.String()]
	if !ok || instance.ID != id {
		instance = Instance{
			ID:   id,
			addr: addr,
			keys: make(map[string]struct{}),
		}
		isNew = true
	}
	instance.tm = time.Now()
	i.instances[addr.String()] = instance
	i.mux.Unlock()
	return isNew
}

func (i *Instances) search(key string) string {
	i.mux.RLock()
	instance := i.searchInt(key)
	i.mux.RUnlock()
	return instance
}

func (i *Instances) searchInt(key string) string {
	if _, ok := i.mineKeys[key]; ok {
		return Mine
	}
	for k, v := range i.instances {
		if _, ok := v.keys[key]; ok {
			return k
		}
	}
	return ""
}

func (i *Instances) searchIntInst(key string) *id.ID {
	if _, ok := i.mineKeys[key]; ok {
		return &id.NullID
	}
	for _, v := range i.instances {
		if _, ok := v.keys[key]; ok {
			return &v.ID
		}
	}
	return nil
}

func (i *Instances) toOwn(key string) bool {
	var owned bool
	i.mux.Lock()
	if i.searchInt(key) == "" {
		i.mineKeys[key] = struct{}{}
		owned = true
	}
	i.mux.Unlock()
	return owned
}

func (i *Instances) fromOwn(key string) {
	i.mux.Lock()
	delete(i.mineKeys, key)
	i.mux.Unlock()
}

func (i *Instances) save(id id.ID, key string, addr net.Addr) error {
	i.mux.Lock()
	if owner := i.searchIntInst(key); owner != nil && *owner != id {
		i.mux.Unlock()
		return fmt.Errorf("it's not yours, it's %x", owner)
	}
	instance, ok := i.instances[addr.String()]
	if !ok || instance.ID != id {
		i.mux.Unlock()
		return errors.New("i dont know you")
	}
	instance.tm = time.Now()
	instance.keys[key] = struct{}{}
	i.instances[addr.String()] = instance
	i.mux.Unlock()
	return nil
}

func (i *Instances) reset(key string) {
	i.mux.Lock()
	// TODO need a hook here
	delete(i.mineKeys, key)
	for _, v := range i.instances {
		delete(v.keys, key)
	}
	i.mux.Unlock()
}

func (i *Instances) cleanup() int {
	var toDel = make([]string, 0)
	i.mux.Lock()
	for k, v := range i.instances {
		if time.Since(v.tm) > 5*time.Second {
			toDel = append(toDel, k)
		}
	}
	for _, key := range toDel {
		delete(i.instances, key)
	}
	i.mux.Unlock()
	return len(toDel)
}
