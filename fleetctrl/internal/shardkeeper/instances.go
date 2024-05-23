package shardkeeper

import (
	"crypto/sha256"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl/id"
)

type (
	Keeper struct {
		ID id.ID

		mux       sync.RWMutex
		instances map[id.ID]shard
		mineKeys  map[string]struct{}

		options *Options
	}
	shard struct {
		shard Shard
		keys  map[string]struct{}
	}
)

func New(self id.ID, options *Options) *Keeper {
	return &Keeper{
		ID:        self,
		instances: make(map[id.ID]shard),
		mineKeys:  make(map[string]struct{}),
		options:   options,
	}
}

// IDs Gives you remote shard identifiers.
func (i *Keeper) IDs() []id.ID {
	i.mux.RLock()
	var ids = make([]id.ID, 0, len(i.instances))
	for shardID := range i.instances {
		ids = append(ids, shardID)
	}
	i.mux.RUnlock()
	return ids
}

// Mine Gives you all the keys associated with the current instance.
func (i *Keeper) Mine() []string {
	i.mux.RLock()
	var keys = make([]string, 0, len(i.mineKeys))
	for key := range i.mineKeys {
		keys = append(keys, key)
	}
	i.mux.RUnlock()
	return keys
}

// Hash Gives you a hash sum of sorted shard IDs that can be validated by all shards.
func (i *Keeper) Hash() [32]byte {
	var allIDs = i.IDs()
	allIDs = append(allIDs, i.ID)
	sort.Slice(allIDs, func(i, j int) bool {
		return allIDs[i].Less(allIDs[j])
	})

	var data = make([]byte, 0, (len(allIDs)+1)*id.Size)
	for _, idx := range allIDs {
		data = append(data, idx[:]...)
	}
	return sha256.Sum256(data)
}

// Len Gives the number of remote shards.
func (i *Keeper) Len() int {
	i.mux.RLock()
	cnt := len(i.instances)
	i.mux.RUnlock()
	return cnt
}

// Keep Remembers if the shard is not known or updates the activity date if it is a known shard.
// You need to call this function periodically for the shards to be considered active.
func (i *Keeper) Keep(id id.ID, addr net.Addr) bool {
	i.mux.Lock()

	ex, ok := i.instances[id]
	if ok {
		ex.shard.Active = time.Now()
		i.instances[id] = ex
		i.mux.Unlock()
		return false
	}
	// new one
	i.instances[id] = shard{
		shard: Shard{
			ID:     id,
			Addr:   addr,
			Active: time.Now(),
		},
		keys: make(map[string]struct{}),
	}
	i.mux.Unlock()
	return true
}

// Lookup Performs a search for a shard by the key it is associated with.
func (i *Keeper) Lookup(key string) (Shard, bool) {
	i.mux.RLock()
	instance, ok := i.searchLocked(key)
	i.mux.RUnlock()
	return instance, ok
}

// Shard Gives the data of a specific shard by identifier.
func (i *Keeper) Shard(id id.ID) (Shard, bool) {
	i.mux.RLock()
	instance, ok := i.instances[id]
	i.mux.RUnlock()
	return instance.shard, ok
}

func (i *Keeper) searchLocked(key string) (Shard, bool) {
	if _, ok := i.mineKeys[key]; ok {
		return i.Me(), ok
	}
	for _, ins := range i.instances {
		if _, ok := ins.keys[key]; !ok {
			continue
		}
		return ins.shard, true
	}
	return Shard{}, false
}

func (i *Keeper) Me() Shard {
	return Shard{ID: i.ID}
}

// Own Remembers this key as belonging to himself.
func (i *Keeper) Own(key string) bool {
	var owned bool
	i.mux.Lock()
	if _, ok := i.searchLocked(key); !ok {
		i.mineKeys[key] = struct{}{}
		owned = true
	}
	i.mux.Unlock()
	return owned
}

// Forget Forgets that key, if that key belonged to himself.
func (i *Keeper) Forget(key string) {
	i.mux.Lock()
	delete(i.mineKeys, key)
	i.mux.Unlock()
}

// Save Associates this key with a specific shard.
func (i *Keeper) Save(id id.ID, addr net.Addr, key string) error {
	i.mux.Lock()

	// already known
	if owner, found := i.searchLocked(key); found {
		i.mux.Unlock()
		if owner.ID != id {
			return fmt.Errorf("it's not yours, it's %s", owner.ID)
		}
		return nil
	}

	instance, ok := i.instances[id]
	if !ok {
		// a new shard
		instance = shard{
			shard: Shard{
				ID:   id,
				Addr: addr,
			},
			keys: make(map[string]struct{}),
		}
	}

	if instance.shard.Addr.String() != addr.String() {
		instance.shard.Addr = addr
	}
	instance.shard.Active = time.Now()
	instance.keys[key] = struct{}{}
	i.instances[id] = instance

	i.mux.Unlock()
	return nil
}

func (i *Keeper) Reset(key string) {
	i.mux.Lock()
	// TODO need a hook here
	delete(i.mineKeys, key)
	for _, v := range i.instances {
		delete(v.keys, key)
	}
	i.mux.Unlock()
}

func (i *Keeper) Cleanup() int {
	var toDel = make([]id.ID, 0)

	i.mux.Lock()
	for k, v := range i.instances {
		if !v.shard.Live() {
			toDel = append(toDel, k)
		}
	}

	for _, key := range toDel {
		if i.options.PersistentKeys {
			continue
		}
		delete(i.instances, key)
	}
	i.mux.Unlock()

	return len(toDel)
}
