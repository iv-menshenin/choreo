package fleetctrl

import "github.com/iv-menshenin/choreo/fleetctrl/internal/shardkeeper"

type Options struct {
	PersistentKeys bool
}

func (o *Options) shardOptions() *shardkeeper.Options {
	return &shardkeeper.Options{
		PersistentKeys: o.PersistentKeys,
	}
}
