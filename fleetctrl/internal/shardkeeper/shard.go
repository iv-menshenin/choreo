package shardkeeper

import (
	"net"
	"time"

	"github.com/iv-menshenin/choreo/fleetctrl/id"
)

const activeTime = 15 * time.Second

type Shard struct {
	ID     id.ID
	Addr   net.Addr
	Active time.Time
}

func (s Shard) Me() bool {
	return s.Addr == nil
}

func (s Shard) Live() bool {
	if s.Me() {
		return true
	}
	return time.Since(s.Active) < activeTime
}

func (s Shard) NetAddr() net.Addr {
	return s.Addr
}

func (s Shard) ShardID() id.ID {
	return s.ID
}
