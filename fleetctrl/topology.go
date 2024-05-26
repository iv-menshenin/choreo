package fleetctrl

import "net"

func (m *Manager) Fleet() []net.Addr {
	var addrs = make([]net.Addr, 0, m.keeper.Len())
	for _, id := range m.keeper.IDs() {
		shard, ok := m.keeper.Shard(id)
		if ok && shard.Live() {
			addrs = append(addrs, shard.Addr)
		}
	}
	return addrs
}
