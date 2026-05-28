package api

import "sync"

type seenCache struct {
	mu       sync.Mutex
	capacity int
	order    []string
	items    map[string]struct{}
}

func newSeenCache(capacity int) *seenCache {
	return &seenCache{
		capacity: capacity,
		items:    make(map[string]struct{}),
	}
}

func (c *seenCache) seenOrAdd(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[key]; ok {
		return true
	}
	c.items[key] = struct{}{}
	c.order = append(c.order, key)
	if len(c.order) > c.capacity {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.items, oldest)
	}
	return false
}
