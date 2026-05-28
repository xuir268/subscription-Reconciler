package api

import "sync"

// seenCache is an in-memory LRU set used as a fast-path duplicate guard for
// store webhook event IDs. It is NOT the authoritative deduplication layer —
// the DB ON CONFLICT is. This cache avoids opening a DB transaction for events
// that were recently seen. Pre-warm it from the DB on startup so it survives
// restarts (see WarmSeenCache).
type seenCache struct {
	mu       sync.Mutex
	capacity int
	order    []string
	items    map[string]struct{}
}

func newSeenCache(capacity int) *seenCache {
	return &seenCache{
		capacity: capacity,
		items:    make(map[string]struct{}, capacity),
	}
}

// seenOrAdd returns true if key was already in the cache. Always adds the key.
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

// add inserts a key without checking if it already exists.
func (c *seenCache) add(key string) {
	c.seenOrAdd(key)
}
