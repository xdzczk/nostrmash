package live

import (
	"container/list"
	"sync"
)

// dedupCache is a bounded LRU of recently persisted event IDs mapped to their
// verified author pubkeys. Relay overlap delivers most events several times;
// before this cache every copy paid full validation (JSON canonicalization,
// SHA-256, Schnorr verify) plus the canonical insert transaction just for the
// database to answer "already have it".
//
// Only events that passed validation and reached the store are added, so a
// hit proves the ID belongs to an already-verified canonical event; the
// cached pubkey (not anything claimed by the incoming payload) is what flows
// into provenance writes.
type dedupCache struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]*list.Element
	order    *list.List
}

type dedupEntry struct {
	id     string
	pubkey string
}

func newDedupCache(capacity int) *dedupCache {
	if capacity <= 0 {
		return nil
	}
	return &dedupCache{
		capacity: capacity,
		entries:  make(map[string]*list.Element, capacity),
		order:    list.New(),
	}
}

// Get reports whether id is cached and returns its verified author pubkey.
// A hit refreshes recency.
func (c *dedupCache) Get(id string) (pubkey string, ok bool) {
	if c == nil {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, exists := c.entries[id]
	if !exists {
		return "", false
	}
	c.order.MoveToFront(elem)
	return elem.Value.(*dedupEntry).pubkey, true
}

// Add records a verified, persisted event ID, evicting the least recently
// seen entry at capacity.
func (c *dedupCache) Add(id, pubkey string) {
	if c == nil || id == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, exists := c.entries[id]; exists {
		c.order.MoveToFront(elem)
		elem.Value.(*dedupEntry).pubkey = pubkey
		return
	}
	if c.order.Len() >= c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			c.order.Remove(oldest)
			delete(c.entries, oldest.Value.(*dedupEntry).id)
		}
	}
	c.entries[id] = c.order.PushFront(&dedupEntry{id: id, pubkey: pubkey})
}

// Len returns the current entry count (test helper).
func (c *dedupCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
