package api

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

const (
	defaultDiscoveryCacheMaxEntries   = 256
	defaultDiscoveryTrendingTTL       = 60 * time.Second
	defaultDiscoveryPublicStatsTTL    = 10 * time.Minute
	discoveryCacheContentTypeJSONName = "application/json"
)

type DiscoveryCacheOptions struct {
	Enabled        *bool
	MaxEntries     int
	TrendingTTL    time.Duration
	PublicStatsTTL time.Duration
}

type discoveryCacheConfig struct {
	Enabled        bool
	MaxEntries     int
	TrendingTTL    time.Duration
	PublicStatsTTL time.Duration
}

func defaultDiscoveryCacheConfig() discoveryCacheConfig {
	return discoveryCacheConfig{
		Enabled:        true,
		MaxEntries:     defaultDiscoveryCacheMaxEntries,
		TrendingTTL:    defaultDiscoveryTrendingTTL,
		PublicStatsTTL: defaultDiscoveryPublicStatsTTL,
	}
}

func (c discoveryCacheConfig) withOverrides(overrides DiscoveryCacheOptions) discoveryCacheConfig {
	out := c
	if overrides.Enabled != nil {
		out.Enabled = *overrides.Enabled
	}
	if overrides.MaxEntries > 0 {
		out.MaxEntries = overrides.MaxEntries
	}
	if overrides.TrendingTTL > 0 {
		out.TrendingTTL = overrides.TrendingTTL
	}
	if overrides.PublicStatsTTL > 0 {
		out.PublicStatsTTL = overrides.PublicStatsTTL
	}
	return out
}

type discoveryResponseCache struct {
	mu         sync.Mutex
	now        func() time.Time
	maxEntries int
	seq        uint64
	entries    map[string]discoveryCacheEntry
}

type discoveryCacheEntry struct {
	payload   []byte
	expiresAt time.Time
	createdAt uint64
}

func newDiscoveryResponseCache(maxEntries int) *discoveryResponseCache {
	if maxEntries <= 0 {
		maxEntries = defaultDiscoveryCacheMaxEntries
	}
	return &discoveryResponseCache{
		now:        time.Now,
		maxEntries: maxEntries,
		entries:    make(map[string]discoveryCacheEntry, maxEntries),
	}
}

func (c *discoveryResponseCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if !entry.expiresAt.After(now) {
		delete(c.entries, key)
		return nil, false
	}
	return append([]byte(nil), entry.payload...), true
}

func (c *discoveryResponseCache) set(key string, payload []byte, ttl time.Duration) {
	if c == nil || ttl <= 0 {
		return
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteExpiredLocked(now)
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.maxEntries {
		c.evictOldestLocked()
	}
	c.seq++
	c.entries[key] = discoveryCacheEntry{
		payload:   append([]byte(nil), payload...),
		expiresAt: now.Add(ttl),
		createdAt: c.seq,
	}
}

func (c *discoveryResponseCache) deleteExpiredLocked(now time.Time) {
	for key, entry := range c.entries {
		if !entry.expiresAt.After(now) {
			delete(c.entries, key)
		}
	}
}

func (c *discoveryResponseCache) evictOldestLocked() {
	var (
		oldestKey string
		oldestSeq uint64
		found     bool
	)
	for key, entry := range c.entries {
		if !found || entry.createdAt < oldestSeq {
			found = true
			oldestSeq = entry.createdAt
			oldestKey = key
		}
	}
	if found {
		delete(c.entries, oldestKey)
	}
}

func (h Handlers) writeDiscoveryCachedResponse(w http.ResponseWriter, key string) bool {
	payload, ok := h.discoveryCache.get(key)
	if !ok {
		return false
	}
	w.Header().Set("Content-Type", discoveryCacheContentTypeJSONName)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
	return true
}

func (h Handlers) cacheDiscoveryPayload(key string, payload map[string]any, ttl time.Duration) {
	if h.discoveryCache == nil || ttl <= 0 {
		return
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return
	}
	h.discoveryCache.set(key, encoded, ttl)
}
