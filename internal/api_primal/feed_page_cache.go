package api_primal

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"
)

const feedPageCacheTTL = 45 * time.Second

// feedPageCache keeps the first page of a viewer's follows feed. Opening
// the app issues that request more than once; a short TTL turns the repeat
// into a memory hit. live_events_from_follows drops the page when the set
// of live activities for that viewer changes, which is the client's signal
// that the home screen should reload.
type feedPageCache struct {
	ttl time.Duration

	mu    sync.Mutex
	pages map[string]feedPageEntry
	live  map[string]string
}

type feedPageEntry struct {
	at   time.Time
	page []any
}

func newFeedPageCache(ttl time.Duration) *feedPageCache {
	if ttl <= 0 {
		ttl = feedPageCacheTTL
	}
	return &feedPageCache{
		ttl:   ttl,
		pages: map[string]feedPageEntry{},
		live:  map[string]string{},
	}
}

func feedPageCacheKey(viewer string, kinds []int, limit int) string {
	var b strings.Builder
	b.WriteString(strings.ToLower(strings.TrimSpace(viewer)))
	b.WriteByte('|')
	for _, kind := range kinds {
		b.WriteString(strconv.Itoa(kind))
		b.WriteByte(',')
	}
	b.WriteByte('|')
	b.WriteString(strconv.Itoa(limit))
	return b.String()
}

func (c *feedPageCache) get(key string, now time.Time) ([]any, bool) {
	if c == nil || key == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.pages[key]
	if !ok || now.Sub(entry.at) > c.ttl {
		return nil, false
	}
	page := make([]any, len(entry.page))
	copy(page, entry.page)
	return page, true
}

func (c *feedPageCache) set(key string, page []any, now time.Time) {
	if c == nil || key == "" || page == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pages) > 1024 {
		c.pages = map[string]feedPageEntry{}
	}
	c.pages[key] = feedPageEntry{at: now, page: page}
}

func (c *feedPageCache) noteLive(viewer string, signature string) {
	if c == nil {
		return
	}
	viewer = strings.ToLower(strings.TrimSpace(viewer))
	if viewer == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, seen := c.live[viewer]
	c.live[viewer] = signature
	if !seen || prev == signature {
		return
	}
	prefix := viewer + "|"
	for key := range c.pages {
		if strings.HasPrefix(key, prefix) {
			delete(c.pages, key)
		}
	}
}

func livePageSignature(page []any) string {
	var b strings.Builder
	for _, item := range page {
		raw, ok := item.(json.RawMessage)
		if !ok {
			continue
		}
		id := strings.TrimSpace(eventIDFromRaw(raw))
		if id == "" {
			continue
		}
		b.WriteString(id)
		b.WriteByte(',')
	}
	return b.String()
}
