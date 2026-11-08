package api

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	defaultPublicReadCacheTTL      = 60 * time.Second
	defaultPublicReadCacheEntries  = 2048
	defaultPublicReadCacheBody     = 512 * 1024
	defaultPublicReadRatePerSecond = 40
	defaultPublicReadRateBurst     = 80
)

// PublicReadCacheConfig bounds the anonymous note-page cache. Those routes
// are fetched once per note by clients that walk threads, and the responses
// do not depend on the caller.
type PublicReadCacheConfig struct {
	TTL           time.Duration
	MaxEntries    int
	MaxBodyBytes  int
	RatePerSecond float64
	Burst         int
}

// PublicReadCache stores successful anonymous GET responses and caps how
// fast one client address can call the wrapped routes.
type PublicReadCache struct {
	ttl        time.Duration
	maxEntries int
	maxBody    int
	rate       float64
	burst      float64

	mu      sync.Mutex
	items   map[string]publicReadEntry
	order   []string
	buckets map[string]publicReadBucket
	flight  singleflight.Group
}

type publicReadEntry struct {
	status      int
	contentType string
	body        []byte
	expires     time.Time
}

type publicReadBucket struct {
	tokens  float64
	updated time.Time
}

func NewPublicReadCache(cfg PublicReadCacheConfig) *PublicReadCache {
	if cfg.TTL <= 0 {
		cfg.TTL = defaultPublicReadCacheTTL
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = defaultPublicReadCacheEntries
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = defaultPublicReadCacheBody
	}
	if cfg.RatePerSecond <= 0 {
		cfg.RatePerSecond = defaultPublicReadRatePerSecond
	}
	if cfg.Burst <= 0 {
		cfg.Burst = defaultPublicReadRateBurst
	}
	return &PublicReadCache{
		ttl:        cfg.TTL,
		maxEntries: cfg.MaxEntries,
		maxBody:    cfg.MaxBodyBytes,
		rate:       cfg.RatePerSecond,
		burst:      float64(cfg.Burst),
		items:      map[string]publicReadEntry{},
		buckets:    map[string]publicReadBucket{},
	}
}

func (c *PublicReadCache) Handler(next http.Handler) http.Handler {
	if c == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || requestCarriesIdentity(r) {
			next.ServeHTTP(w, r)
			return
		}
		if !c.allow(clientAddress(r), time.Now()) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"too many requests"}}` + "\n"))
			return
		}
		key := r.URL.RequestURI()
		if entry, ok := c.get(key, time.Now()); ok {
			writePublicReadEntry(w, entry, c.ttl)
			return
		}
		shared, _, _ := c.flight.Do(key, func() (any, error) {
			if entry, ok := c.get(key, time.Now()); ok {
				return entry, nil
			}
			captured := &publicReadCapture{header: make(http.Header)}
			next.ServeHTTP(captured, r)
			entry := publicReadEntry{
				status:      captured.statusCode(),
				contentType: captured.header.Get("Content-Type"),
				body:        append([]byte(nil), captured.body...),
			}
			if entry.status == http.StatusOK && len(entry.body) > 0 && len(entry.body) <= c.maxBody {
				entry.expires = time.Now().Add(c.ttl)
				c.set(key, entry)
			}
			return entry, nil
		})
		writePublicReadEntry(w, shared.(publicReadEntry), c.ttl)
	})
}

func (c *PublicReadCache) get(key string, now time.Time) (publicReadEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.items[key]
	if !ok || !entry.expires.After(now) {
		return publicReadEntry{}, false
	}
	return entry, true
}

func (c *PublicReadCache) set(key string, entry publicReadEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := make([]string, 0, len(c.order)+1)
	for _, item := range c.order {
		if item != key {
			next = append(next, item)
		}
	}
	c.order = append(next, key)
	c.items[key] = entry
	for len(c.items) > c.maxEntries && len(c.order) > 1 {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.items, oldest)
	}
}

func (c *PublicReadCache) allow(addr string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.buckets) > 4096 {
		c.buckets = map[string]publicReadBucket{}
	}
	bucket := c.buckets[addr]
	if bucket.updated.IsZero() {
		bucket.tokens = c.burst
	} else {
		bucket.tokens += now.Sub(bucket.updated).Seconds() * c.rate
		if bucket.tokens > c.burst {
			bucket.tokens = c.burst
		}
	}
	bucket.updated = now
	if bucket.tokens < 1 {
		c.buckets[addr] = bucket
		return false
	}
	bucket.tokens--
	c.buckets[addr] = bucket
	return true
}

func writePublicReadEntry(w http.ResponseWriter, entry publicReadEntry, ttl time.Duration) {
	if entry.contentType != "" {
		w.Header().Set("Content-Type", entry.contentType)
	}
	if entry.status == http.StatusOK {
		w.Header().Set("Cache-Control", "public, max-age="+itoa(int(ttl.Seconds())))
	}
	status := entry.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(entry.body)
}

func requestCarriesIdentity(r *http.Request) bool {
	if strings.TrimSpace(r.Header.Get("Authorization")) != "" {
		return true
	}
	return len(r.Cookies()) > 0
}

func clientAddress(r *http.Request) string {
	if ip := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); ip != "" {
		return ip
	}
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		return strings.TrimSpace(strings.Split(forwarded, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func itoa(n int) string {
	if n <= 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

type publicReadCapture struct {
	header http.Header
	status int
	body   []byte
}

func (c *publicReadCapture) Header() http.Header { return c.header }

func (c *publicReadCapture) WriteHeader(status int) {
	if c.status == 0 {
		c.status = status
	}
}

func (c *publicReadCapture) Write(p []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	c.body = append(c.body, p...)
	return len(p), nil
}

func (c *publicReadCapture) statusCode() int {
	if c.status == 0 {
		return http.StatusOK
	}
	return c.status
}
