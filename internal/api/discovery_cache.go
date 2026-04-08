package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultDiscoveryCacheMaxEntries    = 256
	defaultDiscoveryBundleTTL          = 60 * time.Second
	defaultDiscoveryDiscoveryTTL       = 60 * time.Second
	defaultDiscoveryPublicStatsTTL     = 10 * time.Minute
	defaultDiscoverySuggestionTTL      = 60 * time.Second
	discoveryCacheContentTypeJSONName  = "application/json"
	publicCacheValueNormalizeMaxLength = 512
)

type DiscoveryCacheOptions struct {
	Enabled        *bool
	MaxEntries     int
	BundleTTL      time.Duration
	DiscoveryTTL   time.Duration
	PublicStatsTTL time.Duration
	SuggestionTTL  time.Duration

	// Backward-compatible aliases. Prefer family-based TTL fields above.
	TrendingTTL time.Duration
	StatsTTL    time.Duration
}

type discoveryCacheConfig struct {
	Enabled        bool
	MaxEntries     int
	BundleTTL      time.Duration
	DiscoveryTTL   time.Duration
	PublicStatsTTL time.Duration
	SuggestionTTL  time.Duration
}

func defaultDiscoveryCacheConfig() discoveryCacheConfig {
	return discoveryCacheConfig{
		Enabled:        true,
		MaxEntries:     defaultDiscoveryCacheMaxEntries,
		BundleTTL:      defaultDiscoveryBundleTTL,
		DiscoveryTTL:   defaultDiscoveryDiscoveryTTL,
		PublicStatsTTL: defaultDiscoveryPublicStatsTTL,
		SuggestionTTL:  defaultDiscoverySuggestionTTL,
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
		out.BundleTTL = overrides.TrendingTTL
		out.DiscoveryTTL = overrides.TrendingTTL
		out.SuggestionTTL = overrides.TrendingTTL
	}
	if overrides.StatsTTL > 0 {
		out.PublicStatsTTL = overrides.StatsTTL
	}
	if overrides.BundleTTL > 0 {
		out.BundleTTL = overrides.BundleTTL
	}
	if overrides.DiscoveryTTL > 0 {
		out.DiscoveryTTL = overrides.DiscoveryTTL
	}
	if overrides.SuggestionTTL > 0 {
		out.SuggestionTTL = overrides.SuggestionTTL
	}
	if overrides.PublicStatsTTL > 0 {
		out.PublicStatsTTL = overrides.PublicStatsTTL
	}
	return out
}

type publicCacheFamily string

const (
	publicCacheFamilyBundle     publicCacheFamily = "bundle"
	publicCacheFamilyDiscovery  publicCacheFamily = "discovery"
	publicCacheFamilyStats      publicCacheFamily = "stats"
	publicCacheFamilySuggestion publicCacheFamily = "suggestion"
)

func (c discoveryCacheConfig) ttlForFamily(family publicCacheFamily) time.Duration {
	switch family {
	case publicCacheFamilyBundle:
		return c.BundleTTL
	case publicCacheFamilyStats:
		return c.PublicStatsTTL
	case publicCacheFamilySuggestion:
		return c.SuggestionTTL
	case publicCacheFamilyDiscovery:
		fallthrough
	default:
		return c.DiscoveryTTL
	}
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

type cacheLookupObserver func(family, endpoint string, hit bool)

type publicResponseCachePolicy struct {
	family   publicCacheFamily
	endpoint string
	key      string
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
	return h.writePublicCachedResponse(w, publicResponseCachePolicy{
		family:   publicCacheFamilyDiscovery,
		endpoint: "legacy_discovery",
		key:      key,
	})
}

func (h Handlers) writePublicCachedResponse(w http.ResponseWriter, policy publicResponseCachePolicy) bool {
	if h.discoveryCache == nil {
		return false
	}
	payload, ok := h.discoveryCache.get(policy.key)
	if h.cacheLookupObserver != nil {
		h.cacheLookupObserver(string(policy.family), policy.endpoint, ok)
	}
	if !ok {
		return false
	}
	w.Header().Set("Content-Type", discoveryCacheContentTypeJSONName)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
	return true
}

func (h Handlers) cacheDiscoveryPayload(key string, payload map[string]any, ttl time.Duration) {
	h.cachePublicPayload(publicResponseCachePolicy{
		family:   publicCacheFamilyDiscovery,
		endpoint: "legacy_discovery",
		key:      key,
	}, payload, ttl)
}

func (h Handlers) cachePublicPayload(policy publicResponseCachePolicy, payload map[string]any, ttlOverride ...time.Duration) {
	ttl := h.cacheConfig.ttlForFamily(policy.family)
	if len(ttlOverride) > 0 {
		ttl = ttlOverride[0]
	}
	if h.discoveryCache == nil || ttl <= 0 {
		return
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return
	}
	h.discoveryCache.set(policy.key, encoded, ttl)
}

func (h Handlers) newPublicCachePolicy(family publicCacheFamily, endpoint string, params map[string]any) publicResponseCachePolicy {
	return publicResponseCachePolicy{
		family:   family,
		endpoint: endpoint,
		key:      buildPublicCacheKey(string(family)+":"+endpoint, params),
	}
}

func buildPublicCacheKey(scope string, params map[string]any) string {
	if len(params) == 0 {
		return scope
	}
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	builder.WriteString(scope)
	for _, key := range keys {
		builder.WriteByte(':')
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(url.QueryEscape(normalizeCacheValue(params[key])))
	}
	return builder.String()
}

func normalizeCacheValue(value any) string {
	switch typed := value.(type) {
	case string:
		return normalizeCacheText(typed)
	case fmt.Stringer:
		return normalizeCacheText(typed.String())
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return normalizeCacheText(fmt.Sprint(value))
	}
}

func normalizeCacheText(raw string) string {
	normalized := strings.Join(strings.Fields(strings.TrimSpace(raw)), " ")
	if len(normalized) > publicCacheValueNormalizeMaxLength {
		return normalized[:publicCacheValueNormalizeMaxLength]
	}
	return normalized
}

func normalizeCacheFolded(raw string) string {
	return strings.ToLower(normalizeCacheText(raw))
}

func normalizeCacheHashtag(raw string) string {
	normalized := normalizeCacheFolded(raw)
	normalized = strings.TrimPrefix(normalized, "#")
	return normalizeCacheText(normalized)
}
