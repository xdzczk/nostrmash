package api

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

// HTTPRateLimitOptions controls API-side per-client request limiting.
type HTTPRateLimitOptions struct {
	DefaultRPM     int
	DefaultBurst   int
	SearchRPM      int
	BatchRPM       int
	DiscoveryRPM   int
	SuggestRPM     int
	PublicStatsRPM int
}

// WithHTTPRateLimit applies per-IP token-bucket limits for public HTTP APIs.
func WithHTTPRateLimit(opts HTTPRateLimitOptions, next http.Handler) http.Handler {
	limiter := newRateLimiter(opts)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isRateLimitExemptPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		plan := limiter.planForPath(r.URL.Path)
		if !plan.enabled {
			next.ServeHTTP(w, r)
			return
		}
		clientIP := requestClientIP(r)
		if clientIP == "" {
			clientIP = "unknown"
		}
		if !limiter.allow(clientIP+":"+plan.bucket, plan.rpm, plan.burst, time.Now()) {
			writeError(r.Context(), w, http.StatusTooManyRequests, "rate_limited", "too many requests")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isRateLimitExemptPath(path string) bool {
	switch path {
	case "/health", "/ready", "/metrics":
		return true
	default:
		return false
	}
}

type rateBucket struct {
	tokens float64
	last   time.Time
}

type rateLimitPlan struct {
	enabled bool
	bucket  string
	rpm     int
	burst   int
}

type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]rateBucket
	opts    HTTPRateLimitOptions
}

func newRateLimiter(opts HTTPRateLimitOptions) *rateLimiter {
	if opts.DefaultBurst <= 0 {
		opts.DefaultBurst = 1
	}
	return &rateLimiter{
		buckets: make(map[string]rateBucket),
		opts:    opts,
	}
}

func (l *rateLimiter) planForPath(path string) rateLimitPlan {
	rpm := l.opts.DefaultRPM
	bucket := "default"
	switch classifyPublicEndpoint(path) {
	case publicEndpointClassSearch:
		if l.opts.SearchRPM > 0 {
			rpm = l.opts.SearchRPM
		}
		bucket = "search"
	case publicEndpointClassDiscovery:
		if l.opts.DiscoveryRPM > 0 {
			rpm = l.opts.DiscoveryRPM
		}
		bucket = "discovery"
	case publicEndpointClassSuggest:
		if l.opts.SuggestRPM > 0 {
			rpm = l.opts.SuggestRPM
		}
		bucket = "suggest"
	case publicEndpointClassPublicStats:
		if l.opts.PublicStatsRPM > 0 {
			rpm = l.opts.PublicStatsRPM
		}
		bucket = "public_stats"
	case publicEndpointClassBatch:
		if l.opts.BatchRPM > 0 {
			rpm = l.opts.BatchRPM
		}
		bucket = "batch"
	}
	return rateLimitPlan{
		enabled: rpm > 0,
		bucket:  bucket,
		rpm:     rpm,
		burst:   l.opts.DefaultBurst,
	}
}

func (l *rateLimiter) allow(key string, rpm int, burst int, now time.Time) bool {
	if rpm <= 0 {
		return true
	}
	if burst <= 0 {
		burst = 1
	}
	refillPerSecond := float64(rpm) / 60.0
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.buckets[key]
	if b.last.IsZero() {
		b.last = now
		b.tokens = float64(burst)
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * refillPerSecond
		if b.tokens > float64(burst) {
			b.tokens = float64(burst)
		}
	}
	b.last = now
	if b.tokens < 1.0 {
		l.buckets[key] = b
		return false
	}
	b.tokens -= 1.0
	l.buckets[key] = b
	return true
}

type publicEndpointClass string

const (
	publicEndpointClassUnknown     publicEndpointClass = "unknown"
	publicEndpointClassSearch      publicEndpointClass = "search"
	publicEndpointClassSuggest     publicEndpointClass = "suggest"
	publicEndpointClassDiscovery   publicEndpointClass = "discovery"
	publicEndpointClassPublicStats publicEndpointClass = "public_stats"
	publicEndpointClassBatch       publicEndpointClass = "batch"
)

func classifyPublicEndpoint(path string) publicEndpointClass {
	path = strings.TrimSpace(path)
	switch {
	case path == "/api/v1/search/suggest":
		return publicEndpointClassSuggest
	case path == "/api/v1/search", path == "/api/v1/search/notes", path == "/api/v1/search/profiles":
		return publicEndpointClassSearch
	case strings.HasSuffix(path, "/batch") && (strings.HasPrefix(path, "/api/v1/") || strings.HasPrefix(path, "/primal/v1/")):
		return publicEndpointClassBatch
	case path == "/api/v1/discovery/stats/network",
		path == "/api/v1/discovery/stats/content",
		path == "/api/v1/discovery/stats/relays",
		path == "/api/v1/discovery/network/stats",
		path == "/api/v1/discovery/content/stats":
		return publicEndpointClassPublicStats
	case strings.HasPrefix(path, "/api/v1/discovery/"):
		return publicEndpointClassDiscovery
	default:
		return publicEndpointClassUnknown
	}
}
