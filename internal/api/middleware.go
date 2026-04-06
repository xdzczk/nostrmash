package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/failure"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

const defaultRouteTemplate = "/_unmatched"

// HTTPRateLimitOptions controls API-side per-client request limiting.
type HTTPRateLimitOptions struct {
	DefaultRPM   int
	DefaultBurst int
	SearchRPM    int
	BatchRPM     int
}

// WithRequestID attaches a request id to the context and response.
func WithRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if id == "" {
			var err error
			id, err = randomID()
			if err != nil {
				id = "unknown"
			}
		}
		ctx := logging.ContextWithRequestID(r.Context(), id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// LogRequests emits one structured log line per request.
func LogRequests(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := traceutil.ExtractHTTPContext(r.Context(), r.Header)
		pathTemplate := requestPathTemplate(r)
		spanCtx, span := traceutil.StartSpan(ctx, "http.request",
			traceutil.KV("http.method", r.Method),
			traceutil.KV("http.route", pathTemplate),
		)
		r = r.WithContext(spanCtx)
		if traceID := traceutil.TraceID(spanCtx); traceID != "" {
			w.Header().Set("X-Trace-ID", traceID)
		}

		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		defer func() {
			if recovered := recover(); recovered != nil {
				span.End(failure.FromPanic(recovered))
				panic(recovered)
			}
			var spanErr error
			if sw.code >= http.StatusInternalServerError {
				spanErr = errors.New("http status " + strings.TrimSpace(http.StatusText(sw.code)))
			}
			span.End(spanErr)
			metrics.ObserveAPI(r.Method, pathTemplate, sw.code, time.Since(start))
			logging.WithRequestID(r.Context(), log).Info("http_request",
				"method", r.Method,
				"path_actual", r.URL.Path,
				"path_template", pathTemplate,
				"status", sw.code,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		}()
		next.ServeHTTP(sw, r)
	})
}

// WithPanicRecovery converts unexpected panics into API-safe 500 responses.
func WithPanicRecovery(log *slog.Logger, next http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err := failure.FromPanic(recovered)
				class := failure.ClassifyError(err)
				logging.WithRequestID(r.Context(), log).Error("http_panic_recovered",
					"failure_class", class.Class,
					"failure_reason", class.Reason,
					"path", r.URL.Path,
					"method", r.Method,
					"trace_id", traceutil.TraceID(r.Context()),
					"error", err,
				)
				writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
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

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func randomID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// RequireBearerToken protects endpoints with an env-configured bearer token.
func RequireBearerToken(token string, next http.Handler) http.Handler {
	expected := strings.TrimSpace(token)
	if next == nil {
		next = http.NotFoundHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expected == "" {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "admin_unavailable", "admin auth is not configured")
			return
		}
		got, ok := bearerTokenFromHeader(r.Header.Get("Authorization"))
		if !ok {
			writeError(r.Context(), w, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			return
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
			writeError(r.Context(), w, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bearerTokenFromHeader(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(value, prefix))
	if token == "" {
		return "", false
	}
	return token, true
}

func requestPathTemplate(r *http.Request) string {
	pattern := strings.TrimSpace(r.Pattern)
	if pattern == "" {
		return defaultRouteTemplate
	}
	if parts := strings.SplitN(pattern, " ", 2); len(parts) == 2 {
		pattern = strings.TrimSpace(parts[1])
	}
	if pattern == "" {
		return defaultRouteTemplate
	}
	return pattern
}

func requestClientIP(r *http.Request) string {
	hostPort := strings.TrimSpace(r.RemoteAddr)
	if hostPort == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return hostPort
	}
	return host
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
	switch {
	case path == "/api/v1/search":
		if l.opts.SearchRPM > 0 {
			rpm = l.opts.SearchRPM
		}
		bucket = "search"
	case strings.HasSuffix(path, "/batch"):
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
