package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

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
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		metrics.ObserveAPI(r.Method, r.URL.Path, sw.code, time.Since(start))
		logging.WithRequestID(r.Context(), log).Info("http_request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.code,
			"duration_ms", time.Since(start).Milliseconds(),
		)
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
