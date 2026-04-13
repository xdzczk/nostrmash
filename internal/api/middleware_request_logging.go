package api

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/failure"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

const defaultRouteTemplate = "/_unmatched"

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
		spanCtx, span := traceutil.StartSpan(ctx, "http.request",
			traceutil.KV("http.method", r.Method),
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
			pathTemplate := requestPathTemplate(r)
			span.SetAttr("http.route", pathTemplate)
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

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func randomID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
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
