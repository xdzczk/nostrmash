package api

import (
	"bufio"
	"compress/gzip"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
)

// gzipMinSize is the smallest body worth compressing. Below this the gzip
// header/trailer overhead and CPU cost outweigh the transfer savings, so the
// buffered bytes are flushed uncompressed instead.
const gzipMinSize = 1024

var gzipWriterPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return w
	},
}

// WithGzip compresses response bodies when the client advertises gzip
// support. Responses are buffered up to gzipMinSize before deciding, so tiny
// payloads (errors, health checks) are never compressed. Upgrade requests
// (the Primal WebSocket endpoint) bypass wrapping entirely so hijacking works
// untouched, and responses that already carry a Content-Encoding (e.g. the
// promhttp metrics handler compressing its own output) are passed through.
func WithGzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "" {
			next.ServeHTTP(w, r)
			return
		}
		// Always declare the variant axis: public responses are now
		// cacheable by shared proxies, and a cached identity response
		// must not be conflated with a gzip one (or vice versa).
		w.Header().Add("Vary", "Accept-Encoding")
		if !acceptsGzip(r) {
			next.ServeHTTP(w, r)
			return
		}
		gw := &gzipResponseWriter{ResponseWriter: w, code: http.StatusOK}
		defer gw.finalize()
		next.ServeHTTP(gw, r)
	})
}

func acceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		encoding := part
		if idx := strings.IndexByte(encoding, ';'); idx >= 0 {
			encoding = encoding[:idx]
		}
		if strings.EqualFold(strings.TrimSpace(encoding), "gzip") {
			return true
		}
	}
	return false
}

// gzipResponseWriter defers both WriteHeader and body writes until it knows
// whether the response is worth compressing. Exactly one of three modes is
// entered on the first decision point: compressed, passthrough, or (at
// finalize time with a small buffer) uncompressed flush.
type gzipResponseWriter struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
	buf         []byte
	gz          *gzip.Writer
	passthrough bool
	hijacked    bool
}

func (w *gzipResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.code = code
		w.wroteHeader = true
	}
}

func (w *gzipResponseWriter) Write(p []byte) (int, error) {
	if w.passthrough {
		return w.ResponseWriter.Write(p)
	}
	if w.gz != nil {
		return w.gz.Write(p)
	}
	w.buf = append(w.buf, p...)
	if len(w.buf) >= gzipMinSize {
		if err := w.startCompressing(); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// startCompressing commits to gzip mode: emits headers (with Content-Encoding
// and without any stale Content-Length) and streams the buffered bytes into a
// pooled gzip writer.
func (w *gzipResponseWriter) startCompressing() error {
	if !compressibleResponse(w.Header()) {
		return w.startPassthrough()
	}
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(w.code)
	gz := gzipWriterPool.Get().(*gzip.Writer)
	gz.Reset(w.ResponseWriter)
	w.gz = gz
	if len(w.buf) > 0 {
		if _, err := gz.Write(w.buf); err != nil {
			return err
		}
		w.buf = nil
	}
	return nil
}

// startPassthrough commits to uncompressed mode and drains the buffer.
func (w *gzipResponseWriter) startPassthrough() error {
	w.passthrough = true
	w.ResponseWriter.WriteHeader(w.code)
	if len(w.buf) > 0 {
		if _, err := w.ResponseWriter.Write(w.buf); err != nil {
			return err
		}
		w.buf = nil
	}
	return nil
}

func compressibleResponse(h http.Header) bool {
	if h.Get("Content-Encoding") != "" {
		return false
	}
	contentType := h.Get("Content-Type")
	if idx := strings.IndexByte(contentType, ';'); idx >= 0 {
		contentType = contentType[:idx]
	}
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	switch {
	case contentType == "",
		strings.HasPrefix(contentType, "text/"),
		contentType == "application/json",
		contentType == "application/javascript",
		contentType == "application/xml":
		return true
	default:
		return false
	}
}

// finalize flushes whatever mode the writer ended in. A never-flushed small
// buffer is written uncompressed; an active gzip stream is closed and the
// writer returned to the pool.
func (w *gzipResponseWriter) finalize() {
	if w.hijacked || w.passthrough {
		return
	}
	if w.gz != nil {
		_ = w.gz.Close()
		gzipWriterPool.Put(w.gz)
		w.gz = nil
		return
	}
	w.ResponseWriter.WriteHeader(w.code)
	if len(w.buf) > 0 {
		_, _ = w.ResponseWriter.Write(w.buf)
		w.buf = nil
	}
}

// Flush commits the current mode so streaming handlers keep working. An
// undecided writer becomes passthrough: a handler that flushes mid-response
// wants incremental delivery, which buffering-for-compression would defeat.
func (w *gzipResponseWriter) Flush() {
	if w.gz == nil && !w.passthrough {
		_ = w.startPassthrough()
	}
	if w.gz != nil {
		_ = w.gz.Flush()
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Hijack hands the raw connection to the caller (WebSocket upgrades). Upgrade
// requests skip this wrapper entirely, but keep the passthrough for safety.
func (w *gzipResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	w.hijacked = true
	return hijacker.Hijack()
}

func (w *gzipResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
