package api

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWithGzipCompressesLargeJSON(t *testing.T) {
	body := strings.Repeat(`{"k":"v"},`, 400)
	handler := WithGzip(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/home", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip encoding, got %q", got)
	}
	if got := rec.Header().Get("Vary"); !strings.Contains(got, "Accept-Encoding") {
		t.Fatalf("expected Vary: Accept-Encoding, got %q", got)
	}
	gr, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	decoded, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if string(decoded) != body {
		t.Fatalf("round-trip mismatch: got %d bytes want %d", len(decoded), len(body))
	}
	if rec.Body.Len() >= len(body) {
		t.Fatalf("compressed body (%d) not smaller than original (%d)", rec.Body.Len(), len(body))
	}
}

func TestWithGzipSkipsSmallResponses(t *testing.T) {
	handler := WithGzip(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not_found"}`))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/x", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("small response should stay uncompressed, got encoding %q", got)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status must survive buffering: got %d", rec.Code)
	}
	if rec.Body.String() != `{"error":"not_found"}` {
		t.Fatalf("unexpected body %q", rec.Body.String())
	}
}

func TestWithGzipRespectsClientWithoutSupport(t *testing.T) {
	body := strings.Repeat("data,", 1000)
	handler := WithGzip(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/home", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("client without gzip must get identity, got %q", got)
	}
	if rec.Body.String() != body {
		t.Fatal("body must pass through unmodified")
	}
}

func TestWithGzipSkipsUpgradeRequests(t *testing.T) {
	var sawWrapped bool
	handler := WithGzip(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, sawWrapped = w.(*gzipResponseWriter)
		w.WriteHeader(http.StatusSwitchingProtocols)
	}))

	req := httptest.NewRequest(http.MethodGet, "/primal/ws", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Upgrade", "websocket")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if sawWrapped {
		t.Fatal("upgrade request must not be wrapped for compression")
	}
}

func TestWithGzipSkipsNonCompressibleContent(t *testing.T) {
	body := bytes.Repeat([]byte{0x1, 0x2, 0x3, 0x4}, 600)
	handler := WithGzip(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(body)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/blob", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("binary content must not be compressed, got %q", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), body) {
		t.Fatal("binary body must pass through unmodified")
	}
}

func TestWithGzipSkipsAlreadyEncodedResponses(t *testing.T) {
	body := strings.Repeat("x", 4096)
	handler := WithGzip(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Encoding", "br")
		_, _ = w.Write([]byte(body))
	}))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("pre-encoded response must pass through, got %q", got)
	}
	if rec.Body.String() != body {
		t.Fatal("pre-encoded body must pass through unmodified")
	}
}

func TestWithGzipFlushSwitchesToPassthrough(t *testing.T) {
	handler := WithGzip(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("chunk-1"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write([]byte("chunk-2"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("flushed streaming response must stay uncompressed, got %q", got)
	}
	if rec.Body.String() != "chunk-1chunk-2" {
		t.Fatalf("unexpected streamed body %q", rec.Body.String())
	}
}
