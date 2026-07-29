package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/logging"
)

func TestWithRequestID_PropagatesIncomingHeader(t *testing.T) {
	var captured string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = logging.RequestIDFromContext(r.Context())
	})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Request-ID", "incoming-id-42")
	rec := httptest.NewRecorder()

	WithRequestID(next).ServeHTTP(rec, req)

	if captured != "incoming-id-42" {
		t.Fatalf("unexpected context request id: got %q", captured)
	}
	if got := rec.Header().Get("X-Request-ID"); got != "incoming-id-42" {
		t.Fatalf("unexpected response request id: got %q", got)
	}
}

func TestWithRequestID_GeneratesWhenHeaderMissing(t *testing.T) {
	var captured string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = logging.RequestIDFromContext(r.Context())
	})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	WithRequestID(next).ServeHTTP(rec, req)

	if captured == "" || captured == "unknown" {
		t.Fatalf("expected generated request id, got %q", captured)
	}
	if got := rec.Header().Get("X-Request-ID"); got != captured {
		t.Fatalf("response request id mismatch: got %q want %q", got, captured)
	}
}

func TestRequireBearerToken_MissingToken(t *testing.T) {
	protected := RequireBearerToken("secret-token", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/system", nil)
	rec := httptest.NewRecorder()

	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusUnauthorized)
	}

	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if envelope.Error.Code != "unauthorized" {
		t.Fatalf("unexpected code: got %q", envelope.Error.Code)
	}
}

func TestRequireBearerToken_AllowsValidToken(t *testing.T) {
	protected := RequireBearerToken("secret-token", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/system", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()

	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNoContent)
	}
}

func TestRequestPathTemplate_UsesServeMuxPattern(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/abc", nil)
	req.Pattern = "GET /api/v1/events/{id}"
	if got := requestPathTemplate(req); got != "/api/v1/events/{id}" {
		t.Fatalf("unexpected path template: got %q", got)
	}
}

func TestRequestClientIP_PrefersProxyHeaders(t *testing.T) {
	cases := []struct {
		name       string
		remoteAddr string
		realIP     string
		forwarded  string
		want       string
	}{
		{
			name:       "remote_addr_only",
			remoteAddr: "10.0.1.6:4321",
			want:       "10.0.1.6",
		},
		{
			name:       "x_real_ip_beats_remote_addr",
			remoteAddr: "10.0.1.6:4321",
			realIP:     "203.0.113.10",
			want:       "203.0.113.10",
		},
		{
			name:       "x_forwarded_for_leftmost",
			remoteAddr: "10.0.1.6:4321",
			forwarded:  "198.51.100.7, 10.0.1.6",
			want:       "198.51.100.7",
		},
		{
			name:       "x_real_ip_beats_x_forwarded_for",
			remoteAddr: "10.0.1.6:4321",
			realIP:     "203.0.113.10",
			forwarded:  "198.51.100.7, 10.0.1.6",
			want:       "203.0.113.10",
		},
		{
			name:       "invalid_header_falls_back_to_remote_addr",
			remoteAddr: "10.0.1.6:4321",
			realIP:     "not-an-ip",
			forwarded:  "also-bad, still-bad",
			want:       "10.0.1.6",
		},
		{
			name:       "ipv6_x_real_ip",
			remoteAddr: "10.0.1.6:4321",
			realIP:     "2001:db8::1",
			want:       "2001:db8::1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/events/1", nil)
			req.RemoteAddr = tc.remoteAddr
			if tc.realIP != "" {
				req.Header.Set("X-Real-IP", tc.realIP)
			}
			if tc.forwarded != "" {
				req.Header.Set("X-Forwarded-For", tc.forwarded)
			}
			if got := requestClientIP(req); got != tc.want {
				t.Fatalf("requestClientIP() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWithHTTPRateLimit_ProxyHeadersSeparateClients(t *testing.T) {
	limited := WithHTTPRateLimit(HTTPRateLimitOptions{
		DefaultRPM:   1,
		DefaultBurst: 1,
	}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	// Same proxy RemoteAddr, two distinct forwarded clients — each gets its own
	// burst token instead of sharing one global bucket.
	for _, clientIP := range []string{"203.0.113.10", "198.51.100.20"} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/events/abc", nil)
		req.RemoteAddr = "10.0.1.6:1234"
		req.Header.Set("X-Forwarded-For", clientIP)
		rec := httptest.NewRecorder()
		limited.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("client %s unexpectedly limited: status %d", clientIP, rec.Code)
		}
	}

	// A repeat from the first client should now be limited.
	repeat := httptest.NewRequest(http.MethodGet, "/api/v1/events/abc", nil)
	repeat.RemoteAddr = "10.0.1.6:1234"
	repeat.Header.Set("X-Forwarded-For", "203.0.113.10")
	repeatRec := httptest.NewRecorder()
	limited.ServeHTTP(repeatRec, repeat)
	if repeatRec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected repeat client to be rate limited, got %d", repeatRec.Code)
	}
}

func TestWithHTTPRateLimit_ExemptsHealthAndLimitsSearch(t *testing.T) {
	limited := WithHTTPRateLimit(HTTPRateLimitOptions{
		DefaultRPM:   1,
		DefaultBurst: 1,
		SearchRPM:    1,
	}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthReq.RemoteAddr = "127.0.0.1:1234"
	healthRec := httptest.NewRecorder()
	limited.ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusNoContent {
		t.Fatalf("unexpected health status: got %d want %d", healthRec.Code, http.StatusNoContent)
	}

	searchReq1 := httptest.NewRequest(http.MethodGet, "/api/v1/search?q=x", nil)
	searchReq1.RemoteAddr = "127.0.0.1:4321"
	searchRec1 := httptest.NewRecorder()
	limited.ServeHTTP(searchRec1, searchReq1)
	if searchRec1.Code != http.StatusNoContent {
		t.Fatalf("unexpected first search status: got %d want %d", searchRec1.Code, http.StatusNoContent)
	}

	searchReq2 := httptest.NewRequest(http.MethodGet, "/api/v1/search?q=x", nil)
	searchReq2.RemoteAddr = "127.0.0.1:4321"
	searchRec2 := httptest.NewRecorder()
	limited.ServeHTTP(searchRec2, searchReq2)
	if searchRec2.Code != http.StatusTooManyRequests {
		t.Fatalf("unexpected second search status: got %d want %d", searchRec2.Code, http.StatusTooManyRequests)
	}
}

func TestWithHTTPRateLimit_ClassifiedPublicBuckets(t *testing.T) {
	limited := WithHTTPRateLimit(HTTPRateLimitOptions{
		DefaultRPM:     100,
		DefaultBurst:   1,
		SearchRPM:      100,
		DiscoveryRPM:   1,
		SuggestRPM:     1,
		PublicStatsRPM: 1,
	}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	ip := "127.0.0.1:5555"
	for _, path := range []string{
		"/api/v1/discovery/notes/trending",
		"/api/v1/search/suggest?q=nost",
		"/api/v1/discovery/stats/network",
	} {
		first := httptest.NewRequest(http.MethodGet, path, nil)
		first.RemoteAddr = ip
		firstRec := httptest.NewRecorder()
		limited.ServeHTTP(firstRec, first)
		if firstRec.Code != http.StatusNoContent {
			t.Fatalf("unexpected first status for %s: got %d want %d", path, firstRec.Code, http.StatusNoContent)
		}

		second := httptest.NewRequest(http.MethodGet, path, nil)
		second.RemoteAddr = ip
		secondRec := httptest.NewRecorder()
		limited.ServeHTTP(secondRec, second)
		if secondRec.Code != http.StatusTooManyRequests {
			t.Fatalf("unexpected second status for %s: got %d want %d", path, secondRec.Code, http.StatusTooManyRequests)
		}
	}
}

func TestWithHTTPRateLimit_ConcurrentRequestsShareBucket(t *testing.T) {
	limited := WithHTTPRateLimit(HTTPRateLimitOptions{
		DefaultRPM:   1,
		DefaultBurst: 1,
	}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	const total = 24
	var allowed int32
	var denied int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func() {
			defer wg.Done()
			<-start
			req := httptest.NewRequest(http.MethodGet, "/api/v1/events/abc", nil)
			req.RemoteAddr = "127.0.0.1:9999"
			rec := httptest.NewRecorder()
			limited.ServeHTTP(rec, req)
			switch rec.Code {
			case http.StatusNoContent:
				atomic.AddInt32(&allowed, 1)
			case http.StatusTooManyRequests:
				atomic.AddInt32(&denied, 1)
			default:
				t.Errorf("unexpected status code: %d", rec.Code)
			}
		}()
	}
	close(start)
	wg.Wait()

	if allowed < 1 {
		t.Fatalf("expected at least one allowed request, got %d", allowed)
	}
	if denied < 1 {
		t.Fatalf("expected at least one denied request, got %d", denied)
	}
}

func TestWithPublicRequestGuards_RejectsInvalidAndHighCostParams(t *testing.T) {
	guarded := WithPublicRequestGuards(PublicRequestGuardOptions{
		MaxResultLimit:          20,
		MaxPageSize:             20,
		MaxPageOffset:           200,
		MaxSearchWindowHours:    24,
		MaxDiscoveryWindowHours: 7 * 24,
	}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	for _, path := range []string{
		"/api/v1/search/notes?q=nostr&limit=21",
		"/api/v1/search/notes?q=nostr&window=7d",
		"/api/v1/discovery/hashtags/trending?offset=201",
		"/api/v1/discovery/hashtags/nostr/notes?window=all",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		guarded.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unexpected guard status for %s: got %d want %d", path, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestWithPublicRequestGuards_DoesNotAffectAdminPaths(t *testing.T) {
	guarded := WithPublicRequestGuards(PublicRequestGuardOptions{
		MaxResultLimit:          1,
		MaxPageSize:             1,
		MaxPageOffset:           1,
		MaxSearchWindowHours:    1,
		MaxDiscoveryWindowHours: 1,
	}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/admin/v1/jobs?limit=9999&window=all&offset=9999", nil)
	rec := httptest.NewRecorder()
	guarded.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("unexpected admin status: got %d want %d", rec.Code, http.StatusNoContent)
	}
}

func TestWithPanicRecovery_ReturnsInternalErrorEnvelope(t *testing.T) {
	handler := WithRequestID(WithPanicRecovery(nil, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/1", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusInternalServerError)
	}
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if envelope.Error.Code != "internal_error" {
		t.Fatalf("unexpected code: got %q", envelope.Error.Code)
	}
	if envelope.Error.RequestID == "" {
		t.Fatalf("expected request id in error envelope")
	}
}

func TestLogRequests_ResolvesPathTemplateAfterDispatch(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	var buf strings.Builder
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	server := httptest.NewServer(LogRequests(logger, mux))
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/v1/events/abc123")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()

	logOutput := buf.String()
	if !strings.Contains(logOutput, "/api/v1/events/{id}") {
		t.Fatalf("expected path_template to contain route pattern, got log: %s", logOutput)
	}
	if strings.Contains(logOutput, "/_unmatched") {
		t.Fatalf("path_template should not be /_unmatched for a matched route, got log: %s", logOutput)
	}
}

func TestLogRequests_PreservesWebsocketHijacker(t *testing.T) {
	mux := http.NewServeMux()
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	mux.HandleFunc("GET /primal/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		_ = conn.Close()
	})
	server := httptest.NewServer(LogRequests(slog.Default(), mux))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	_ = conn.Close()
}
