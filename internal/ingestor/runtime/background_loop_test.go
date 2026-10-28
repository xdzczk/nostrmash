package runtime

import (
	"bytes"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

func TestGuardBackgroundLoopRecoversPanic(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	guardBackgroundLoop(log, "contact_list_crawl", func() {
		panic("repeated read on failed websocket connection")
	})

	logged := buf.String()
	if !strings.Contains(logged, "ingestor_background_loop_panic") || !strings.Contains(logged, "contact_list_crawl") {
		t.Fatalf("panic was not logged: %s", logged)
	}

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `nostrmash_ingestor_background_loop_panics_total{loop="contact_list_crawl"}`) {
		t.Fatalf("panic metric missing:\n%s", body)
	}
}
