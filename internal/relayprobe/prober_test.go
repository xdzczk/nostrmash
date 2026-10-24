package relayprobe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

func TestClassifyFrame_Event(t *testing.T) {
	frame := []byte(`["EVENT","sub-1",{"id":"abc","kind":0}]`)
	if got := classifyFrame(frame); got != "EVENT" {
		t.Fatalf("expected EVENT, got %s", got)
	}
}

func TestClassifyFrame_EOSE(t *testing.T) {
	frame := []byte(`["EOSE","sub-1"]`)
	if got := classifyFrame(frame); got != "EOSE" {
		t.Fatalf("expected EOSE, got %s", got)
	}
}

func TestClassifyFrame_Notice(t *testing.T) {
	frame := []byte(`["NOTICE","rate limited"]`)
	if got := classifyFrame(frame); got != "NOTICE" {
		t.Fatalf("expected NOTICE, got %s", got)
	}
}

func TestClassifyFrame_Closed(t *testing.T) {
	frame := []byte(`["CLOSED","sub-1","auth-required:"]`)
	if got := classifyFrame(frame); got != "CLOSED" {
		t.Fatalf("expected CLOSED, got %s", got)
	}
}

func TestClassifyFrame_Invalid(t *testing.T) {
	if got := classifyFrame([]byte("not json")); got != "unknown" {
		t.Fatalf("expected unknown for invalid json, got %s", got)
	}
	if got := classifyFrame([]byte("[]")); got != "unknown" {
		t.Fatalf("expected unknown for empty array, got %s", got)
	}
	if got := classifyFrame(nil); got != "unknown" {
		t.Fatalf("expected unknown for nil, got %s", got)
	}
}

func TestTruncateError(t *testing.T) {
	short := "short error"
	if got := truncateError(errMsg(short)); got != short {
		t.Fatalf("short error should not be truncated, got %s", got)
	}

	long := make([]byte, 300)
	for i := range long {
		long[i] = 'x'
	}
	got := truncateError(errMsg(string(long)))
	if len(got) != 200 {
		t.Fatalf("expected truncated to 200 chars, got %d", len(got))
	}
}

type errMsg string

func (e errMsg) Error() string { return string(e) }

func TestProbe_ClosedAfterSubscribeIsSubscriptionClosed(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`["CLOSED","sub","auth-required"]`))
	}))
	t.Cleanup(srv.Close)

	result := NewProber(ProbeConfig{
		ConnectTimeout: time.Second,
		EOSETimeout:    time.Second,
	}).Probe(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http"))

	if !result.ConnectOK || !result.SubscribeOK || result.EOSEOK {
		t.Fatalf("expected connect and subscribe ok with eose failed, got %+v", result)
	}
	if result.Status != relayregistry.ProbeStatusSubscriptionClosed {
		t.Fatalf("expected subscription_closed, got %s", result.Status)
	}
	if result.ErrorCode != "subscription_closed" {
		t.Fatalf("expected error code subscription_closed, got %s", result.ErrorCode)
	}
}
