package api_primal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestPrimalDMHTTPMessages_PreservesParityShapeAndNoStoreHeaders(t *testing.T) {
	const receiver = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const peer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	handlers := mustNewHandlers(t, fakeEventReader{
		getDirectMsgsRangeFn: func(_ context.Context, pubkey string, gotPeer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error) {
			if pubkey != receiver || gotPeer != peer || since != 0 || limit != 2 || offset != 0 {
				t.Fatalf("unexpected direct message args pubkey=%s peer=%s since=%d until=%d limit=%d offset=%d", pubkey, gotPeer, since, until, limit, offset)
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"dm_2","kind":4,"pubkey":"` + peer + `","created_at":20}`),
				json.RawMessage(`{"id":"dm_1","kind":4,"pubkey":"` + receiver + `","created_at":10}`),
			}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				receiver: {Pubkey: receiver, MetadataEventID: "md_receiver"},
				peer:     {Pubkey: peer, MetadataEventID: "md_peer"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_receiver": json.RawMessage(`{"id":"md_receiver","kind":0,"pubkey":"` + receiver + `"}`),
				"md_peer":     json.RawMessage(`{"id":"md_peer","kind":0,"pubkey":"` + peer + `"}`),
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /primal/v1/dms/messages", handlers.PostDirectMessages)

	req := httptest.NewRequest(http.MethodPost, "/primal/v1/dms/messages", bytes.NewBufferString(`{"pubkey":"`+receiver+`","sender":"`+peer+`","limit":2}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("expected no-store cache control, got %q", got)
	}
	if got := rec.Header().Get("Pragma"); got != "no-cache" {
		t.Fatalf("expected no-cache pragma, got %q", got)
	}

	var resp primalDMEventsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(resp.Events))
	}
	first, ok := resp.Events[0].(map[string]any)
	if !ok || first["id"] != "dm_2" {
		t.Fatalf("unexpected first event: %#v", resp.Events[0])
	}
	last, ok := resp.Events[len(resp.Events)-1].(map[string]any)
	if !ok || int(last["kind"].(float64)) != primalKindRange {
		t.Fatalf("unexpected range event: %#v", resp.Events[len(resp.Events)-1])
	}
}

func TestPrimalDMHTTPResetCount_UsesSignedAuthAndNoStoreHeaders(t *testing.T) {
	const peer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	var gotReceiver string
	var gotPeer string
	handlers := mustNewHandlers(t, fakeEventReader{
		resetDMCountFn: func(_ context.Context, receiver string, sender string) error {
			gotReceiver = receiver
			gotPeer = sender
			return nil
		},
		resetDMUnreadFn: func(_ context.Context, pubkey string, other string) error {
			if pubkey != gotReceiver || other != gotPeer {
				t.Fatalf("unexpected unread reset args pubkey=%s peer=%s", pubkey, other)
			}
			return nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /primal/v1/dms/reset-count", handlers.PostResetDirectMessageCount)

	body, err := json.Marshal(map[string]any{
		"peer_pubkey":     peer,
		"event_from_user": buildSignedAuthEvent(t),
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/primal/v1/dms/reset-count", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("expected no-store cache control, got %q", got)
	}
	if got := rec.Header().Get("Pragma"); got != "no-cache" {
		t.Fatalf("expected no-cache pragma, got %q", got)
	}

	var resp primalDMEventsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Events) != 0 {
		t.Fatalf("expected empty event list, got %#v", resp.Events)
	}
	if gotReceiver == "" || gotPeer != peer {
		t.Fatalf("unexpected reset args receiver=%s peer=%s", gotReceiver, gotPeer)
	}
}

func TestPrimalDMHTTPResetCount_RejectsFutureEvent(t *testing.T) {
	const peer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	handlers := mustNewHandlers(t, fakeEventReader{}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /primal/v1/dms/reset-count", handlers.PostResetDirectMessageCount)

	body, err := json.Marshal(map[string]any{
		"peer_pubkey":     peer,
		"event_from_user": buildSignedAuthEventAt(t, time.Now().Add(301*time.Second).Unix()),
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/primal/v1/dms/reset-count", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	var resp apiErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if resp.Error.Code != "invalid_request" || resp.Error.Message != "event from the future" {
		t.Fatalf("unexpected error payload: %#v", resp)
	}
}
