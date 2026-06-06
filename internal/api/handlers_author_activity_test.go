package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetAuthorZaps_ReturnsSentZapsWithCursor(t *testing.T) {
	encodedCursor, err := encodeEventCursor(&query.EventCursor{CreatedAt: 1000, ID: "zap_a"})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorSentZapsFn: func(_ context.Context, pubkey string, limit int, gotCursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			if pubkey != "sender_pk" || limit != 2 {
				t.Fatalf("unexpected args: pubkey=%s limit=%d", pubkey, limit)
			}
			if gotCursor == nil || gotCursor.ID != "zap_a" || gotCursor.CreatedAt != 1000 {
				t.Fatalf("unexpected cursor: %#v", gotCursor)
			}
			return []json.RawMessage{
				json.RawMessage(`{"event_id":"zap_b","sender_pubkey":"sender_pk","target_event_id":"note_1","sats":21,"msats":21000,"created_at":999,"event":{"id":"zap_b","kind":9735}}`),
			}, nil, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/zaps", handlers.GetAuthorZaps)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/sender_pk/zaps?limit=2&cursor="+encodedCursor, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp struct {
		Pubkey      string            `json:"pubkey"`
		Zaps        []json.RawMessage `json:"zaps"`
		Consistency string            `json:"consistency"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Pubkey != "sender_pk" || len(resp.Zaps) != 1 || resp.Consistency != "eventual" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestGetAuthorZaps_ReturnsEmptyList(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorSentZapsFn: func(_ context.Context, _ string, _ int, _ *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			return []json.RawMessage{}, nil, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/zaps", handlers.GetAuthorZaps)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/receiver_only/zaps", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Zaps []json.RawMessage `json:"zaps"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Zaps == nil || len(resp.Zaps) != 0 {
		t.Fatalf("expected empty zaps array, got %+v", resp.Zaps)
	}
}

func TestGetAuthorReactions_ReturnsItems(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorReactionsFn: func(_ context.Context, pubkey string, limit int, _ *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			if pubkey != "reactor_pk" || limit != 20 {
				t.Fatalf("unexpected args: pubkey=%s limit=%d", pubkey, limit)
			}
			return []json.RawMessage{
				json.RawMessage(`{"event_id":"react_1","target_event_id":"note_1","reaction":"+","created_at":100,"event":{"id":"react_1","kind":7}}`),
			}, nil, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/reactions", handlers.GetAuthorReactions)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/reactor_pk/reactions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Reactions []json.RawMessage `json:"reactions"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Reactions) != 1 {
		t.Fatalf("expected one reaction, got %+v", resp.Reactions)
	}
}
