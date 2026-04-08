package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetThread_UsesSharedServiceAndPreservesResponseShape(t *testing.T) {
	cursor, err := encodeEventCursor(&query.EventCursor{CreatedAt: 1000, ID: "evt_cursor"})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	next := &store.EventOrderCursor{CreatedAt: 999, ID: "evt_next"}

	handlers := mustNewHandlers(t, fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, eventID string) (json.RawMessage, error) {
			if eventID != "evt_parent" {
				t.Fatalf("unexpected event id: %s", eventID)
			}
			return json.RawMessage(`{"id":"evt_parent"}`), nil
		},
		getEventAncestors: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			if eventID != "evt_parent" {
				t.Fatalf("unexpected event id: %s", eventID)
			}
			if maxDepth != 4 {
				t.Fatalf("unexpected max depth: %d", maxDepth)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_root"}`)}, []string{"evt_missing"}, nil
		},
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			if eventID != "evt_parent" {
				t.Fatalf("unexpected event id: %s", eventID)
			}
			if limit != 2 {
				t.Fatalf("unexpected limit: %d", limit)
			}
			if cursor == nil || cursor.CreatedAt != 1000 || cursor.ID != "evt_cursor" {
				t.Fatalf("unexpected cursor: %#v", cursor)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_reply_1"}`)}, next, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/threads/{eventId}", handlers.GetThread)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/threads/evt_parent?limit=2&max_depth=4&cursor="+cursor, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		EventID          string            `json:"event_id"`
		Event            json.RawMessage   `json:"event"`
		Ancestors        []json.RawMessage `json:"ancestors"`
		MissingAncestors []string          `json:"missing_ancestor_ids"`
		Replies          []json.RawMessage `json:"replies"`
		NextCursor       string            `json:"next_cursor"`
		Consistency      string            `json:"consistency"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.EventID != "evt_parent" || len(resp.Ancestors) != 1 || len(resp.Replies) != 1 || resp.Consistency != "eventual" {
		t.Fatalf("unexpected thread response: %+v", resp)
	}
	if len(resp.MissingAncestors) != 1 || resp.MissingAncestors[0] != "evt_missing" {
		t.Fatalf("unexpected missing ancestors: %#v", resp.MissingAncestors)
	}
	if strings.TrimSpace(resp.NextCursor) == "" {
		t.Fatal("expected next_cursor to be present")
	}
}

func TestGetThreadSummary_ReturnsProjectionBackedPayload(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getThreadSummaryFn: func(_ context.Context, rootEventID string) (store.ThreadSummaryProjection, error) {
			if rootEventID != "root_evt" {
				t.Fatalf("unexpected root id: %s", rootEventID)
			}
			return store.ThreadSummaryProjection{
				RootEventID:      rootEventID,
				ReplyCount:       8,
				ParticipantCount: 5,
				MaxDepth:         3,
				LastActivityAt:   1700001111,
				Replies24h:       2,
				Replies7d:        6,
				Consistency:      "eventual",
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/threads/{root_event_id}/summary", handlers.GetThreadSummary)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/threads/root_evt/summary", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var payload struct {
		RootEventID          string `json:"root_event_id"`
		ReplyCount           int64  `json:"reply_count"`
		ParticipantCount     int    `json:"participant_count"`
		MaxDepth             int    `json:"max_depth"`
		LastActivityAt       int64  `json:"last_activity_at"`
		Consistency          string `json:"consistency"`
		ConversationVelocity struct {
			Replies24h int64 `json:"replies_24h"`
			Replies7d  int64 `json:"replies_7d"`
		} `json:"conversation_velocity"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.RootEventID != "root_evt" || payload.ReplyCount != 8 || payload.ParticipantCount != 5 || payload.MaxDepth != 3 {
		t.Fatalf("unexpected summary payload: %+v", payload)
	}
	if payload.LastActivityAt != 1700001111 || payload.ConversationVelocity.Replies24h != 2 || payload.ConversationVelocity.Replies7d != 6 {
		t.Fatalf("unexpected velocity payload: %+v", payload)
	}
	if payload.Consistency != "eventual" {
		t.Fatalf("unexpected consistency: %s", payload.Consistency)
	}
}

func TestGetThreadActivity_ReturnsVelocitySnapshot(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getThreadSummaryFn: func(_ context.Context, rootEventID string) (store.ThreadSummaryProjection, error) {
			if rootEventID != "root_evt" {
				t.Fatalf("unexpected root id: %s", rootEventID)
			}
			return store.ThreadSummaryProjection{
				RootEventID:      rootEventID,
				ParticipantCount: 5,
				LastActivityAt:   1700001111,
				Replies24h:       2,
				Replies7d:        6,
				Consistency:      "eventual",
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/threads/{root_event_id}/activity", handlers.GetThreadActivity)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/threads/root_evt/activity?window=7d", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var payload struct {
		RootEventID      string  `json:"root_event_id"`
		Window           string  `json:"window"`
		ParticipantCount int     `json:"participant_count"`
		LastActivityAt   int64   `json:"last_activity_at"`
		VelocityScore    float64 `json:"velocity_score"`
		Consistency      string  `json:"consistency"`
		Activity         struct {
			Replies24h int64 `json:"replies_24h"`
			Replies7d  int64 `json:"replies_7d"`
		} `json:"activity"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.RootEventID != "root_evt" || payload.Window != "7d" || payload.ParticipantCount != 5 || payload.LastActivityAt != 1700001111 {
		t.Fatalf("unexpected activity payload: %+v", payload)
	}
	if payload.Activity.Replies24h != 2 || payload.Activity.Replies7d != 6 || payload.VelocityScore != 6 {
		t.Fatalf("unexpected activity velocity values: %+v", payload)
	}
	if payload.Consistency != "eventual" {
		t.Fatalf("unexpected consistency: %s", payload.Consistency)
	}
}
