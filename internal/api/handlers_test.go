package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

type fakeEventReader struct {
	getEventRawByIDFn  func(context.Context, string) (json.RawMessage, error)
	getEventWithProvFn func(context.Context, string) (store.EventWithProvenance, error)
	getEventRawsByIDs  func(context.Context, []string) (map[string]json.RawMessage, error)
	getEventSeenOnByID func(context.Context, string) ([]model.EventRelay, error)
	getProfileByPubkey func(context.Context, string) (store.ProfileProjection, error)
	getProfilesByBatch func(context.Context, []string) (map[string]store.ProfileProjection, error)
	getAuthorEventsFn  func(context.Context, string, int) ([]json.RawMessage, error)
	getAuthorRepliesFn func(context.Context, string, int) ([]json.RawMessage, error)
	getEventCountsFn   func(context.Context, string) (store.EventCounts, error)
	getEventRepliesFn  func(context.Context, string, int, *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	getEventAncestors  func(context.Context, string, int) ([]json.RawMessage, []string, error)
	listRelayHealthFn  func(context.Context) ([]model.IngestCheckpoint, error)
}

func (f fakeEventReader) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	if f.getEventRawByIDFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventRawByIDFn(ctx, id)
}

func (f fakeEventReader) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if f.getEventRawsByIDs == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventRawsByIDs(ctx, ids)
}

func (f fakeEventReader) GetEventWithProvenance(ctx context.Context, id string) (store.EventWithProvenance, error) {
	if f.getEventWithProvFn == nil {
		return store.EventWithProvenance{}, errors.New("not implemented")
	}
	return f.getEventWithProvFn(ctx, id)
}

func (f fakeEventReader) GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error) {
	if f.getEventSeenOnByID == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventSeenOnByID(ctx, id)
}

func (f fakeEventReader) GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error) {
	if f.getProfileByPubkey == nil {
		return store.ProfileProjection{}, errors.New("not implemented")
	}
	return f.getProfileByPubkey(ctx, pubkey)
}

func (f fakeEventReader) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
	if f.getProfilesByBatch == nil {
		return nil, errors.New("not implemented")
	}
	return f.getProfilesByBatch(ctx, pubkeys)
}

func (f fakeEventReader) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getAuthorEventsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorEventsFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getAuthorRepliesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorRepliesFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error) {
	if f.getEventCountsFn == nil {
		return store.EventCounts{}, errors.New("not implemented")
	}
	return f.getEventCountsFn(ctx, eventID)
}

func (f fakeEventReader) GetEventReplies(
	ctx context.Context,
	eventID string,
	limit int,
	cursor *store.EventOrderCursor,
) ([]json.RawMessage, *store.EventOrderCursor, error) {
	if f.getEventRepliesFn == nil {
		return nil, nil, errors.New("not implemented")
	}
	return f.getEventRepliesFn(ctx, eventID, limit, cursor)
}

func (f fakeEventReader) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
	if f.getEventAncestors == nil {
		return nil, nil, errors.New("not implemented")
	}
	return f.getEventAncestors(ctx, eventID, maxDepth)
}

func (f fakeEventReader) ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	if f.listRelayHealthFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.listRelayHealthFn(ctx)
}

func TestBatchGetEvents_SuccessWithExplicitMissingIDs(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				ids[0]: json.RawMessage(`{"id":"evt_1","kind":1}`),
				ids[2]: json.RawMessage(`{"id":"evt_3","kind":1}`),
			}, nil
		},
	}, 10)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events/batch", strings.NewReader(`{"ids":["evt_1","evt_2","evt_3"]}`))
	rec := httptest.NewRecorder()
	handlers.BatchGetEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Events  []json.RawMessage `json:"events"`
		Missing []string          `json:"missing"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(resp.Events))
	}
	if len(resp.Missing) != 1 || resp.Missing[0] != "evt_2" {
		t.Fatalf("expected missing to contain evt_2, got %#v", resp.Missing)
	}
}

func TestGetEventByID_NotFoundUsesErrorEnvelopeAndRequestID(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getEventWithProvFn: func(_ context.Context, _ string) (store.EventWithProvenance, error) {
			return store.EventWithProvenance{}, store.ErrNotFound
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}", handlers.GetEventByID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/missing", nil)
	req.Header.Set("X-Request-ID", "req-test-123")
	rec := httptest.NewRecorder()
	WithRequestID(mux).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNotFound)
	}
	if got := rec.Header().Get("X-Request-ID"); got != "req-test-123" {
		t.Fatalf("unexpected response request id: got %q", got)
	}

	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if envelope.Error.Code != "not_found" {
		t.Fatalf("unexpected error code: got %q", envelope.Error.Code)
	}
	if envelope.Error.RequestID != "req-test-123" {
		t.Fatalf("unexpected request id in envelope: got %q", envelope.Error.RequestID)
	}
}

func TestGetEventByID_ReturnsEventAndProvenance(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getEventWithProvFn: func(_ context.Context, _ string) (store.EventWithProvenance, error) {
			return store.EventWithProvenance{
				Event: json.RawMessage(`{"id":"evt_1","kind":1}`),
				Relays: []model.EventRelay{
					{RelayURL: "wss://relay.one", SeenAt: time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC)},
				},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}", handlers.GetEventByID)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Event map[string]any `json:"event"`
		Prov  struct {
			Relays []seenOnEntry `json:"relays"`
		} `json:"provenance"`
		Consistency string `json:"consistency"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Event["id"] != "evt_1" || len(resp.Prov.Relays) != 1 || resp.Consistency != "strong" {
		t.Fatalf("unexpected response payload: %+v", resp)
	}
}

func TestBatchGetEvents_EnforcesConfiguredLimit(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{}, 2)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events/batch", strings.NewReader(`{"ids":["a","b","c"]}`))
	rec := httptest.NewRecorder()
	WithRequestID(http.HandlerFunc(handlers.BatchGetEvents)).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
	}

	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if envelope.Error.Code != "batch_limit_exceeded" {
		t.Fatalf("unexpected error code: got %q", envelope.Error.Code)
	}
	if envelope.Error.RequestID == "" {
		t.Fatal("expected generated request id in error envelope")
	}
}

func TestGetEventSeenOn_Success(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getEventSeenOnByID: func(_ context.Context, id string) ([]model.EventRelay, error) {
			return []model.EventRelay{
				{EventID: id, RelayURL: "wss://relay.one", SeenAt: time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC)},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}/seen-on", handlers.GetEventSeenOn)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_1/seen-on", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
}

func TestGetAuthorEvents_SortsAlreadyProjectedOrder(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getAuthorEventsFn: func(_ context.Context, _ string, limit int) ([]json.RawMessage, error) {
			if limit != 20 {
				t.Fatalf("unexpected default limit: %d", limit)
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"newest","created_at":1002}`),
				json.RawMessage(`{"id":"tie_b","created_at":1000}`),
				json.RawMessage(`{"id":"tie_a","created_at":1000}`),
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/events", handlers.GetAuthorEvents)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/events", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
}

func TestGetAuthorReplies_ReturnsItems(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getAuthorRepliesFn: func(_ context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
			if pubkey != "pubkey_x" || limit != 20 {
				t.Fatalf("unexpected args: pubkey=%s limit=%d", pubkey, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"reply_1"}`)}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/replies", handlers.GetAuthorReplies)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/replies", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Pubkey string            `json:"pubkey"`
		Items  []json.RawMessage `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Pubkey != "pubkey_x" || len(resp.Items) != 1 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestGetEventCounts_ExposesEventualConsistency(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getEventCountsFn: func(_ context.Context, id string) (store.EventCounts, error) {
			return store.EventCounts{
				EventID:       id,
				ReplyCount:    3,
				ReactionCount: 4,
				RepostCount:   2,
				Consistency:   "eventual",
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}/counts", handlers.GetEventCounts)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_1/counts", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Consistency string `json:"consistency"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Consistency != "eventual" {
		t.Fatalf("unexpected consistency value: got %q want %q", resp.Consistency, "eventual")
	}
}

func TestGetEventReplies_UsesCursorAndReturnsNextCursor(t *testing.T) {
	nextCursor := &store.EventOrderCursor{CreatedAt: 1001, ID: "evt_b"}
	encoded, err := encodeEventCursor(&store.EventOrderCursor{CreatedAt: 1000, ID: "evt_a"})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	handlers := NewHandlers(fakeEventReader{
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			if eventID != "evt_parent" {
				t.Fatalf("unexpected event id: %s", eventID)
			}
			if limit != 2 {
				t.Fatalf("unexpected limit: %d", limit)
			}
			if cursor == nil || cursor.CreatedAt != 1000 || cursor.ID != "evt_a" {
				t.Fatalf("unexpected cursor: %#v", cursor)
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"evt_a"}`),
				json.RawMessage(`{"id":"evt_b"}`),
			}, nextCursor, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}/replies", handlers.GetEventReplies)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_parent/replies?limit=2&cursor="+encoded, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if strings.TrimSpace(resp.NextCursor) == "" {
		t.Fatalf("expected next_cursor in response")
	}
}

func TestReady_UsesErrorEnvelopeWhenDependencyUnavailable(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	req.Header.Set("X-Request-ID", "req-ready-1")
	rec := httptest.NewRecorder()
	handler := WithRequestID(Ready(nil))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusServiceUnavailable)
	}
	var resp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Error.Code != "dependency_unavailable" {
		t.Fatalf("unexpected error code: %s", resp.Error.Code)
	}
}

func TestGetRelaysHealth_ReturnsPersistedCheckpointRows(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		listRelayHealthFn: func(_ context.Context) ([]model.IngestCheckpoint, error) {
			return []model.IngestCheckpoint{
				{
					RelayURL:    "wss://relay.one",
					Mode:        "live",
					FilterGroup: "social_core",
					Status:      "healthy",
					UpdatedAt:   time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC),
				},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/relays/health", handlers.GetRelaysHealth)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/relays/health", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Relays []relayHealthEntry `json:"relays"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Relays) != 1 || resp.Relays[0].RelayURL != "wss://relay.one" {
		t.Fatalf("unexpected relays payload: %+v", resp.Relays)
	}
}

func TestGetEventAncestors_IncludesMissingAncestorIDs(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getEventAncestors: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			if eventID != "evt_child" {
				t.Fatalf("unexpected event id: %s", eventID)
			}
			if maxDepth != 4 {
				t.Fatalf("unexpected max depth: %d", maxDepth)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_root"}`)}, []string{"evt_missing_parent"}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}/ancestors", handlers.GetEventAncestors)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_child/ancestors?max_depth=4", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Missing []string `json:"missing_ancestor_ids"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Missing) != 1 || resp.Missing[0] != "evt_missing_parent" {
		t.Fatalf("unexpected missing ids: %#v", resp.Missing)
	}
}
