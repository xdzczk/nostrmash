package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
	storeread "github.com/xdzczk/nostrmash/internal/store/read"
)

func TestGetNoteSummary_ComposesProductPayload(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return json.RawMessage(`{
				"id":"evt_1",
				"pubkey":"pk_1",
				"kind":1,
				"created_at":1700000001,
				"content":"hello",
				"tags":[["e","root_evt","","root"],["e","quote_evt","","quote"]]
			}`), nil
		},
		getEventCountsFn: func(context.Context, string) (store.EventCounts, error) {
			return store.EventCounts{
				EventID:       "evt_1",
				ReplyCount:    2,
				ReactionCount: 4,
				RepostCount:   3,
			}, nil
		},
		getNoteStatsFn: func(context.Context, string) (storeread.NoteStats, error) {
			return storeread.NoteStats{
				EventID:         "evt_1",
				ReplyCount:      2,
				ReactionCount:   4,
				RepostCount:     3,
				ZapCount:        1,
				ZapMSats:        21000,
				HasImage:        true,
				HasVideo:        false,
				HasLink:         true,
				HasArticle:      false,
				AttachmentCount: 2,
			}, nil
		},
		getEventAncestors: func(context.Context, string, int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"root_evt"}`),
				json.RawMessage(`{"id":"parent_evt"}`),
			}, []string{"missing_evt"}, nil
		},
		getProfileByPubkey: func(context.Context, string) (store.ProfileProjection, error) {
			return store.ProfileProjection{
				Pubkey:            "pk_1",
				MetadataEventID:   "meta_1",
				MetadataCreatedAt: 1234,
				ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
			}, nil
		},
		getProfilePublicStatsByPubkey: func(context.Context, string) (store.ProfilePublicStatsProjection, error) {
			return store.ProfilePublicStatsProjection{
				Pubkey:         "pk_1",
				FollowerCount:  10,
				FollowingCount: 8,
				NoteCount:      44,
				ReplyCount:     9,
			}, nil
		},
		getNoteConversationVelocityFn: func(context.Context, string) (storeread.NoteConversationVelocity, error) {
			return storeread.NoteConversationVelocity{
				Replies24h: 5,
				Replies7d:  12,
			}, nil
		},
		getNoteQuoteRepostLinkageFn: func(context.Context, string, int) (store.NoteQuoteRepostLinkageProjection, error) {
			return store.NoteQuoteRepostLinkageProjection{
				EventID:     "evt_1",
				QuoteCount:  2,
				RepostCount: 3,
				RecentActivity: []store.QuoteRepostActivityProjection{
					{
						EventID:     "repost_evt_1",
						ActorPubkey: "pk_rep",
						CreatedAt:   1700000010,
						Action:      "quote",
						Quote:       "great post",
						LinkedNote: store.QuoteRepostLinkedNoteProjection{
							EventID:      "repost_evt_1",
							AuthorPubkey: "pk_rep",
							CreatedAt:    1700000010,
							Content:      "great post",
						},
					},
				},
			}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/notes/{event_id}/summary", handlers.GetNoteSummary)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notes/evt_1/summary?include_activity=true", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode summary response: %v", err)
	}
	counts, ok := payload["counts"].(map[string]any)
	if !ok {
		t.Fatalf("missing counts payload: %#v", payload)
	}
	if counts["zap_count"].(float64) != 1 {
		t.Fatalf("unexpected zap_count in summary: %#v", counts)
	}
	media, ok := payload["media"].(map[string]any)
	if !ok {
		t.Fatalf("missing media payload: %#v", payload)
	}
	if media["has_image"] != true || media["has_link"] != true || media["attachment_count"].(float64) != 2 {
		t.Fatalf("unexpected media payload: %#v", media)
	}
	thread, ok := payload["thread"].(map[string]any)
	if !ok || thread["root_event_id"] != "root_evt" || thread["parent_event_id"] != "parent_evt" {
		t.Fatalf("unexpected thread payload: %#v", thread)
	}
	qr, ok := payload["quote_repost_context"].(map[string]any)
	if !ok {
		t.Fatalf("missing quote_repost_context payload: %#v", payload)
	}
	linkage, ok := qr["linkage"].(map[string]any)
	if !ok {
		t.Fatalf("missing linkage payload: %#v", qr)
	}
	if linkage["quote_count"].(float64) != 2 || linkage["repost_count"].(float64) != 3 {
		t.Fatalf("unexpected linkage counts: %#v", linkage)
	}
}

func TestGetNoteRelated_UsesBoundedLimitAndReturnsPayload(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getRelatedNotesFn: func(context.Context, string, int) ([]storeread.RelatedNote, error) {
			return []storeread.RelatedNote{
				{
					EventID:      "rel_1",
					AuthorPubkey: "pk_rel_1",
					CreatedAt:    1700000010,
					Content:      "related one",
					Event:        json.RawMessage(`{"id":"rel_1"}`),
					Reasons:      []string{"shared_hashtag"},
					RankScore:    50,
				},
			}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/notes/{event_id}/related", handlers.GetNoteRelated)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notes/evt_1/related?limit=10", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/notes/evt_1/related?limit=999", nil)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for limit bound: got %d want %d", badRec.Code, http.StatusBadRequest)
	}
}

func TestNoteEndpoints_MissingNoteReturnsNotFound(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
		getRelatedNotesFn: func(context.Context, string, int) ([]storeread.RelatedNote, error) {
			return nil, store.ErrNotFound
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/notes/{event_id}/summary", handlers.GetNoteSummary)
	mux.HandleFunc("GET /api/v1/notes/{event_id}/related", handlers.GetNoteRelated)

	reqSummary := httptest.NewRequest(http.MethodGet, "/api/v1/notes/missing/summary", nil)
	recSummary := httptest.NewRecorder()
	mux.ServeHTTP(recSummary, reqSummary)
	if recSummary.Code != http.StatusNotFound {
		t.Fatalf("unexpected status for missing summary: got %d want %d", recSummary.Code, http.StatusNotFound)
	}

	reqRelated := httptest.NewRequest(http.MethodGet, "/api/v1/notes/missing/related", nil)
	recRelated := httptest.NewRecorder()
	mux.ServeHTTP(recRelated, reqRelated)
	if recRelated.Code != http.StatusNotFound {
		t.Fatalf("unexpected status for missing related: got %d want %d", recRelated.Code, http.StatusNotFound)
	}
}
