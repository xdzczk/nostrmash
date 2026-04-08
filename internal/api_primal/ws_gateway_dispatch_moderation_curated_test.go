package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestWSGateway_ModerationAndCuratedCacheCalls(t *testing.T) {
	gateway := mustNewWSGateway(t, fakeEventReader{
		getModerationFn: func(_ context.Context, pubkey string, kind int) ([]string, error) {
			if kind == 10000 {
				return []string{"spam"}, nil
			}
			return []string{"trusted_pubkey"}, nil
		},
		isHiddenFn: func(_ context.Context, viewerPubkey string, eventID string) (bool, string, error) {
			return true, "spam", nil
		},
		getParamListByIdentifierFn: func(_ context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
			if kind != 30000 {
				t.Fatalf("unexpected parameterized list kind: %d", kind)
			}
			if identifier != "topic" {
				t.Fatalf("unexpected parameterized list identifier: %s", identifier)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"replaceable_list_evt"}`)}, nil
		},
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"replaceable_evt"}`), nil
		},
		getParamEventsFn: func(_ context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error) {
			return []json.RawMessage{json.RawMessage(`{"id":"replaceable_evt_2"}`)}, nil
		},
		getNetworkStatsFn: func(_ context.Context) (store.NetworkStats, error) {
			return store.NetworkStats{Events: 10, Profiles: 2, Relays: 3}, nil
		},
		getCuratedRecommendedReadsFn: func(_ context.Context, limit int) ([]store.CuratedRecommendedRead, error) {
			return []store.CuratedRecommendedRead{{EventID: "evt_read_1", Title: "Read 1", URL: "https://example.com/r1", Rank: 10}}, nil
		},
		getCuratedReadsTopicsFn: func(_ context.Context, limit int) ([]store.CuratedReadsTopic, error) {
			return []store.CuratedReadsTopic{{Topic: "nostr", Rank: 5}}, nil
		},
		getCuratedFeaturedAuthorsFn: func(_ context.Context, limit int) ([]store.CuratedFeaturedAuthor, error) {
			return []store.CuratedFeaturedAuthor{{Pubkey: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Rank: 8}}, nil
		},
		getCreatorPaidTiersFn: func(_ context.Context, pubkey string) ([]json.RawMessage, error) {
			return []json.RawMessage{json.RawMessage(`{"tier_id":"gold","title":"Gold","price_sats":1000}`)}, nil
		},
		getByKindPubkeyFn: func(_ context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
			return []json.RawMessage{}, nil
		},
		getPubkeyByLNAddressFn: func(_ context.Context, lnAddress string) (string, error) {
			return "pk_ln", nil
		},
		getModerationByIdentifierFn: func(_ context.Context, pubkey string, identifier string) ([]string, error) {
			return []string{"trusted_pubkey"}, nil
		},
	}, WSGatewayOptions{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	requests := [][]any{
		{"REQ", "sub_mutelist", map[string]any{"cache": []any{"mutelist", map[string]any{"pubkey": "pk"}}}},
		{"REQ", "sub_mutelists", map[string]any{"cache": []any{"mutelists", map[string]any{"pubkey": "pk"}}}},
		{"REQ", "sub_allowlist", map[string]any{"cache": []any{"allowlist", map[string]any{"pubkey": "pk"}}}},
		{"REQ", "sub_hidden", map[string]any{"cache": []any{"is_hidden_by_content_moderation", map[string]any{"pubkey": "pk", "event_id": "evt"}}}},
		{"REQ", "sub_param_list", map[string]any{"cache": []any{"parameterized_replaceable_list", map[string]any{"pubkey": "pk", "kind": 30023, "identifier": "topic"}}}},
		{"REQ", "sub_param_event", map[string]any{"cache": []any{"parametrized_replaceable_event", map[string]any{"pubkey": "pk", "kind": 30023, "identifier": "topic"}}}},
		{"REQ", "sub_param_events", map[string]any{"cache": []any{"parametrized_replaceable_events", map[string]any{"kind": 30023, "d_tag": "topic"}}}},
		{"REQ", "sub_stats", map[string]any{"cache": []any{"network_stats", map[string]any{}}}},
		{"REQ", "sub_server", map[string]any{"cache": []any{"server_name", map[string]any{}}}},
		{"REQ", "sub_reads", map[string]any{"cache": []any{"get_recommended_reads", map[string]any{}}}},
		{"REQ", "sub_topics", map[string]any{"cache": []any{"get_reads_topics", map[string]any{}}}},
		{"REQ", "sub_authors", map[string]any{"cache": []any{"get_featured_authors", map[string]any{}}}},
		{"REQ", "sub_tiers", map[string]any{"cache": []any{"creator_paid_tiers", map[string]any{"pubkey": "pk_tiers"}}}},
		{"REQ", "sub_ln", map[string]any{"cache": []any{"user_of_ln_address", map[string]any{"ln_address": "alice@example.com"}}}},
	}
	for _, req := range requests {
		if err := conn.WriteJSON(req); err != nil {
			t.Fatalf("write req: %v", err)
		}
		sawEvent := false
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("read response frame: %v", err)
			}
			var frame []any
			if err := json.Unmarshal(raw, &frame); err != nil {
				t.Fatalf("decode response frame: %v", err)
			}
			if len(frame) > 0 && frame[0] == "EOSE" {
				break
			}
			if len(frame) > 0 && frame[0] == "EVENT" {
				sawEvent = true
			}
		}
		if !sawEvent {
			t.Fatalf("expected at least one event for request: %#v", req)
		}
	}
}
