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

func TestWSGateway_ModerationEmptyListsReturnEvents(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getModerationFn: func(_ context.Context, pubkey string, kind int) ([]string, error) {
			return nil, store.ErrNotFound
		},
		getModerationByIdentifierFn: func(_ context.Context, pubkey string, identifier string) ([]string, error) {
			return nil, store.ErrNotFound
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
		{"REQ", "sub_mutelist_empty", map[string]any{"cache": []any{"mutelist", map[string]any{"pubkey": "pk"}}}},
		{"REQ", "sub_mutelists_empty", map[string]any{"cache": []any{"mutelists", map[string]any{"pubkey": "pk"}}}},
		{"REQ", "sub_allowlist_empty", map[string]any{"cache": []any{"allowlist", map[string]any{"pubkey": "pk"}}}},
		{"REQ", "sub_search_filter_empty", map[string]any{"cache": []any{"search_filterlist", map[string]any{"pubkey": "pk", "query": "abc"}}}},
	}
	for _, req := range requests {
		if err := conn.WriteJSON(req); err != nil {
			t.Fatalf("write req: %v", err)
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		if len(frame) > 0 && frame[0] == "NOTICE" {
			t.Fatalf("did not expect NOTICE for empty list response, got %s", string(raw))
		}
		if len(frame) > 0 && frame[0] == "EOSE" {
			continue
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Fatalf("read eose: %v", err)
		}
	}
}

func TestWSGateway_FilterlistResponsesIncludeListEventsAndMetadata(t *testing.T) {
	const viewer = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const mutedA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const mutedB = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const allowed = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			switch {
			case kind == 10000 && dTag == "":
				return json.RawMessage(`{"id":"mute_base","kind":10000,"tags":[["p","` + mutedA + `"],["word","scam"]]}`), nil
			case kind == 30000 && dTag == "mute":
				return json.RawMessage(`{"id":"mute_named","kind":30000,"tags":[["d","mute"],["p","` + mutedB + `"]]}`), nil
			case kind == 30000 && dTag == "mutelists":
				return json.RawMessage(`{"id":"mutelists_evt","kind":30000,"tags":[["d","mutelists"],["p","` + mutedA + `"]]}`), nil
			case kind == 30000 && dTag == "allowlist":
				return json.RawMessage(`{"id":"allowlist_evt","kind":30000,"tags":[["d","allowlist"],["p","` + allowed + `"]]}`), nil
			default:
				return nil, store.ErrNotFound
			}
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				mutedA:  {Pubkey: mutedA, MetadataEventID: "md_muted_a"},
				mutedB:  {Pubkey: mutedB, MetadataEventID: "md_muted_b"},
				allowed: {Pubkey: allowed, MetadataEventID: "md_allowed"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_muted_a": json.RawMessage(`{"id":"md_muted_a","kind":0,"pubkey":"` + mutedA + `"}`),
				"md_muted_b": json.RawMessage(`{"id":"md_muted_b","kind":0,"pubkey":"` + mutedB + `"}`),
				"md_allowed": json.RawMessage(`{"id":"md_allowed","kind":0,"pubkey":"` + allowed + `"}`),
			}, nil
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

	type expectation struct {
		subID      string
		request    string
		listEvent  string
		metadataID string
	}
	cases := []expectation{
		{subID: "sub_mutelist_rich", request: "mutelist", listEvent: "mute_base", metadataID: "md_muted_a"},
		{subID: "sub_mutelists_rich", request: "mutelists", listEvent: "mutelists_evt", metadataID: "md_muted_a"},
		{subID: "sub_allowlist_rich", request: "allowlist", listEvent: "allowlist_evt", metadataID: "md_allowed"},
	}
	for _, tc := range cases {
		if err := conn.WriteJSON([]any{"REQ", tc.subID, map[string]any{"cache": []any{tc.request, map[string]any{"pubkey": viewer}}}}); err != nil {
			t.Fatalf("write %s req: %v", tc.request, err)
		}
		sawListEvent := false
		sawMetadata := false
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("read %s frame: %v", tc.request, err)
			}
			var frame []any
			if err := json.Unmarshal(raw, &frame); err != nil {
				t.Fatalf("decode %s frame: %v", tc.request, err)
			}
			if len(frame) > 0 && frame[0] == "EOSE" {
				break
			}
			if len(frame) < 3 || frame[0] != "EVENT" {
				continue
			}
			event, ok := frame[2].(map[string]any)
			if !ok {
				continue
			}
			id, _ := event["id"].(string)
			if id == tc.listEvent {
				sawListEvent = true
			}
			if id == tc.metadataID {
				sawMetadata = true
			}
		}
		if !sawListEvent || !sawMetadata {
			t.Fatalf("unexpected %s response list=%v metadata=%v", tc.request, sawListEvent, sawMetadata)
		}
	}
}

func TestWSGateway_SearchFilterlistReturnsFilteringReason(t *testing.T) {
	const viewer = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const muted = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const allowed = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			switch {
			case kind == 10000 && dTag == "":
				return json.RawMessage(`{"id":"mute_base","kind":10000,"tags":[["p","` + muted + `"],["word","scam"]]}`), nil
			case kind == 30000 && dTag == "allowlist":
				return json.RawMessage(`{"id":"allowlist_evt","kind":30000,"tags":[["d","allowlist"],["p","` + allowed + `"]]}`), nil
			default:
				return nil, store.ErrNotFound
			}
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				viewer: {Pubkey: viewer, MetadataEventID: "md_viewer"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_viewer": json.RawMessage(`{"id":"md_viewer","kind":0,"pubkey":"` + viewer + `"}`),
			}, nil
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

	assertReason := func(subID string, kwargs map[string]any, expectedAction string) {
		t.Helper()
		if err := conn.WriteJSON([]any{"REQ", subID, map[string]any{"cache": []any{"search_filterlist", kwargs}}}); err != nil {
			t.Fatalf("write search_filterlist req: %v", err)
		}
		foundReason := false
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("read search_filterlist frame: %v", err)
			}
			var frame []any
			if err := json.Unmarshal(raw, &frame); err != nil {
				t.Fatalf("decode search_filterlist frame: %v", err)
			}
			if len(frame) > 0 && frame[0] == "EOSE" {
				break
			}
			if len(frame) < 3 || frame[0] != "EVENT" {
				continue
			}
			event, ok := frame[2].(map[string]any)
			if !ok || event["kind"] != float64(10000131) {
				continue
			}
			contentRaw, _ := event["content"].(string)
			var payload map[string]any
			if err := json.Unmarshal([]byte(contentRaw), &payload); err != nil {
				t.Fatalf("decode filtering reason: %v", err)
			}
			if payload["action"] != expectedAction {
				t.Fatalf("unexpected filtering action: got=%v want=%v payload=%#v", payload["action"], expectedAction, payload)
			}
			foundReason = true
		}
		if !foundReason {
			t.Fatalf("expected filtering reason for kwargs=%#v", kwargs)
		}
	}

	assertReason("sub_filter_block", map[string]any{"pubkey": muted, "user_pubkey": viewer}, "block")
	assertReason("sub_filter_allow", map[string]any{"pubkey": allowed, "user_pubkey": viewer}, "allow")
	assertReason("sub_filter_term", map[string]any{"user_pubkey": viewer, "query": "sca"}, "block")
}

func TestWSGateway_IsHiddenByContentModerationSupportsTagAwarePubkeysAndReasons(t *testing.T) {
	const viewer = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const muted = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const allowed = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			switch {
			case kind == 10000 && dTag == "":
				return json.RawMessage(`{"id":"mute_base","kind":10000,"tags":[["p","` + muted + `"]]}`), nil
			case kind == 30000 && dTag == "allowlist":
				return json.RawMessage(`{"id":"allowlist_evt","kind":30000,"tags":[["d","allowlist"],["p","` + allowed + `"]]}`), nil
			default:
				return nil, store.ErrNotFound
			}
		},
		isHiddenFn: func(_ context.Context, viewerPubkey string, eventID string) (bool, string, error) {
			switch eventID {
			case "evt_hidden":
				return true, "muted_pubkey:" + muted, nil
			case "evt_visible":
				return false, "", nil
			default:
				return false, "", store.ErrNotFound
			}
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

	if err := conn.WriteJSON([]any{"REQ", "sub_hidden_rich", map[string]any{"cache": []any{"is_hidden_by_content_moderation", map[string]any{
		"user_pubkey": viewer,
		"pubkeys":     []any{muted, allowed},
		"event_ids":   []any{"evt_hidden", "evt_visible"},
		"event_id":    "evt_hidden",
	}}}}); err != nil {
		t.Fatalf("write hidden moderation req: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read hidden moderation frame: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode hidden moderation frame: %v", err)
	}
	if len(frame) < 3 || frame[0] != "EVENT" {
		t.Fatalf("unexpected hidden moderation frame: %s", string(raw))
	}
	event, ok := frame[2].(map[string]any)
	if !ok || event["kind"] != float64(10000137) {
		t.Fatalf("unexpected hidden moderation payload: %#v", frame[2])
	}
	if event["hidden"] != true {
		t.Fatalf("expected hidden=true for singular event payload: %#v", event)
	}
	contentRaw, _ := event["content"].(string)
	var content struct {
		Pubkeys  map[string]bool `json:"pubkeys"`
		EventIDs map[string]bool `json:"event_ids"`
		Reasons  struct {
			Pubkeys  map[string]string `json:"pubkeys"`
			EventIDs map[string]string `json:"event_ids"`
		} `json:"reasons"`
	}
	if err := json.Unmarshal([]byte(contentRaw), &content); err != nil {
		t.Fatalf("decode hidden moderation content: %v", err)
	}
	if !content.Pubkeys[muted] || content.Pubkeys[allowed] {
		t.Fatalf("unexpected pubkey hidden map: %#v", content.Pubkeys)
	}
	if !content.EventIDs["evt_hidden"] || content.EventIDs["evt_visible"] {
		t.Fatalf("unexpected event hidden map: %#v", content.EventIDs)
	}
	if !strings.HasPrefix(content.Reasons.Pubkeys[muted], "muted_pubkey:") {
		t.Fatalf("unexpected pubkey reason payload: %#v", content.Reasons.Pubkeys)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read hidden moderation eose: %v", err)
	}
}
