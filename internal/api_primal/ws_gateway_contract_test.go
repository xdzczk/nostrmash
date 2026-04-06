package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestWSGateway_GetRecommendedReadsContract(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getCuratedRecommendedReadsFn: func(_ context.Context, limit int) ([]store.CuratedRecommendedRead, error) {
			return []store.CuratedRecommendedRead{
				{EventID: "evt_read_1", Title: "Read 1", URL: "https://example.com/1", Rank: 20},
				{EventID: "evt_read_2", Title: "Read 2", URL: "https://example.com/2", Rank: 10},
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
	if err := conn.WriteJSON([]any{"REQ", "sub_recommended_reads_contract", map[string]any{"cache": []any{"get_recommended_reads", map[string]any{"limit": 2}}}}); err != nil {
		t.Fatalf("write get_recommended_reads request: %v", err)
	}
	frames := make([]any, 0, 3)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read get_recommended_reads frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode get_recommended_reads frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "get_recommended_reads", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func TestWSGateway_GetReadsTopicsContract(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getCuratedReadsTopicsFn: func(_ context.Context, limit int) ([]store.CuratedReadsTopic, error) {
			return []store.CuratedReadsTopic{
				{Topic: "nostr", Rank: 10},
				{Topic: "bitcoin", Rank: 9},
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
	if err := conn.WriteJSON([]any{"REQ", "sub_reads_topics_contract", map[string]any{"cache": []any{"get_reads_topics", map[string]any{"limit": 2}}}}); err != nil {
		t.Fatalf("write get_reads_topics request: %v", err)
	}
	frames := make([]any, 0, 3)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read get_reads_topics frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode get_reads_topics frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "get_reads_topics", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func TestWSGateway_GetFeaturedAuthorsContract(t *testing.T) {
	const authorA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const authorB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := NewWSGateway(fakeEventReader{
		getCuratedFeaturedAuthorsFn: func(_ context.Context, limit int) ([]store.CuratedFeaturedAuthor, error) {
			return []store.CuratedFeaturedAuthor{
				{Pubkey: authorA, Rank: 12},
				{Pubkey: authorB, Rank: 11},
			}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				authorA: {Pubkey: authorA, MetadataEventID: "md_author_a"},
				authorB: {Pubkey: authorB, MetadataEventID: "md_author_b"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_author_a": json.RawMessage(`{"id":"md_author_a","kind":0,"pubkey":"` + authorA + `"}`),
				"md_author_b": json.RawMessage(`{"id":"md_author_b","kind":0,"pubkey":"` + authorB + `"}`),
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
	if err := conn.WriteJSON([]any{"REQ", "sub_featured_authors_contract", map[string]any{"cache": []any{"get_featured_authors", map[string]any{"limit": 2}}}}); err != nil {
		t.Fatalf("write get_featured_authors request: %v", err)
	}
	frames := make([]any, 0, 6)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read get_featured_authors frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode get_featured_authors frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "get_featured_authors", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func TestWSGateway_CreatorPaidTiersContract(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getByKindPubkeyFn: func(_ context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
			_ = kind
			_ = pubkey
			_ = limit
			return []json.RawMessage{
				json.RawMessage(`{"id":"tiers_index_evt","kind":17000,"pubkey":"pk_tiers","tags":[["e","tier_evt_1"],["e","tier_evt_2"]]}`),
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"tier_evt_1": json.RawMessage(`{"id":"tier_evt_1","kind":17001}`),
				"tier_evt_2": json.RawMessage(`{"id":"tier_evt_2","kind":17001}`),
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
	if err := conn.WriteJSON([]any{"REQ", "sub_creator_paid_tiers_contract", map[string]any{"cache": []any{"creator_paid_tiers", map[string]any{"pubkey": "pk_tiers"}}}}); err != nil {
		t.Fatalf("write creator_paid_tiers request: %v", err)
	}
	frames := make([]any, 0, 5)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read creator_paid_tiers frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode creator_paid_tiers frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "creator_paid_tiers", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func TestWSGateway_UserOfLNAddressContract(t *testing.T) {
	const pubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := NewWSGateway(fakeEventReader{
		getPubkeyByLNAddressFn: func(_ context.Context, lnAddress string) (string, error) {
			return pubkey, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				pubkey: {Pubkey: pubkey, MetadataEventID: "md_ln_user"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_ln_user": json.RawMessage(`{"id":"md_ln_user","kind":0,"pubkey":"` + pubkey + `"}`),
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
	if err := conn.WriteJSON([]any{"REQ", "sub_ln_lookup_contract", map[string]any{"cache": []any{"user_of_ln_address", map[string]any{"ln_address": "Alice@Example.com"}}}}); err != nil {
		t.Fatalf("write user_of_ln_address request: %v", err)
	}
	frames := make([]any, 0, 4)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read user_of_ln_address frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode user_of_ln_address frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "user_of_ln_address", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func TestWSGateway_ParameterizedReplaceableListContract(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamListByIdentifierFn: func(_ context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
			if pubkey != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
				return []json.RawMessage{}, nil
			}
			if kind != 30000 || identifier != "custom-list" || limit != 2 {
				return []json.RawMessage{}, nil
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"pl_evt_2","kind":30000,"pubkey":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","created_at":20,"tags":[["d","custom-list"],["p","bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]]}`),
				json.RawMessage(`{"id":"pl_evt_1","kind":30000,"pubkey":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","created_at":10,"tags":[["d","custom-list"],["p","cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"]]}`),
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

	requestPath := filepath.Join("testdata", "ws_contracts", "parameterized_replaceable_list", "success", "request.json")
	requestRaw, err := os.ReadFile(requestPath)
	if err != nil {
		t.Fatalf("read parameterized_replaceable_list request: %v", err)
	}
	var requestPayload []any
	if err := json.Unmarshal(requestRaw, &requestPayload); err != nil {
		t.Fatalf("decode parameterized_replaceable_list request: %v", err)
	}
	if err := conn.WriteJSON(requestPayload); err != nil {
		t.Fatalf("write parameterized_replaceable_list request: %v", err)
	}
	frames := make([]any, 0, 4)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read parameterized_replaceable_list frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode parameterized_replaceable_list frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "parameterized_replaceable_list", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func TestWSGateway_ParametrizedReplaceableEventsContract(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			switch {
			case pubkey == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" && kind == 30023 && dTag == "read-a":
				return json.RawMessage(`{"id":"pre_evt_a","kind":30023,"pubkey":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","created_at":30}`), nil
			case pubkey == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" && kind == 30023 && dTag == "read-b":
				return json.RawMessage(`{"id":"pre_evt_b","kind":30023,"pubkey":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","created_at":20}`), nil
			default:
				return nil, store.ErrNotFound
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

	requestPath := filepath.Join("testdata", "ws_contracts", "parametrized_replaceable_events", "success", "request.json")
	requestRaw, err := os.ReadFile(requestPath)
	if err != nil {
		t.Fatalf("read parametrized_replaceable_events request: %v", err)
	}
	var requestPayload []any
	if err := json.Unmarshal(requestRaw, &requestPayload); err != nil {
		t.Fatalf("decode parametrized_replaceable_events request: %v", err)
	}
	if err := conn.WriteJSON(requestPayload); err != nil {
		t.Fatalf("write parametrized_replaceable_events request: %v", err)
	}
	frames := make([]any, 0, 4)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read parametrized_replaceable_events frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode parametrized_replaceable_events frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "parametrized_replaceable_events", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func TestWSGateway_SearchFilterlistContract(t *testing.T) {
	const viewer = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const muted = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			if kind == 10000 && dTag == "" {
				return json.RawMessage(`{"id":"mute_base","kind":10000,"tags":[["p","` + muted + `"]]}`), nil
			}
			return nil, store.ErrNotFound
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

	if err := conn.WriteJSON([]any{"REQ", "sub_filterlist_contract", map[string]any{"cache": []any{"search_filterlist", map[string]any{
		"pubkey":      muted,
		"user_pubkey": viewer,
	}}}}); err != nil {
		t.Fatalf("write search_filterlist request: %v", err)
	}
	frames := make([]any, 0, 4)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read search_filterlist frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode search_filterlist frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "search_filterlist", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func TestWSGateway_GetHighlightsContract(t *testing.T) {
	const highlightAuthor = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := NewWSGateway(fakeEventReader{
		getHighlightsByEventFn: func(_ context.Context, eventID string, limit int) ([]json.RawMessage, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"hl_2","kind":9802,"created_at":20,"pubkey":"` + highlightAuthor + `","tags":[["e","evt_target"]]}`),
				json.RawMessage(`{"id":"hl_1","kind":9802,"created_at":10,"pubkey":"` + highlightAuthor + `","tags":[["e","evt_target"]]}`),
			}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				highlightAuthor: {Pubkey: highlightAuthor, MetadataEventID: "md_highlighter"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_highlighter": json.RawMessage(`{"id":"md_highlighter","kind":0,"pubkey":"` + highlightAuthor + `"}`),
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

	if err := conn.WriteJSON([]any{"REQ", "sub_highlights_contract", map[string]any{"cache": []any{"get_highlights", map[string]any{
		"event_id": "evt_target",
		"limit":    2,
	}}}}); err != nil {
		t.Fatalf("write get_highlights request: %v", err)
	}
	frames := make([]any, 0, 6)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read get_highlights frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode get_highlights frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "get_highlights", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func TestWSGateway_LongFormContentThreadViewContract(t *testing.T) {
	const author = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"evt_read_root","kind":30023,"pubkey":"` + author + `"}`), nil
		},
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"` + id + `","kind":30023,"pubkey":"` + author + `"}`), nil
		},
		getEventAncestors: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{json.RawMessage(`{"id":"evt_parent","kind":1,"pubkey":"` + author + `"}`)}, []string{}, nil
		},
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"reply_2","kind":1,"created_at":22,"pubkey":"` + author + `"}`),
				json.RawMessage(`{"id":"reply_1","kind":1,"created_at":11,"pubkey":"` + author + `"}`),
			}, nil, nil
		},
		getEventsByATagAndKindFn: func(_ context.Context, kind int, aTagValue string, limit int) ([]json.RawMessage, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"reply_a_only","kind":1,"created_at":33,"pubkey":"` + author + `"}`),
			}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				author: {Pubkey: author, MetadataEventID: "md_author"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_author": json.RawMessage(`{"id":"md_author","kind":0,"pubkey":"` + author + `"}`),
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

	if err := conn.WriteJSON([]any{"REQ", "sub_read_thread_contract", map[string]any{"cache": []any{"long_form_content_thread_view", map[string]any{
		"pubkey":     author,
		"kind":       30023,
		"identifier": "read-id",
		"limit":      2,
	}}}}); err != nil {
		t.Fatalf("write long_form_content_thread_view request: %v", err)
	}
	frames := make([]any, 0, 8)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read long_form_content_thread_view frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode long_form_content_thread_view frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "long_form_content_thread_view", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func TestWSGateway_IsHiddenByContentModerationContract(t *testing.T) {
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

	if err := conn.WriteJSON([]any{"REQ", "sub_hidden_contract", map[string]any{"cache": []any{"is_hidden_by_content_moderation", map[string]any{
		"user_pubkey": viewer,
		"pubkeys":     []any{muted, allowed},
		"event_ids":   []any{"evt_hidden", "evt_visible"},
		"event_id":    "evt_hidden",
	}}}}); err != nil {
		t.Fatalf("write is_hidden_by_content_moderation request: %v", err)
	}
	frames := make([]any, 0, 3)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read is_hidden_by_content_moderation frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode is_hidden_by_content_moderation frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "is_hidden_by_content_moderation", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

