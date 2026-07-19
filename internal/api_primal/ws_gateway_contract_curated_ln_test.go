package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestWSGateway_GetRecommendedReadsContract(t *testing.T) {
	gateway := mustNewWSGateway(t, fakeEventReader{
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
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
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
	gateway := mustNewWSGateway(t, fakeEventReader{
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
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
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
	gateway := mustNewWSGateway(t, fakeEventReader{
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
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
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
	gateway := mustNewWSGateway(t, fakeEventReader{
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
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
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
	gateway := mustNewWSGateway(t, fakeEventReader{
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
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
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
