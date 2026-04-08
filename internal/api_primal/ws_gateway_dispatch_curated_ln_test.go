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

func TestWSGateway_GetRecommendedReadsEmitsCuratedKindPayload(t *testing.T) {
	gateway := mustNewWSGateway(t, fakeEventReader{
		getCuratedRecommendedReadsFn: func(_ context.Context, limit int) ([]store.CuratedRecommendedRead, error) {
			if limit != 2 {
				t.Fatalf("unexpected recommended reads limit: %d", limit)
			}
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

	if err := conn.WriteJSON([]any{"REQ", "sub_recommended_reads_shape", map[string]any{
		"cache": []any{"get_recommended_reads", map[string]any{"limit": 2}},
	}}); err != nil {
		t.Fatalf("write get_recommended_reads request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	if len(events) != 1 {
		t.Fatalf("unexpected recommended reads event count: got=%d want=1", len(events))
	}
	event, ok := events[0].(map[string]any)
	if !ok || event["kind"] != float64(10000145) {
		t.Fatalf("unexpected recommended reads payload: %#v", events[0])
	}
	contentRaw, _ := event["content"].(string)
	var content struct {
		Reads []store.CuratedRecommendedRead `json:"reads"`
	}
	if err := json.Unmarshal([]byte(contentRaw), &content); err != nil {
		t.Fatalf("decode recommended reads content: %v", err)
	}
	if len(content.Reads) != 2 || content.Reads[0].EventID != "evt_read_1" || content.Reads[1].EventID != "evt_read_2" {
		t.Fatalf("unexpected recommended reads content: %#v", content)
	}
}

func TestWSGateway_GetReadsTopicsEmitsCuratedKindPayload(t *testing.T) {
	gateway := mustNewWSGateway(t, fakeEventReader{
		getCuratedReadsTopicsFn: func(_ context.Context, limit int) ([]store.CuratedReadsTopic, error) {
			if limit != 2 {
				t.Fatalf("unexpected reads topics limit: %d", limit)
			}
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

	if err := conn.WriteJSON([]any{"REQ", "sub_reads_topics_shape", map[string]any{
		"cache": []any{"get_reads_topics", map[string]any{"limit": 2}},
	}}); err != nil {
		t.Fatalf("write get_reads_topics request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	if len(events) != 1 {
		t.Fatalf("unexpected reads topics event count: got=%d want=1", len(events))
	}
	event, ok := events[0].(map[string]any)
	if !ok || event["kind"] != float64(10000146) {
		t.Fatalf("unexpected reads topics payload: %#v", events[0])
	}
	contentRaw, _ := event["content"].(string)
	var content struct {
		Topics []store.CuratedReadsTopic `json:"topics"`
	}
	if err := json.Unmarshal([]byte(contentRaw), &content); err != nil {
		t.Fatalf("decode reads topics content: %v", err)
	}
	if len(content.Topics) != 2 || content.Topics[0].Topic != "nostr" || content.Topics[1].Topic != "bitcoin" {
		t.Fatalf("unexpected reads topics content: %#v", content)
	}
}

func TestWSGateway_GetFeaturedAuthorsIncludesMetadata(t *testing.T) {
	const authorA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const authorB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := mustNewWSGateway(t, fakeEventReader{
		getCuratedFeaturedAuthorsFn: func(_ context.Context, limit int) ([]store.CuratedFeaturedAuthor, error) {
			if limit != 2 {
				t.Fatalf("unexpected featured authors limit: %d", limit)
			}
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

	if err := conn.WriteJSON([]any{"REQ", "sub_featured_authors_shape", map[string]any{
		"cache": []any{"get_featured_authors", map[string]any{"limit": 2}},
	}}); err != nil {
		t.Fatalf("write get_featured_authors request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	if len(events) != 3 {
		t.Fatalf("unexpected featured authors event count: got=%d want=3", len(events))
	}
	first, ok := events[0].(map[string]any)
	if !ok || first["kind"] != float64(10000148) {
		t.Fatalf("unexpected featured authors first payload: %#v", events[0])
	}
	contentRaw, _ := first["content"].(string)
	var content struct {
		Authors []store.CuratedFeaturedAuthor `json:"authors"`
	}
	if err := json.Unmarshal([]byte(contentRaw), &content); err != nil {
		t.Fatalf("decode featured authors content: %v", err)
	}
	if len(content.Authors) != 2 || content.Authors[0].Pubkey != authorA || content.Authors[1].Pubkey != authorB {
		t.Fatalf("unexpected featured authors content: %#v", content)
	}
	if indexOfEventID(events, "md_author_a") == -1 || indexOfEventID(events, "md_author_b") == -1 {
		t.Fatalf("expected metadata events in featured authors response")
	}
}

func TestWSGateway_CreatorPaidTiersPrefersLiveEventChain(t *testing.T) {
	gateway := mustNewWSGateway(t, fakeEventReader{
		getByKindPubkeyFn: func(_ context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
			if kind != 17000 || pubkey != "pk_tiers" || limit != 1 {
				t.Fatalf("unexpected creator paid tiers source query kind=%d pubkey=%s limit=%d", kind, pubkey, limit)
			}
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

	if err := conn.WriteJSON([]any{"REQ", "sub_creator_paid_tiers_shape", map[string]any{
		"cache": []any{"creator_paid_tiers", map[string]any{"pubkey": "pk_tiers"}},
	}}); err != nil {
		t.Fatalf("write creator_paid_tiers request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	if len(events) != 3 {
		t.Fatalf("unexpected creator paid tiers event count: got=%d want=3", len(events))
	}
	if id := eventIDFromAny(events[0]); id != "tiers_index_evt" {
		t.Fatalf("unexpected creator paid tiers index event id: %q", id)
	}
	if indexOfEventID(events, "tier_evt_1") == -1 || indexOfEventID(events, "tier_evt_2") == -1 {
		t.Fatalf("expected creator tier referenced events in response")
	}
}

func TestWSGateway_UserOfLNAddressReturnsUserPubkeyAndMetadata(t *testing.T) {
	const pubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := mustNewWSGateway(t, fakeEventReader{
		getPubkeyByLNAddressFn: func(_ context.Context, lnAddress string) (string, error) {
			switch lnAddress {
			case "alice@example.com":
				return pubkey, nil
			case "nobody@example.com":
				return "", store.ErrNotFound
			default:
				t.Fatalf("unexpected normalized ln_address: %s", lnAddress)
			}
			return "", store.ErrNotFound
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

	if err := conn.WriteJSON([]any{"REQ", "sub_ln_lookup_shape", map[string]any{
		"cache": []any{"user_of_ln_address", map[string]any{"ln_address": "Alice@Example.com"}},
	}}); err != nil {
		t.Fatalf("write user_of_ln_address request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	if len(events) != 2 {
		t.Fatalf("unexpected ln lookup event count: got=%d want=2", len(events))
	}
	event, ok := events[0].(map[string]any)
	if !ok || event["kind"] != float64(10000138) {
		t.Fatalf("unexpected user_of_ln_address payload: %#v", events[0])
	}
	contentRaw, _ := event["content"].(string)
	var content map[string]any
	if err := json.Unmarshal([]byte(contentRaw), &content); err != nil {
		t.Fatalf("decode user_of_ln_address content: %v", err)
	}
	if content["pubkey"] != pubkey {
		t.Fatalf("unexpected user_of_ln_address content: %#v", content)
	}
	if indexOfEventID(events, "md_ln_user") == -1 {
		t.Fatalf("expected metadata event for ln lookup")
	}

	if err := conn.WriteJSON([]any{"REQ", "sub_ln_lookup_shape_missing", map[string]any{
		"cache": []any{"user_of_ln_address", map[string]any{"ln_address": "nobody@example.com"}},
	}}); err != nil {
		t.Fatalf("write user_of_ln_address missing request: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read user_of_ln_address missing frame: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode user_of_ln_address missing frame: %v", err)
	}
	if len(frame) < 2 || frame[0] != "EOSE" || frame[1] != "sub_ln_lookup_shape_missing" {
		t.Fatalf("missing ln lookup should emit only EOSE: %s", string(raw))
	}
}
