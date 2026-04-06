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

func TestWSGateway_REQCacheEventsThenEOSE(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			out := map[string]json.RawMessage{}
			for _, id := range ids {
				if id == "evt_1" {
					out[id] = json.RawMessage(`{"id":"evt_1","kind":1}`)
				}
			}
			return out, nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub1", map[string]any{"cache": []any{"events", map[string]any{"event_ids": []any{"evt_1", "evt_2"}}}}}); err != nil {
		t.Fatalf("write req: %v", err)
	}

	_, firstRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	_, secondRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read second frame: %v", err)
	}
	var first []any
	if err := json.Unmarshal(firstRaw, &first); err != nil {
		t.Fatalf("decode first frame: %v", err)
	}
	if len(first) < 3 || first[0] != "EVENT" || first[1] != "sub1" {
		t.Fatalf("unexpected first frame: %s", string(firstRaw))
	}
	var second []any
	if err := json.Unmarshal(secondRaw, &second); err != nil {
		t.Fatalf("decode second frame: %v", err)
	}
	if len(second) < 2 || second[0] != "EOSE" || second[1] != "sub1" {
		t.Fatalf("unexpected second frame: %s", string(secondRaw))
	}
}

func TestWSGateway_UnknownCacheRequestReturnsNotice(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{}, WSGatewayOptions{})
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

	if err := conn.WriteJSON([]any{"REQ", "sub_unknown", map[string]any{"cache": []any{"nope"}}}); err != nil {
		t.Fatalf("write req: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read notice frame: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if len(frame) < 3 || frame[0] != "NOTICE" || frame[1] != "sub_unknown" {
		t.Fatalf("unexpected frame: %s", string(raw))
	}
}

func TestWSGateway_CacheUserProfile(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getProfileByPubkey: func(_ context.Context, pubkey string) (store.ProfileProjection, error) {
			return store.ProfileProjection{
				Pubkey:            pubkey,
				MetadataEventID:   "evt_meta_1",
				MetadataCreatedAt: 1700000001,
				ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
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
	if err := conn.WriteJSON([]any{"REQ", "sub_profile", map[string]any{"cache": []any{"user_profile", map[string]any{"pubkey": "pk1"}}}}); err != nil {
		t.Fatalf("write req: %v", err)
	}
	_, firstRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	var first []any
	if err := json.Unmarshal(firstRaw, &first); err != nil {
		t.Fatalf("decode first frame: %v", err)
	}
	if len(first) < 3 || first[0] != "EVENT" || first[1] != "sub_profile" {
		t.Fatalf("unexpected first frame: %s", string(firstRaw))
	}
}

func TestWSGateway_GetBookmarksReturnsSingleLatestEvent(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			if pubkey != "pk_bookmarks" || kind != 10003 || dTag != "" {
				t.Fatalf("unexpected bookmark args pubkey=%s kind=%d dTag=%q", pubkey, kind, dTag)
			}
			return json.RawMessage(`{"id":"bookmark_evt_latest","kind":10003,"pubkey":"pk_bookmarks"}`), nil
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
	if err := conn.WriteJSON([]any{"REQ", "sub_bookmarks", map[string]any{
		"cache": []any{"get_bookmarks", map[string]any{"pubkey": "pk_bookmarks", "limit": 50}},
	}}); err != nil {
		t.Fatalf("write bookmarks req: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	if len(events) != 1 {
		t.Fatalf("expected one bookmark event, got=%d", len(events))
	}
	if id := eventIDFromAny(events[0]); id != "bookmark_evt_latest" {
		t.Fatalf("unexpected bookmark id: got=%q", id)
	}
}

func TestWSGateway_GetHighlightsByTargetIncludesMetadataAndRange(t *testing.T) {
	const highlightAuthor = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := NewWSGateway(fakeEventReader{
		getHighlightsByEventFn: func(_ context.Context, eventID string, limit int) ([]json.RawMessage, error) {
			if eventID != "evt_target" || limit != 2 {
				t.Fatalf("unexpected highlights args event_id=%s limit=%d", eventID, limit)
			}
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
	if err := conn.WriteJSON([]any{"REQ", "sub_highlights", map[string]any{
		"cache": []any{"get_highlights", map[string]any{"event_id": "evt_target", "limit": 2}},
	}}); err != nil {
		t.Fatalf("write highlights req: %v", err)
	}
	frames := readThreadStreamUntilEOSE(t, conn)
	if len(frames) != 4 {
		t.Fatalf("unexpected highlights frame count: got=%d want=4", len(frames))
	}
	if id := eventIDFromAny(frames[0]); id != "hl_2" {
		t.Fatalf("unexpected first highlight id: %q", id)
	}
	if id := eventIDFromAny(frames[1]); id != "hl_1" {
		t.Fatalf("unexpected second highlight id: %q", id)
	}
	if id := eventIDFromAny(frames[2]); id != "md_highlighter" {
		t.Fatalf("unexpected metadata id: %q", id)
	}
	rangeEvent, ok := frames[3].(map[string]any)
	if !ok || rangeEvent["kind"] != float64(10000113) {
		t.Fatalf("unexpected highlights range event: %#v", frames[3])
	}
	contentRaw, _ := rangeEvent["content"].(string)
	var content map[string]any
	if err := json.Unmarshal([]byte(contentRaw), &content); err != nil {
		t.Fatalf("decode highlights range content: %v", err)
	}
	if content["order_by"] != "created_at" || content["since"] != float64(10) || content["until"] != float64(20) {
		t.Fatalf("unexpected highlights range content: %#v", content)
	}
}

func TestWSGateway_CacheUserMentions(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getRefsPubkeyFn: func(_ context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
			if targetPubkey != "pk_mentions" || limit != 1 {
				t.Fatalf("unexpected mentions args pubkey=%s limit=%d", targetPubkey, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_mention_1","kind":1}`)}, nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub_mentions", map[string]any{"cache": []any{"user_mentions", map[string]any{"pubkey": "pk_mentions", "limit": 1}}}}); err != nil {
		t.Fatalf("write mentions req: %v", err)
	}
	_, eventRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read mentions event: %v", err)
	}
	var eventFrame []any
	if err := json.Unmarshal(eventRaw, &eventFrame); err != nil {
		t.Fatalf("decode mentions event: %v", err)
	}
	if len(eventFrame) < 2 || eventFrame[0] != "EVENT" || eventFrame[1] != "sub_mentions" {
		t.Fatalf("unexpected mentions frame: %s", string(eventRaw))
	}
}

func TestWSGateway_CacheUserFollowersUsesFollowerProjection(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getFollowersFn: func(_ context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
			if targetPubkey != "pk_followed" || limit != 1 {
				t.Fatalf("unexpected followers args pubkey=%s limit=%d", targetPubkey, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"follower_pubkey":"alice","source_event_id":"contact_evt_1"}`)}, nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub_followers", map[string]any{"cache": []any{"user_followers", map[string]any{"pubkey": "pk_followed", "limit": 1}}}}); err != nil {
		t.Fatalf("write followers req: %v", err)
	}
	_, eventRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read followers event: %v", err)
	}
	var eventFrame []any
	if err := json.Unmarshal(eventRaw, &eventFrame); err != nil {
		t.Fatalf("decode followers event: %v", err)
	}
	if len(eventFrame) < 2 || eventFrame[0] != "EVENT" || eventFrame[1] != "sub_followers" {
		t.Fatalf("unexpected followers frame: %s", string(eventRaw))
	}
}

func TestWSGateway_SearchAndCacheSearchReturnSameShape(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		searchEventsFn: func(_ context.Context, query string, limit int) ([]json.RawMessage, error) {
			return []json.RawMessage{json.RawMessage(`{"id":"evt_search_1","kind":1}`)}, nil
		},
		searchProfilesFn: func(_ context.Context, query string, limit int) ([]store.ProfileProjection, error) {
			return []store.ProfileProjection{{
				Pubkey:            "pk_search",
				MetadataEventID:   "evt_meta",
				MetadataCreatedAt: 1,
				ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
			}}, nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub_top", map[string]any{"search": "hello", "limit": 5}}); err != nil {
		t.Fatalf("write top-level search req: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read first top-level search frame: %v", err)
	}
	topProfileRaw := make([]byte, 0)
	for i := 0; i < 2; i++ {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read top-level frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode top-level frame: %v", err)
		}
		if len(frame) > 2 && frame[0] == "EVENT" {
			topProfileRaw = raw
		}
	}

	if err := conn.WriteJSON([]any{"REQ", "sub_cache", map[string]any{"cache": []any{"search", map[string]any{"query": "hello", "limit": 5}}}}); err != nil {
		t.Fatalf("write cache search req: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read first cache search frame: %v", err)
	}
	cacheProfileRaw := make([]byte, 0)
	for i := 0; i < 2; i++ {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read cache frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode cache frame: %v", err)
		}
		if len(frame) > 2 && frame[0] == "EVENT" {
			cacheProfileRaw = raw
		}
	}
	if len(topProfileRaw) == 0 || len(cacheProfileRaw) == 0 {
		t.Fatal("expected search profile payloads for both top-level and cache search")
	}
}

func TestWSGateway_NewSocialAndDMCacheCalls(t *testing.T) {
	const validPubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const peerPubkey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := NewWSGateway(fakeEventReader{
		isFollowingFn: func(_ context.Context, followerPubkey, followedPubkey string) (bool, error) {
			return followerPubkey == validPubkey && followedPubkey == peerPubkey, nil
		},
		getMutualFollowsFn: func(_ context.Context, leftPubkey, rightPubkey string, limit int) ([]string, error) {
			return []string{"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}, nil
		},
		getDMContactsDetailedFn: func(_ context.Context, receiver string, limit int, offset int, since int64, until int64) ([]json.RawMessage, error) {
			return []json.RawMessage{json.RawMessage(`{"peer_pubkey":"` + peerPubkey + `","cnt":3,"latest_event_id":"evt_dm_1"}`)}, nil
		},
		getDMCountFn: func(_ context.Context, receiver string, sender string) (int64, error) {
			return 3, nil
		},
		resetDMCountFn: func(_ context.Context, receiver string, sender string) error {
			return nil
		},
		resetDMUnreadFn: func(_ context.Context, receiver string, sender string) error {
			return nil
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

	authEvent := buildSignedAuthEvent(t)

	requests := [][]any{
		{"REQ", "sub_follow", map[string]any{"cache": []any{"is_user_following", map[string]any{"follower_pubkey": validPubkey, "followed_pubkey": peerPubkey}}}},
		{"REQ", "sub_mutual", map[string]any{"cache": []any{"mutual_follows", map[string]any{"left_pubkey": validPubkey, "right_pubkey": peerPubkey, "limit": 3}}}},
		{"REQ", "sub_dm_contacts", map[string]any{"cache": []any{"get_directmsg_contacts", map[string]any{"pubkey": validPubkey}}}},
		{"REQ", "sub_dm_count", map[string]any{"cache": []any{"directmsg_count", map[string]any{"pubkey": validPubkey}}}},
		{"REQ", "sub_dm_reset", map[string]any{"cache": []any{"reset_directmsg_count", map[string]any{"peer_pubkey": peerPubkey, "event_from_user": authEvent}}}},
	}
	for _, req := range requests {
		if err := conn.WriteJSON(req); err != nil {
			t.Fatalf("write req: %v", err)
		}
		seenEvent := false
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("read response frame: %v", err)
			}
			var frame []any
			if err := json.Unmarshal(raw, &frame); err != nil {
				t.Fatalf("decode response frame: %v", err)
			}
			if len(frame) >= 1 && frame[0] == "EVENT" {
				seenEvent = true
				continue
			}
			if len(frame) >= 1 && frame[0] == "EOSE" {
				break
			}
			t.Fatalf("unexpected response frame: %s", string(raw))
		}
		expectEvent := true
		if len(req) > 1 && (req[1] == "sub_dm_reset" || req[1] == "sub_dm_reset_all" || req[1] == "sub_dm_count") {
			expectEvent = false
		}
		if expectEvent && !seenEvent {
			t.Fatalf("expected at least one EVENT frame for request %v", req[1])
		}
		if len(req) > 1 && req[1] == "sub_dm_count" {
			if err := conn.WriteJSON([]any{"CLOSE", "sub_dm_count"}); err != nil {
				t.Fatalf("close live sub: %v", err)
			}
		}
	}
}

func TestWSGateway_ModerationAndCuratedCacheCalls(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
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

func TestWSGateway_ParameterizedReplaceableListRequiresIdentifier(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{}, WSGatewayOptions{})
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

	if err := conn.WriteJSON([]any{"REQ", "sub_param_list_missing_identifier", map[string]any{"cache": []any{"parameterized_replaceable_list", map[string]any{
		"pubkey": "pk",
		"kind":   30023,
	}}}}); err != nil {
		t.Fatalf("write parameterized_replaceable_list request: %v", err)
	}
	_, noticeRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read notice frame: %v", err)
	}
	var notice []any
	if err := json.Unmarshal(noticeRaw, &notice); err != nil {
		t.Fatalf("decode notice frame: %v", err)
	}
	if len(notice) < 3 || notice[0] != "NOTICE" {
		t.Fatalf("expected NOTICE frame, got %s", string(noticeRaw))
	}
	if message, _ := notice[2].(string); message != "request failed" {
		t.Fatalf("unexpected notice message: %q", message)
	}
	_, eoseRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read eose frame: %v", err)
	}
	var eose []any
	if err := json.Unmarshal(eoseRaw, &eose); err != nil {
		t.Fatalf("decode eose frame: %v", err)
	}
	if len(eose) < 2 || eose[0] != "EOSE" {
		t.Fatalf("expected EOSE frame, got %s", string(eoseRaw))
	}
}

func TestWSGateway_ParameterizedReplaceableListAllowsEmptyIdentifier(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamListByIdentifierFn: func(_ context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
			if pubkey != "pk" || kind != 30000 || identifier != "" || limit != 1 {
				t.Fatalf("unexpected list lookup args pubkey=%s kind=%d identifier=%q limit=%d", pubkey, kind, identifier, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"empty_identifier_evt","kind":30000}`)}, nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub_param_list_empty_identifier", map[string]any{"cache": []any{"parameterized_replaceable_list", map[string]any{
		"pubkey":     "pk",
		"identifier": "",
		"limit":      1,
	}}}}); err != nil {
		t.Fatalf("write parameterized_replaceable_list request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	ids := extractEventIDsFromThreadStream(events)
	if !ids["empty_identifier_evt"] {
		t.Fatalf("expected empty-identifier list event in response, got ids=%#v", ids)
	}
}

func TestWSGateway_ParameterizedReplaceableListSupportsDTagAlias(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamListByIdentifierFn: func(_ context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
			if pubkey != "pk" || kind != 30000 || identifier != "from-d-tag" || limit != 1 {
				t.Fatalf("unexpected list lookup args pubkey=%s kind=%d identifier=%q limit=%d", pubkey, kind, identifier, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"d_tag_alias_evt","kind":30000}`)}, nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub_param_list_d_tag", map[string]any{"cache": []any{"parameterized_replaceable_list", map[string]any{
		"pubkey": "pk",
		"d_tag":  "from-d-tag",
		"limit":  1,
	}}}}); err != nil {
		t.Fatalf("write parameterized_replaceable_list request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	ids := extractEventIDsFromThreadStream(events)
	if !ids["d_tag_alias_evt"] {
		t.Fatalf("expected d_tag alias list event in response, got ids=%#v", ids)
	}
}

func TestWSGateway_ParametrizedReplaceableEventsSupportsEventsVector(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			switch {
			case pubkey == "pk_a" && kind == 30023 && dTag == "read-a":
				return json.RawMessage(`{"id":"pr_evt_a","kind":30023,"pubkey":"pk_a"}`), nil
			case pubkey == "pk_b" && kind == 30023 && dTag == "read-b":
				return json.RawMessage(`{"id":"pr_evt_b","kind":30023,"pubkey":"pk_b"}`), nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub_param_events_vector", map[string]any{"cache": []any{"parametrized_replaceable_events", map[string]any{
		"events": []any{
			map[string]any{"pubkey": "pk_a", "kind": 30023, "identifier": "read-a"},
			map[string]any{"pubkey": "pk_missing", "kind": 30023, "identifier": "read-missing"},
			map[string]any{"pubkey": "pk_b", "kind": 30023, "d_tag": "read-b"},
		},
	}}}}); err != nil {
		t.Fatalf("write parametrized_replaceable_events request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	ids := extractEventIDsFromThreadStream(events)
	if !ids["pr_evt_a"] || !ids["pr_evt_b"] {
		t.Fatalf("missing expected parameterized events in payload: %#v", ids)
	}
	if ids["pr_evt_missing"] {
		t.Fatalf("unexpected missing parameterized event in payload: %#v", ids)
	}
}

func TestWSGateway_ParametrizedReplaceableEventSupportsIdentifierAndDTag(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			switch {
			case pubkey == "pk" && kind == 30023 && dTag == "from-identifier":
				return json.RawMessage(`{"id":"param_event_identifier","kind":30023}`), nil
			case pubkey == "pk" && kind == 30023 && dTag == "from-d-tag":
				return json.RawMessage(`{"id":"param_event_d_tag","kind":30023}`), nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub_param_event_identifier", map[string]any{"cache": []any{"parametrized_replaceable_event", map[string]any{
		"pubkey":     "pk",
		"kind":       30023,
		"identifier": "from-identifier",
	}}}}); err != nil {
		t.Fatalf("write parametrized_replaceable_event identifier request: %v", err)
	}
	identifierEvents := readThreadStreamUntilEOSE(t, conn)
	identifierIDs := extractEventIDsFromThreadStream(identifierEvents)
	if !identifierIDs["param_event_identifier"] {
		t.Fatalf("expected identifier event in response, got ids=%#v", identifierIDs)
	}

	if err := conn.WriteJSON([]any{"REQ", "sub_param_event_d_tag", map[string]any{"cache": []any{"parametrized_replaceable_event", map[string]any{
		"pubkey": "pk",
		"kind":   30023,
		"d_tag":  "from-d-tag",
	}}}}); err != nil {
		t.Fatalf("write parametrized_replaceable_event d_tag request: %v", err)
	}
	dTagEvents := readThreadStreamUntilEOSE(t, conn)
	dTagIDs := extractEventIDsFromThreadStream(dTagEvents)
	if !dTagIDs["param_event_d_tag"] {
		t.Fatalf("expected d_tag event in response, got ids=%#v", dTagIDs)
	}
}

func TestWSGateway_GetRecommendedReadsEmitsCuratedKindPayload(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
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
	gateway := NewWSGateway(fakeEventReader{
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
	gateway := NewWSGateway(fakeEventReader{
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
	gateway := NewWSGateway(fakeEventReader{
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
	gateway := NewWSGateway(fakeEventReader{
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

