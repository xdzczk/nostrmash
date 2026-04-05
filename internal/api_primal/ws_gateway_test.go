package api_primal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
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

func TestWSGateway_LongFormContentFeedFollowsIncludesMetadataAndRange(t *testing.T) {
	const viewer = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const followedA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const followedB = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	gateway := NewWSGateway(fakeEventReader{
		getContactListFn: func(_ context.Context, pubkey string) (store.ContactListProjection, error) {
			if pubkey != viewer {
				t.Fatalf("unexpected contact list pubkey: %s", pubkey)
			}
			return store.ContactListProjection{
				Pubkey:          viewer,
				ContactsJSONRaw: json.RawMessage(`["` + followedA + `","` + followedB + `"]`),
			}, nil
		},
		getParamListFn: func(_ context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error) {
			if kind != 30023 || limit != 2 {
				t.Fatalf("unexpected long-form args kind=%d limit=%d", kind, limit)
			}
			switch pubkey {
			case followedA:
				return []json.RawMessage{json.RawMessage(`{"id":"read_a","kind":30023,"created_at":30,"pubkey":"` + followedA + `"}`)}, nil
			case followedB:
				return []json.RawMessage{json.RawMessage(`{"id":"read_b","kind":30023,"created_at":40,"pubkey":"` + followedB + `"}`)}, nil
			default:
				return []json.RawMessage{}, nil
			}
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				followedA: {Pubkey: followedA, MetadataEventID: "md_a"},
				followedB: {Pubkey: followedB, MetadataEventID: "md_b"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_a": json.RawMessage(`{"id":"md_a","kind":0,"pubkey":"` + followedA + `"}`),
				"md_b": json.RawMessage(`{"id":"md_b","kind":0,"pubkey":"` + followedB + `"}`),
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
	if err := conn.WriteJSON([]any{"REQ", "sub_reads_follows", map[string]any{
		"cache": []any{"long_form_content_feed", map[string]any{"pubkey": viewer, "notes": "follows", "limit": 2}},
	}}); err != nil {
		t.Fatalf("write long-form follows req: %v", err)
	}
	frames := readThreadStreamUntilEOSE(t, conn)
	if len(frames) != 5 {
		t.Fatalf("unexpected long-form follows frame count: got=%d want=5", len(frames))
	}
	if id := eventIDFromAny(frames[0]); id != "read_b" {
		t.Fatalf("unexpected first long-form event id: %q", id)
	}
	if id := eventIDFromAny(frames[1]); id != "read_a" {
		t.Fatalf("unexpected second long-form event id: %q", id)
	}
	if indexOfEventID(frames, "md_a") == -1 || indexOfEventID(frames, "md_b") == -1 {
		t.Fatalf("expected metadata frames, got ids=%v", extractOrderedReplyIDs(frames))
	}
	rangeIdx := indexOfRangeEvent(frames)
	if rangeIdx == -1 {
		t.Fatalf("missing range event for long-form follows")
	}
}

func TestWSGateway_LongFormContentThreadViewUsesParameterizedRoot(t *testing.T) {
	const author = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			if pubkey != author || kind != 30023 || dTag != "read-id" {
				t.Fatalf("unexpected parameterized args pubkey=%s kind=%d dTag=%s", pubkey, kind, dTag)
			}
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
			if kind != 1 {
				t.Fatalf("unexpected a-tag kind: %d", kind)
			}
			if aTagValue != "30023:"+author+":read-id" {
				t.Fatalf("unexpected a-tag target: %s", aTagValue)
			}
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
	if err := conn.WriteJSON([]any{"REQ", "sub_read_thread", map[string]any{
		"cache": []any{"long_form_content_thread_view", map[string]any{
			"pubkey":     author,
			"kind":       30023,
			"identifier": "read-id",
			"limit":      2,
		}},
	}}); err != nil {
		t.Fatalf("write long-form thread req: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	if indexOfEventID(events, "evt_read_root") == -1 || indexOfEventID(events, "evt_parent") == -1 {
		t.Fatalf("expected root and parent in long-form thread stream")
	}
	if indexOfEventID(events, "reply_a_only") == -1 {
		t.Fatalf("expected a-target reply in long-form thread stream")
	}
	if indexOfEventID(events, "md_author") == -1 {
		t.Fatalf("expected metadata in long-form thread stream")
	}
	if indexOfRangeEvent(events) == -1 {
		t.Fatalf("expected range marker in long-form thread stream")
	}
}

func TestWSGateway_ThreadViewContinuationSupportsCursorAndOffset(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"` + id + `","kind":1}`), nil
		},
		getEventAncestors: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{json.RawMessage(`{"id":"root_evt","kind":1}`)}, []string{}, nil
		},
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			switch {
			case cursor == nil:
				return []json.RawMessage{
					json.RawMessage(`{"id":"reply_1","created_at":1}`),
					json.RawMessage(`{"id":"reply_2","created_at":2}`),
				}, &store.EventOrderCursor{CreatedAt: 2, ID: "reply_2"}, nil
			case cursor.ID == "reply_2":
				return []json.RawMessage{
					json.RawMessage(`{"id":"reply_3","created_at":3}`),
					json.RawMessage(`{"id":"reply_4","created_at":4}`),
				}, &store.EventOrderCursor{CreatedAt: 4, ID: "reply_4"}, nil
			case cursor.ID == "reply_4":
				return []json.RawMessage{
					json.RawMessage(`{"id":"reply_5","created_at":5}`),
				}, nil, nil
			default:
				return []json.RawMessage{}, nil, nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub_thread_offset", map[string]any{
		"cache": []any{"thread_view", map[string]any{"event_id": "evt_thread", "limit": 2, "offset": 2}},
	}}); err != nil {
		t.Fatalf("write thread offset request: %v", err)
	}
	offsetEvents := readThreadStreamUntilEOSE(t, conn)
	offsetIDs := extractEventIDsFromThreadStream(offsetEvents)
	if !offsetIDs["reply_3"] || !offsetIDs["reply_2"] {
		t.Fatalf("expected offset stream to include reply_3 and reply_2, got ids=%v", keysFromBoolMap(offsetIDs))
	}
	offsetReplyOrder := extractOrderedReplyIDs(offsetEvents)
	if len(offsetReplyOrder) < 2 || offsetReplyOrder[0] != "reply_3" || offsetReplyOrder[1] != "reply_2" {
		t.Fatalf("unexpected offset reply order: %v", offsetReplyOrder)
	}
	nextCursor := extractThreadRangeNextCursor(t, offsetEvents)
	if strings.TrimSpace(nextCursor) == "" {
		t.Fatalf("expected non-empty next_cursor in offset stream")
	}

	if err := conn.WriteJSON([]any{"REQ", "sub_thread_cursor", map[string]any{
		"cache": []any{"thread_view", map[string]any{"event_id": "evt_thread", "limit": 2, "cursor": nextCursor}},
	}}); err != nil {
		t.Fatalf("write thread cursor request: %v", err)
	}
	cursorEvents := readThreadStreamUntilEOSE(t, conn)
	cursorIDs := extractEventIDsFromThreadStream(cursorEvents)
	if !cursorIDs["reply_1"] {
		t.Fatalf("expected cursor stream to include reply_1, got ids=%v", keysFromBoolMap(cursorIDs))
	}
	if next := extractThreadRangeNextCursor(t, cursorEvents); next != "" {
		t.Fatalf("expected empty next_cursor on final page, got %q", next)
	}
}

func TestWSGateway_ThreadViewRejectsMalformedCursor(t *testing.T) {
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

	if err := conn.WriteJSON([]any{"REQ", "sub_bad_thread_cursor", map[string]any{
		"cache": []any{"thread_view", map[string]any{"event_id": "evt_1", "cursor": 123}},
	}}); err != nil {
		t.Fatalf("write malformed cursor request: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read malformed cursor notice: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode malformed cursor notice: %v", err)
	}
	if len(frame) < 3 || frame[0] != "NOTICE" || frame[2] != "cursor is malformed" {
		t.Fatalf("unexpected malformed cursor frame: %s", string(raw))
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read malformed cursor eose: %v", err)
	}
}

func TestWSGateway_ThreadViewStreamOrderingAndMetadata(t *testing.T) {
	const replyPubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const rootPubkey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const eventPubkey = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	gateway := NewWSGateway(fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"evt_focus","kind":1,"pubkey":"` + eventPubkey + `"}`), nil
		},
		getEventAncestors: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"evt_root","kind":1,"pubkey":"` + rootPubkey + `"}`),
			}, []string{}, nil
		},
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"reply_1","kind":1,"created_at":11,"pubkey":"` + replyPubkey + `"}`),
				json.RawMessage(`{"id":"reply_2","kind":1,"created_at":12,"pubkey":"` + replyPubkey + `"}`),
			}, &store.EventOrderCursor{CreatedAt: 12, ID: "reply_2"}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				replyPubkey: {Pubkey: replyPubkey, MetadataEventID: "md_reply"},
				rootPubkey:  {Pubkey: rootPubkey, MetadataEventID: "md_root"},
				eventPubkey: {Pubkey: eventPubkey, MetadataEventID: "md_event"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_reply": json.RawMessage(`{"id":"md_reply","kind":0,"pubkey":"` + replyPubkey + `"}`),
				"md_root":  json.RawMessage(`{"id":"md_root","kind":0,"pubkey":"` + rootPubkey + `"}`),
				"md_event": json.RawMessage(`{"id":"md_event","kind":0,"pubkey":"` + eventPubkey + `"}`),
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
	if err := conn.WriteJSON([]any{"REQ", "sub_thread_order", map[string]any{
		"cache": []any{"thread_view", map[string]any{"event_id": "evt_focus", "limit": 2}},
	}}); err != nil {
		t.Fatalf("write thread ordering req: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	if len(events) < 6 {
		t.Fatalf("expected stream events (replies+metadata+range+parents), got=%d", len(events))
	}
	firstID := eventIDFromAny(events[0])
	secondID := eventIDFromAny(events[1])
	if firstID != "reply_2" || secondID != "reply_1" {
		t.Fatalf("expected reply page first, got first=%q second=%q", firstID, secondID)
	}
	rangeIndex := indexOfRangeEvent(events)
	ancestorIndex := indexOfEventID(events, "evt_root")
	eventIndex := indexOfEventID(events, "evt_focus")
	if rangeIndex == -1 || ancestorIndex == -1 || eventIndex == -1 {
		t.Fatalf("missing range/ancestor/event markers: range=%d ancestor=%d event=%d", rangeIndex, ancestorIndex, eventIndex)
	}
	if !(rangeIndex < ancestorIndex && rangeIndex < eventIndex) {
		t.Fatalf("expected range before ancestors/event, got range=%d ancestor=%d event=%d", rangeIndex, ancestorIndex, eventIndex)
	}
	if indexOfEventID(events, "md_reply") == -1 || indexOfEventID(events, "md_root") == -1 || indexOfEventID(events, "md_event") == -1 {
		t.Fatalf("expected metadata events in stream, ids=%v", extractOrderedReplyIDs(events))
	}
}

func TestWSGateway_ThreadViewContinuation_NoDuplicatesOrSkips(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"` + id + `","kind":1}`), nil
		},
		getEventAncestors: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{}, []string{}, nil
		},
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			all := []struct {
				id string
				ts int64
			}{
				{id: "reply_1", ts: 1},
				{id: "reply_2", ts: 2},
				{id: "reply_3", ts: 3},
				{id: "reply_4", ts: 4},
				{id: "reply_5", ts: 5},
			}
			start := 0
			if cursor != nil {
				for i, entry := range all {
					if entry.id == cursor.ID && entry.ts == cursor.CreatedAt {
						start = i + 1
						break
					}
				}
			}
			end := start + limit
			if end > len(all) {
				end = len(all)
			}
			if start > len(all) {
				start = len(all)
			}
			replies := make([]json.RawMessage, 0, end-start)
			for _, entry := range all[start:end] {
				replies = append(replies, json.RawMessage(`{"id":"`+entry.id+`","created_at":`+strconv.FormatInt(entry.ts, 10)+`}`))
			}
			var next *store.EventOrderCursor
			if end < len(all) && end > 0 {
				last := all[end-1]
				next = &store.EventOrderCursor{CreatedAt: last.ts, ID: last.id}
			}
			return replies, next, nil
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

	seen := make(map[string]struct{})
	ordered := make([]string, 0, 4)
	cursor := ""
	offset := 1 // Skip newest reply_5 in desc order, then paginate through reply_4..reply_1.
	for {
		kwargs := map[string]any{
			"event_id": "evt_thread",
			"limit":    2,
		}
		if cursor == "" {
			kwargs["offset"] = offset
		} else {
			kwargs["cursor"] = cursor
		}
		if err := conn.WriteJSON([]any{"REQ", "sub_thread_no_dupes", map[string]any{
			"cache": []any{"thread_view", kwargs},
		}}); err != nil {
			t.Fatalf("write thread request: %v", err)
		}
		events := readThreadStreamUntilEOSE(t, conn)
		ids := extractOrderedReplyIDs(events)
		for _, id := range ids {
			if _, exists := seen[id]; exists {
				t.Fatalf("duplicate reply id across pages: %s", id)
			}
			seen[id] = struct{}{}
			ordered = append(ordered, id)
		}
		next := extractThreadRangeNextCursor(t, events)
		if strings.TrimSpace(next) == "" {
			break
		}
		cursor = next
		offset = 0
	}

	want := []string{"reply_4", "reply_3", "reply_2", "reply_1"}
	if len(ordered) != len(want) {
		t.Fatalf("unexpected combined page count: got=%d want=%d (%v)", len(ordered), len(want), ordered)
	}
	for i := range want {
		if ordered[i] != want[i] {
			t.Fatalf("unexpected reply order at %d: got=%s want=%s (%v)", i, ordered[i], want[i], ordered)
		}
	}
}

func TestThreadView_HTTPAndWSMemberParity(t *testing.T) {
	reader := fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"evt_1","kind":1}`), nil
		},
		getEventAncestors: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{json.RawMessage(`{"id":"root_evt","kind":1}`)}, []string{"missing_root"}, nil
		},
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			return []json.RawMessage{json.RawMessage(`{"id":"reply_1","created_at":1}`)}, &store.EventOrderCursor{CreatedAt: 1, ID: "reply_1"}, nil
		},
	}

	httpHandlers := NewHandlers(reader, 10)
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("GET /primal/v1/threads/{eventId}", httpHandlers.GetThreadView)
	httpReq := httptest.NewRequest(http.MethodGet, "/primal/v1/threads/evt_1?limit=1", nil)
	httpRec := httptest.NewRecorder()
	httpMux.ServeHTTP(httpRec, httpReq)
	if httpRec.Code != http.StatusOK {
		t.Fatalf("unexpected http status: got=%d want=%d", httpRec.Code, http.StatusOK)
	}
	var httpPayload map[string]any
	if err := json.NewDecoder(httpRec.Body).Decode(&httpPayload); err != nil {
		t.Fatalf("decode http payload: %v", err)
	}

	wsGateway := NewWSGateway(reader, WSGatewayOptions{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", wsGateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteJSON([]any{"REQ", "sub_thread_parity", map[string]any{
		"cache": []any{"thread_view", map[string]any{"event_id": "evt_1", "limit": 1}},
	}}); err != nil {
		t.Fatalf("write thread parity request: %v", err)
	}
	wsEvents := readThreadStreamUntilEOSE(t, conn)
	wsIDs := extractEventIDsFromThreadStream(wsEvents)

	httpIDs := map[string]bool{}
	if event, ok := httpPayload["event"].(map[string]any); ok {
		if id, _ := event["id"].(string); strings.TrimSpace(id) != "" {
			httpIDs[id] = true
		}
	}
	if ancestors, ok := httpPayload["ancestors"].([]any); ok {
		for _, value := range ancestors {
			if event, ok := value.(map[string]any); ok {
				if id, _ := event["id"].(string); strings.TrimSpace(id) != "" {
					httpIDs[id] = true
				}
			}
		}
	}
	if replies, ok := httpPayload["replies"].([]any); ok {
		for _, value := range replies {
			if event, ok := value.(map[string]any); ok {
				if id, _ := event["id"].(string); strings.TrimSpace(id) != "" {
					httpIDs[id] = true
				}
			}
		}
	}
	for id := range httpIDs {
		if !wsIDs[id] {
			t.Fatalf("ws stream missing id from http thread payload: %s (ws=%v)", id, keysFromBoolMap(wsIDs))
		}
	}
	if gotWS := extractThreadRangeNextCursor(t, wsEvents); gotWS != httpPayload["next_cursor"] {
		t.Fatalf("next_cursor mismatch: ws=%v http=%v", gotWS, httpPayload["next_cursor"])
	}
}

func readThreadStreamUntilEOSE(t *testing.T, conn *websocket.Conn) []any {
	t.Helper()
	events := make([]any, 0, 8)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read ws frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode ws frame: %v", err)
		}
		if len(frame) == 0 {
			continue
		}
		if frame[0] == "EOSE" {
			break
		}
		if frame[0] == "NOTICE" {
			t.Fatalf("unexpected notice frame: %s", string(raw))
		}
		if frame[0] != "EVENT" || len(frame) < 3 {
			t.Fatalf("unexpected ws frame: %s", string(raw))
		}
		events = append(events, frame[2])
	}
	if len(events) == 0 {
		t.Fatalf("no event payload received before EOSE")
	}
	return events
}

func extractThreadRangeNextCursor(t *testing.T, events []any) string {
	t.Helper()
	for _, raw := range events {
		event, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		kind, ok := event["kind"].(float64)
		if !ok || int(kind) != 10000113 {
			continue
		}
		content, _ := event["content"].(string)
		var payload map[string]any
		if err := json.Unmarshal([]byte(content), &payload); err != nil {
			t.Fatalf("decode thread range content: %v", err)
		}
		next, _ := payload["next_cursor"].(string)
		return next
	}
	t.Fatalf("range event not found in thread stream")
	return ""
}

func extractEventIDsFromThreadStream(events []any) map[string]bool {
	out := make(map[string]bool)
	for _, raw := range events {
		event, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id, _ := event["id"].(string)
		id = strings.TrimSpace(id)
		if id != "" {
			out[id] = true
		}
	}
	return out
}

func extractOrderedReplyIDs(events []any) []string {
	out := make([]string, 0, len(events))
	for _, raw := range events {
		event, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id, _ := event["id"].(string)
		id = strings.TrimSpace(id)
		if !strings.HasPrefix(id, "reply_") {
			continue
		}
		out = append(out, id)
	}
	return out
}

func keysFromBoolMap(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func eventIDFromAny(value any) string {
	event, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	id, _ := event["id"].(string)
	return strings.TrimSpace(id)
}

func indexOfEventID(events []any, id string) int {
	for i, value := range events {
		if eventIDFromAny(value) == id {
			return i
		}
	}
	return -1
}

func indexOfRangeEvent(events []any) int {
	for i, value := range events {
		event, ok := value.(map[string]any)
		if !ok {
			continue
		}
		kind, ok := event["kind"].(float64)
		if ok && int(kind) == 10000113 {
			return i
		}
	}
	return -1
}

func TestWSGateway_OneShotRequestsDoNotExhaustSubscriptions(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{}, nil
		},
	}, WSGatewayOptions{MaxSubscriptions: 1})
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

	if err := conn.WriteJSON([]any{"REQ", "sub1", map[string]any{"cache": []any{"nope"}}}); err != nil {
		t.Fatalf("write req 1: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil { // notice for unknown request
		t.Fatalf("read req1 frame: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil { // eose
		t.Fatalf("read req1 eose: %v", err)
	}

	if err := conn.WriteJSON([]any{"REQ", "sub2", map[string]any{"cache": []any{"events", map[string]any{"event_ids": []any{"evt_1"}}}}}); err != nil {
		t.Fatalf("write req 2: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read req2 frame: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if len(frame) < 2 || frame[0] != "EOSE" || frame[1] != "sub2" {
		t.Fatalf("unexpected frame: %s", string(raw))
	}
}

func TestWSGateway_EnforcesRateLimit(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{}, WSGatewayOptions{MaxReqPerMinute: 1})
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

	if err := conn.WriteJSON([]any{"REQ", "sub1", map[string]any{"cache": []any{"nope"}}}); err != nil {
		t.Fatalf("write req1: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read req1 notice: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read req1 eose: %v", err)
	}

	if err := conn.WriteJSON([]any{"REQ", "sub2", map[string]any{"cache": []any{"nope"}}}); err != nil {
		t.Fatalf("write req2: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read req2 frame: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if len(frame) < 3 || frame[0] != "NOTICE" || frame[2] != "rate limit exceeded" {
		t.Fatalf("unexpected frame: %s", string(raw))
	}
}

func TestWSGateway_DirectMessagesRateLimitAndValidation(t *testing.T) {
	const validPubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const peerPubkey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := NewWSGateway(fakeEventReader{
		getDirectMsgsRangeFn: func(_ context.Context, pubkey string, peer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error) {
			if pubkey != validPubkey || peer != peerPubkey || limit != 1 {
				t.Fatalf("unexpected DM query args pubkey=%s peer=%s limit=%d", pubkey, peer, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"dm_evt_1","kind":4}`)}, nil
		},
	}, WSGatewayOptions{
		MaxReqPerMinute:   10,
		MaxDMReqPerMinute: 1,
	})
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

	if err := conn.WriteJSON([]any{"REQ", "dm_sub_1", map[string]any{"cache": []any{"get_directmsgs", map[string]any{"pubkey": validPubkey, "peer_pubkey": peerPubkey, "limit": 1}}}}); err != nil {
		t.Fatalf("write req1: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read req1 event: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read req1 range: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read req1 eose: %v", err)
	}

	if err := conn.WriteJSON([]any{"REQ", "dm_sub_2", map[string]any{"cache": []any{"get_directmsgs", map[string]any{"pubkey": validPubkey, "peer_pubkey": peerPubkey, "limit": 1}}}}); err != nil {
		t.Fatalf("write req2: %v", err)
	}
	_, rateLimitedRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read req2 notice: %v", err)
	}
	var rateLimited []any
	if err := json.Unmarshal(rateLimitedRaw, &rateLimited); err != nil {
		t.Fatalf("decode req2 notice: %v", err)
	}
	if len(rateLimited) < 3 || rateLimited[0] != "NOTICE" || rateLimited[2] != "dm rate limit exceeded" {
		t.Fatalf("unexpected rate-limited frame: %s", string(rateLimitedRaw))
	}

	conn.Close()
	conn, _, err = websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws for validation case: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteJSON([]any{"REQ", "bad_dm", map[string]any{"cache": []any{"get_directmsgs", map[string]any{"pubkey": "not_hex", "peer_pubkey": peerPubkey, "limit": 1}}}}); err != nil {
		t.Fatalf("write invalid dm req: %v", err)
	}
	_, invalidRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read invalid dm notice: %v", err)
	}
	var invalid []any
	if err := json.Unmarshal(invalidRaw, &invalid); err != nil {
		t.Fatalf("decode invalid dm notice: %v", err)
	}
	if len(invalid) < 3 || invalid[0] != "NOTICE" || invalid[2] != "invalid pubkey" {
		t.Fatalf("unexpected invalid dm frame: %s", string(invalidRaw))
	}
}

func TestWSGateway_DirectMessageCountContracts(t *testing.T) {
	const validPubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := NewWSGateway(fakeEventReader{
		getDMCountFn: func(_ context.Context, receiver string, sender string) (int64, error) {
			return 7, nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_count", map[string]any{"cache": []any{"directmsg_count", map[string]any{"pubkey": validPubkey}}}}); err != nil {
		t.Fatalf("write directmsg_count req: %v", err)
	}
	_, firstRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first directmsg_count frame: %v", err)
	}
	var firstFrame []any
	if err := json.Unmarshal(firstRaw, &firstFrame); err != nil {
		t.Fatalf("decode first directmsg_count frame: %v", err)
	}
	if len(firstFrame) < 2 || firstFrame[0] != "EOSE" || firstFrame[1] != "sub_dm_count" {
		t.Fatalf("directmsg_count first frame must be EOSE: %s", string(firstRaw))
	}
	if err := conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond)); err != nil {
		t.Fatalf("set directmsg_count read deadline: %v", err)
	}
	_, countRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read broadcast directmsg_count frame: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	var countFrame []any
	if err := json.Unmarshal(countRaw, &countFrame); err != nil {
		t.Fatalf("decode broadcast directmsg_count frame: %v", err)
	}
	if len(countFrame) < 3 || countFrame[0] != "EVENT" || countFrame[1] != "sub_dm_count" {
		t.Fatalf("unexpected broadcast directmsg_count envelope: %s", string(countRaw))
	}
	event, ok := countFrame[2].(map[string]any)
	if !ok {
		t.Fatalf("directmsg_count payload is not object: %s", string(countRaw))
	}
	if event["kind"] != float64(10000117) || event["cnt"] != float64(7) {
		t.Fatalf("unexpected directmsg_count payload: %s", string(countRaw))
	}
	if _, exists := event["pubkey"]; exists {
		t.Fatalf("directmsg_count must not expose pubkey in payload: %s", string(countRaw))
	}
	if _, exists := event["sender"]; exists {
		t.Fatalf("directmsg_count must not expose sender in payload: %s", string(countRaw))
	}
	if err := conn.WriteJSON([]any{"CLOSE", "sub_dm_count"}); err != nil {
		t.Fatalf("close live dm count sub: %v", err)
	}

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_count_2", map[string]any{"cache": []any{"directmsg_count_2", map[string]any{"pubkey": validPubkey}}}}); err != nil {
		t.Fatalf("write directmsg_count_2 req: %v", err)
	}
	_, first2Raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first directmsg_count_2 frame: %v", err)
	}
	var first2Frame []any
	if err := json.Unmarshal(first2Raw, &first2Frame); err != nil {
		t.Fatalf("decode first directmsg_count_2 frame: %v", err)
	}
	if len(first2Frame) < 2 || first2Frame[0] != "EOSE" || first2Frame[1] != "sub_dm_count_2" {
		t.Fatalf("directmsg_count_2 first frame must be EOSE: %s", string(first2Raw))
	}
	if err := conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond)); err != nil {
		t.Fatalf("set directmsg_count_2 read deadline: %v", err)
	}
	_, count2Raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read broadcast directmsg_count_2 frame: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	var count2Frame []any
	if err := json.Unmarshal(count2Raw, &count2Frame); err != nil {
		t.Fatalf("decode broadcast directmsg_count_2 frame: %v", err)
	}
	if len(count2Frame) < 3 || count2Frame[0] != "EVENT" || count2Frame[1] != "sub_dm_count_2" {
		t.Fatalf("unexpected broadcast directmsg_count_2 envelope: %s", string(count2Raw))
	}
	event2, ok := count2Frame[2].(map[string]any)
	if !ok {
		t.Fatalf("directmsg_count_2 payload is not object: %s", string(count2Raw))
	}
	if event2["kind"] != float64(10000134) || event2["content"] != "7" {
		t.Fatalf("unexpected directmsg_count_2 payload: %s", string(count2Raw))
	}
	if _, exists := event2["cnt"]; exists {
		t.Fatalf("directmsg_count_2 must not expose cnt top-level: %s", string(count2Raw))
	}
}

func TestWSGateway_DirectMessageCountLiveRejectsInvalidPubkey(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getDMCountFn: func(_ context.Context, receiver string, sender string) (int64, error) {
			return 7, nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_count_invalid", map[string]any{
		"cache": []any{"directmsg_count", map[string]any{"pubkey": "not-a-valid-pubkey"}},
	}}); err != nil {
		t.Fatalf("write invalid directmsg_count req: %v", err)
	}

	_, noticeRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read notice frame: %v", err)
	}
	var notice []any
	if err := json.Unmarshal(noticeRaw, &notice); err != nil {
		t.Fatalf("decode notice frame: %v", err)
	}
	if len(notice) < 3 || notice[0] != "NOTICE" || notice[1] != "sub_dm_count_invalid" || notice[2] != "invalid pubkey" {
		t.Fatalf("unexpected notice frame: %s", string(noticeRaw))
	}

	_, eoseRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read eose frame: %v", err)
	}
	var eose []any
	if err := json.Unmarshal(eoseRaw, &eose); err != nil {
		t.Fatalf("decode eose frame: %v", err)
	}
	if len(eose) < 2 || eose[0] != "EOSE" || eose[1] != "sub_dm_count_invalid" {
		t.Fatalf("unexpected eose frame: %s", string(eoseRaw))
	}
}

func TestWSGateway_GetDirectMsgsContractOrdering(t *testing.T) {
	const receiver = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const sender = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := NewWSGateway(fakeEventReader{
		getDirectMsgsRangeFn: func(_ context.Context, pubkey string, peer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"dm_2","kind":4,"created_at":20}`),
				json.RawMessage(`{"id":"dm_1","kind":4,"created_at":10}`),
			}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				receiver: {Pubkey: receiver, MetadataEventID: "md_receiver"},
				sender:   {Pubkey: sender, MetadataEventID: "md_sender"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_receiver": json.RawMessage(`{"id":"md_receiver","kind":0,"pubkey":"` + receiver + `"}`),
				"md_sender":   json.RawMessage(`{"id":"md_sender","kind":0,"pubkey":"` + sender + `"}`),
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

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_msgs", map[string]any{"cache": []any{"get_directmsgs", map[string]any{"pubkey": receiver, "peer_pubkey": sender, "limit": 2}}}}); err != nil {
		t.Fatalf("write get_directmsgs req: %v", err)
	}

	frames := make([][]any, 0, 8)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
		frames = append(frames, frame)
	}
	if len(frames) != 5 {
		t.Fatalf("unexpected event count: got=%d want=5", len(frames))
	}
	if event, ok := frames[0][2].(map[string]any); !ok || event["id"] != "dm_2" {
		t.Fatalf("unexpected first message frame: %#v", frames[0])
	}
	if event, ok := frames[1][2].(map[string]any); !ok || event["id"] != "dm_1" {
		t.Fatalf("unexpected second message frame: %#v", frames[1])
	}
	if event, ok := frames[2][2].(map[string]any); !ok || event["id"] != "md_receiver" {
		t.Fatalf("unexpected receiver metadata frame: %#v", frames[2])
	}
	if event, ok := frames[3][2].(map[string]any); !ok || event["id"] != "md_sender" {
		t.Fatalf("unexpected sender metadata frame: %#v", frames[3])
	}
	rangeEvent, ok := frames[4][2].(map[string]any)
	if !ok || rangeEvent["kind"] != float64(10000113) {
		t.Fatalf("unexpected range frame: %#v", frames[4])
	}
	contentRaw, _ := rangeEvent["content"].(string)
	var rangeContent map[string]any
	if err := json.Unmarshal([]byte(contentRaw), &rangeContent); err != nil {
		t.Fatalf("decode range content: %v", err)
	}
	if rangeContent["order_by"] != "created_at" || rangeContent["since"] != float64(10) || rangeContent["until"] != float64(20) {
		t.Fatalf("unexpected range content: %#v", rangeContent)
	}
}

func TestWSGateway_GetDirectMsgContactsContractOrdering(t *testing.T) {
	const receiver = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const peer1 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const peer2 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	gateway := NewWSGateway(fakeEventReader{
		getDMContactsDetailedFn: func(_ context.Context, r string, limit int, offset int, since int64, until int64) ([]json.RawMessage, error) {
			return []json.RawMessage{
				json.RawMessage(`{"peer_pubkey":"` + peer1 + `","cnt":3,"latest_at":30,"latest_event_id":"dm_latest_1"}`),
				json.RawMessage(`{"peer_pubkey":"` + peer2 + `","cnt":1,"latest_at":20,"latest_event_id":"dm_latest_2"}`),
			}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				peer1: {Pubkey: peer1, MetadataEventID: "md_peer_1"},
				peer2: {Pubkey: peer2, MetadataEventID: "md_peer_2"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"dm_latest_1": json.RawMessage(`{"id":"dm_latest_1","kind":4}`),
				"dm_latest_2": json.RawMessage(`{"id":"dm_latest_2","kind":4}`),
				"md_peer_1":   json.RawMessage(`{"id":"md_peer_1","kind":0,"pubkey":"` + peer1 + `"}`),
				"md_peer_2":   json.RawMessage(`{"id":"md_peer_2","kind":0,"pubkey":"` + peer2 + `"}`),
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

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_contacts_contract", map[string]any{"cache": []any{"get_directmsg_contacts", map[string]any{"pubkey": receiver}}}}); err != nil {
		t.Fatalf("write get_directmsg_contacts req: %v", err)
	}
	frames := make([][]any, 0, 10)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
		frames = append(frames, frame)
	}
	if len(frames) != 6 {
		t.Fatalf("unexpected event count: got=%d want=6", len(frames))
	}
	countsEvent, ok := frames[0][2].(map[string]any)
	if !ok || countsEvent["kind"] != float64(10000118) {
		t.Fatalf("unexpected directmsg_counts frame: %#v", frames[0])
	}
	contentRaw, _ := countsEvent["content"].(string)
	var countsContent map[string]map[string]any
	if err := json.Unmarshal([]byte(contentRaw), &countsContent); err != nil {
		t.Fatalf("decode directmsg_counts content: %v", err)
	}
	if countsContent[peer1]["cnt"] != float64(3) || countsContent[peer2]["cnt"] != float64(1) {
		t.Fatalf("unexpected directmsg_counts payload: %#v", countsContent)
	}
	if event, ok := frames[1][2].(map[string]any); !ok || event["id"] != "dm_latest_1" {
		t.Fatalf("unexpected latest dm frame 1: %#v", frames[1])
	}
	if event, ok := frames[2][2].(map[string]any); !ok || event["id"] != "dm_latest_2" {
		t.Fatalf("unexpected latest dm frame 2: %#v", frames[2])
	}
	if event, ok := frames[3][2].(map[string]any); !ok || event["id"] != "md_peer_1" {
		t.Fatalf("unexpected metadata frame 1: %#v", frames[3])
	}
	if event, ok := frames[4][2].(map[string]any); !ok || event["id"] != "md_peer_2" {
		t.Fatalf("unexpected metadata frame 2: %#v", frames[4])
	}
	rangeEvent, ok := frames[5][2].(map[string]any)
	if !ok || rangeEvent["kind"] != float64(10000113) {
		t.Fatalf("unexpected range frame: %#v", frames[5])
	}
	rangeContentRaw, _ := rangeEvent["content"].(string)
	var rangeContent map[string]any
	if err := json.Unmarshal([]byte(rangeContentRaw), &rangeContent); err != nil {
		t.Fatalf("decode range content: %v", err)
	}
	if rangeContent["order_by"] != "latest_at" || rangeContent["since"] != float64(20) || rangeContent["until"] != float64(30) {
		t.Fatalf("unexpected range content: %#v", rangeContent)
	}
}

func TestWSGateway_GetDirectMsgContactsRelationFiltering(t *testing.T) {
	const receiver = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const followedPeer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const otherPeer = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	gateway := NewWSGateway(fakeEventReader{
		getDMContactsDetailedFn: func(_ context.Context, r string, limit int, offset int, since int64, until int64) ([]json.RawMessage, error) {
			return []json.RawMessage{
				json.RawMessage(`{"peer_pubkey":"` + followedPeer + `","cnt":3,"latest_at":30,"latest_event_id":"dm_latest_1"}`),
				json.RawMessage(`{"peer_pubkey":"` + otherPeer + `","cnt":1,"latest_at":20,"latest_event_id":"dm_latest_2"}`),
			}, nil
		},
		getContactListFn: func(_ context.Context, pubkey string) (store.ContactListProjection, error) {
			return store.ContactListProjection{Pubkey: receiver, ContactsJSONRaw: json.RawMessage(`["` + followedPeer + `"]`)}, nil
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

	checkPeerSet := func(subID string, relation string, expectedPeers []string) {
		t.Helper()
		if err := conn.WriteJSON([]any{"REQ", subID, map[string]any{"cache": []any{"get_directmsg_contacts", map[string]any{"pubkey": receiver, "relation": relation}}}}); err != nil {
			t.Fatalf("write req relation=%s: %v", relation, err)
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read relation=%s first frame: %v", relation, err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode relation=%s first frame: %v", relation, err)
		}
		event, ok := frame[2].(map[string]any)
		if !ok {
			t.Fatalf("relation=%s first payload not object: %s", relation, string(raw))
		}
		contentRaw, _ := event["content"].(string)
		var content map[string]map[string]any
		if err := json.Unmarshal([]byte(contentRaw), &content); err != nil {
			t.Fatalf("decode relation=%s content: %v", relation, err)
		}
		if len(content) != len(expectedPeers) {
			t.Fatalf("unexpected peer count for relation=%s got=%d want=%d payload=%#v", relation, len(content), len(expectedPeers), content)
		}
		for _, peer := range expectedPeers {
			if _, ok := content[peer]; !ok {
				t.Fatalf("missing peer %s for relation=%s payload=%#v", peer, relation, content)
			}
		}
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("read relation=%s tail frame: %v", relation, err)
			}
			var tail []any
			if err := json.Unmarshal(raw, &tail); err != nil {
				t.Fatalf("decode relation=%s tail frame: %v", relation, err)
			}
			if len(tail) > 0 && tail[0] == "EOSE" {
				break
			}
		}
	}

	checkPeerSet("sub_dm_contacts_follows", "follows", []string{followedPeer})
	checkPeerSet("sub_dm_contacts_other", "other", []string{otherPeer})
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

func TestWSGateway_DMResetRequiresSignedAuthEvent(t *testing.T) {
	const peerPubkey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
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

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_reset_bad", map[string]any{
		"cache": []any{"reset_directmsg_count", map[string]any{"peer_pubkey": peerPubkey}},
	}}); err != nil {
		t.Fatalf("write bad reset req: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read bad reset response: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode bad reset response: %v", err)
	}
	if len(frame) < 3 || frame[0] != "NOTICE" {
		t.Fatalf("expected NOTICE frame, got %s", string(raw))
	}
}

func TestWSGateway_DMResetRejectsFutureEventAndEmitsNoEventOnSuccess(t *testing.T) {
	const peerPubkey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := NewWSGateway(fakeEventReader{
		resetDMCountFn: func(_ context.Context, receiver string, sender string) error {
			return nil
		},
		resetDMUnreadFn: func(_ context.Context, pubkey string, peer string) error {
			return nil
		},
		resetDMCountsFn: func(_ context.Context, receiver string) error {
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

	futureAuthEvent := buildSignedAuthEventAt(t, time.Now().Add(301*time.Second).Unix())
	if err := conn.WriteJSON([]any{"REQ", "sub_dm_reset_future", map[string]any{
		"cache": []any{"reset_directmsg_count", map[string]any{"peer_pubkey": peerPubkey, "event_from_user": futureAuthEvent}},
	}}); err != nil {
		t.Fatalf("write future reset req: %v", err)
	}
	_, futureRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read future reset response: %v", err)
	}
	var futureFrame []any
	if err := json.Unmarshal(futureRaw, &futureFrame); err != nil {
		t.Fatalf("decode future reset response: %v", err)
	}
	if len(futureFrame) < 3 || futureFrame[0] != "NOTICE" || futureFrame[2] != "event from the future" {
		t.Fatalf("unexpected future reset frame: %s", string(futureRaw))
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read future reset eose: %v", err)
	}

	okAuthEvent := buildSignedAuthEvent(t)
	if err := conn.WriteJSON([]any{"REQ", "sub_dm_reset_ok", map[string]any{
		"cache": []any{"reset_directmsg_count", map[string]any{"peer_pubkey": peerPubkey, "event_from_user": okAuthEvent}},
	}}); err != nil {
		t.Fatalf("write successful reset req: %v", err)
	}
	_, successRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read successful reset response: %v", err)
	}
	var successFrame []any
	if err := json.Unmarshal(successRaw, &successFrame); err != nil {
		t.Fatalf("decode successful reset response: %v", err)
	}
	if len(successFrame) < 2 || successFrame[0] != "EOSE" || successFrame[1] != "sub_dm_reset_ok" {
		t.Fatalf("successful reset must emit only EOSE: %s", string(successRaw))
	}

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_reset_all_ok", map[string]any{
		"cache": []any{"reset_directmsg_counts", map[string]any{"event_from_user": okAuthEvent}},
	}}); err != nil {
		t.Fatalf("write successful reset-all req: %v", err)
	}
	_, resetAllRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read successful reset-all response: %v", err)
	}
	var resetAllFrame []any
	if err := json.Unmarshal(resetAllRaw, &resetAllFrame); err != nil {
		t.Fatalf("decode successful reset-all response: %v", err)
	}
	if len(resetAllFrame) < 2 || resetAllFrame[0] != "EOSE" || resetAllFrame[1] != "sub_dm_reset_all_ok" {
		t.Fatalf("successful reset-all must emit only EOSE: %s", string(resetAllRaw))
	}
}

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

func TestWSGateway_OriginPolicy(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{}, WSGatewayOptions{
		AllowedOrigins: []string{"https://allowed.example"},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"

	header := http.Header{}
	header.Set("Origin", "https://blocked.example")
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err == nil {
		t.Fatal("expected websocket dial to fail for blocked origin")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("unexpected status code: got=%d want=%d", status, http.StatusForbidden)
	}

	header = http.Header{}
	header.Set("Origin", "https://allowed.example")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("expected allowed origin dial success: %v", err)
	}
	_ = conn.Close()
}

func TestPrimalBatchUserInfosEndpoint(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				pubkeys[0]: {
					Pubkey:            pubkeys[0],
					MetadataEventID:   "evt_meta",
					MetadataCreatedAt: 1700000001,
					ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
				},
			}, nil
		},
	}, 10)
	req := httptest.NewRequest(http.MethodPost, "/primal/v1/user_infos", strings.NewReader(`{"pubkeys":["pk1","pk2"]}`))
	rec := httptest.NewRecorder()
	handlers.BatchGetUserInfos(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Profiles       []any    `json:"profiles"`
		MissingPubkeys []string `json:"missing_pubkeys"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Profiles) != 1 || len(resp.MissingPubkeys) != 1 || resp.MissingPubkeys[0] != "pk2" {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func buildSignedAuthEvent(t *testing.T) map[string]any {
	return buildSignedAuthEventAt(t, time.Now().Unix())
}

func buildSignedAuthEventAt(t *testing.T, createdAt int64) map[string]any {
	t.Helper()
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatalf("new private key: %v", err)
	}
	pubkey := hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))
	kind := 27235
	tags := [][]string{{"t", "dm_reset"}}
	content := "reset dm counters"
	canonical, err := json.Marshal([]any{0, pubkey, createdAt, kind, tags, content})
	if err != nil {
		t.Fatalf("marshal canonical auth event: %v", err)
	}
	sum := sha256.Sum256(canonical)
	id := hex.EncodeToString(sum[:])
	sig, err := schnorr.Sign(priv, sum[:])
	if err != nil {
		t.Fatalf("sign auth event: %v", err)
	}
	return map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       kind,
		"tags":       tags,
		"content":    content,
		"sig":        hex.EncodeToString(sig.Serialize()),
	}
}
