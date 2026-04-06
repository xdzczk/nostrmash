package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/store"
)

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

