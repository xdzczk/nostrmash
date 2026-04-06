package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/store"
)

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

