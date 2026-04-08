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

func TestWSGateway_GetDirectMsgContactsContractOrdering(t *testing.T) {
	const receiver = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const peer1 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const peer2 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	gateway := mustNewWSGateway(t, fakeEventReader{
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
	gateway := mustNewWSGateway(t, fakeEventReader{
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
