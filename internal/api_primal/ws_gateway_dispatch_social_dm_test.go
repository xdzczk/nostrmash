package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

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
