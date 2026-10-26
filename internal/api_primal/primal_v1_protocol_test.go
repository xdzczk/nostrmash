package api_primal

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestPrimalV1EnrichPageMatchesFixture(t *testing.T) {
	gateway := mustNewWSGateway(t, fakeEventReader{}, WSGatewayOptions{Dialect: WSDialectPrimalV1})
	note := json.RawMessage(`{"id":"evt1","pubkey":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","created_at":10,"kind":1,"tags":[],"content":"hi","sig":""}`)
	events := gateway.enrichFeedEvents(context.Background(), "", []json.RawMessage{note}, "created_at", nil, nil)
	kinds := make([]int, 0, len(events))
	var elements []any
	for i, event := range events {
		if i == 0 {
			kinds = append(kinds, 1)
			continue
		}
		item, ok := event.(map[string]any)
		if !ok {
			t.Fatalf("event %d is %T", i, event)
		}
		kind, ok := kindOf(item["kind"])
		if !ok {
			t.Fatalf("event %d has no kind", i)
		}
		kinds = append(kinds, kind)
		if kind == primalKindRange {
			var payload map[string]any
			if err := json.Unmarshal([]byte(stringValue(item["content"])), &payload); err != nil {
				t.Fatal(err)
			}
			elements, _ = payload["elements"].([]any)
		}
	}
	raw, err := os.ReadFile("testdata/primal_v1/enrich_page.json")
	if err != nil {
		t.Fatal(err)
	}
	var want struct {
		Kinds    []int    `json:"kinds"`
		Elements []string `json:"elements"`
	}
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if len(kinds) != len(want.Kinds) {
		t.Fatalf("kinds %v want %v", kinds, want.Kinds)
	}
	for i := range want.Kinds {
		if kinds[i] != want.Kinds[i] {
			t.Fatalf("kinds %v want %v", kinds, want.Kinds)
		}
	}
	if len(elements) != len(want.Elements) || elements[0] != want.Elements[0] {
		t.Fatalf("elements %#v", elements)
	}
}

func TestPrimalV1ProtocolFrames(t *testing.T) {
	gateway := mustNewWSGateway(t, fakeEventReader{}, WSGatewayOptions{Dialect: WSDialectPrimalV1})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws/v1", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	conn := dialPrimal(t, server.URL, "/primal/ws/v1")
	defer conn.Close()

	writeReq(t, conn, "sub_unknown", map[string]any{"cache": []any{"not_a_real_call", map[string]any{}}})
	notice := readJSONFrame(t, conn)
	if notice[0] != "NOTICE" || notice[2] != "unknown api request" {
		t.Fatalf("notice: %#v", notice)
	}
	eose := readJSONFrame(t, conn)
	if eose[0] != "EOSE" {
		t.Fatalf("eose: %#v", eose)
	}

	writeReq(t, conn, "sub_feed", map[string]any{"cache": []any{"multi_kind_mega_feed_directive", map[string]any{
		"spec": `{"id":"latest","kind":"notes"}`,
	}}})
	if frame := readJSONFrame(t, conn); frame[0] != "NOTICE" || frame[2] != "error" {
		t.Fatalf("feed notice: %#v", frame)
	}
	if frame := readJSONFrame(t, conn); frame[0] != "EOSE" {
		t.Fatalf("feed eose: %#v", frame)
	}

	writeReq(t, conn, "sub_member", map[string]any{"cache": []any{"membership_status", map[string]any{}}})
	member := readJSONFrame(t, conn)
	if member[0] != "EVENT" {
		t.Fatalf("membership: %#v", member)
	}
	memberEvent, _ := member[2].(map[string]any)
	memberKind, _ := kindOf(memberEvent["kind"])
	if memberKind != primalKindPremiumState || !strings.Contains(stringValue(memberEvent["content"]), `"tier":"free"`) {
		t.Fatalf("membership event: %#v", memberEvent)
	}
	if frame := readJSONFrame(t, conn); frame[0] != "EOSE" {
		t.Fatalf("membership eose: %#v", frame)
	}

	converted, err := bech32.ConvertBits(bytes.Repeat([]byte{0}, 32), 8, 5, true)
	if err != nil {
		t.Fatal(err)
	}
	npub, err := bech32.Encode("npub", converted)
	if err != nil {
		t.Fatal(err)
	}
	writeReq(t, conn, "sub_nip19", map[string]any{"cache": []any{"nip19_decode", map[string]any{"value": npub}}})
	decoded := readJSONFrame(t, conn)
	decodedEvent, _ := decoded[2].(map[string]any)
	if !strings.Contains(stringValue(decodedEvent["content"]), hex.EncodeToString(bytes.Repeat([]byte{0}, 32))) {
		t.Fatalf("nip19: %#v", decodedEvent)
	}
	if frame := readJSONFrame(t, conn); frame[0] != "EOSE" {
		t.Fatalf("nip19 eose: %#v", frame)
	}

	writeReq(t, conn, "sub_proto", map[string]any{"cache": []any{"set_primal_protocol", map[string]any{"compression": "zlib"}}})
	if frame := readJSONFrame(t, conn); frame[0] != "EOSE" {
		t.Fatalf("protocol eose: %#v", frame)
	}

	writeReq(t, conn, "sub_settings", map[string]any{"cache": []any{"get_default_app_settings", map[string]any{"client": "primal-ios"}}})
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := zlib.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	inflated, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	var bulk []any
	if err := json.Unmarshal(inflated, &bulk); err != nil {
		t.Fatal(err)
	}
	if len(bulk) != 3 || bulk[0] != "EVENTS" || bulk[1] != "sub_settings" {
		t.Fatalf("bulk: %#v", bulk)
	}
	msgs, _ := bulk[2].([]any)
	if len(msgs) != 1 {
		t.Fatalf("msgs: %#v", msgs)
	}
	event, _ := msgs[0].(map[string]any)
	kind, _ := kindOf(event["kind"])
	if kind != primalKindDefaultSettings {
		t.Fatalf("settings kind %d", kind)
	}
	if frame := readJSONFrame(t, conn); frame[0] != "EOSE" {
		t.Fatalf("settings eose: %#v", frame)
	}
}

func TestPrimalV1UserProfileShape(t *testing.T) {
	const pubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := mustNewWSGateway(t, fakeEventReader{
		getProfileByPubkey: func(_ context.Context, got string) (store.ProfileProjection, error) {
			if got != pubkey {
				t.Fatalf("pubkey %s", got)
			}
			return store.ProfileProjection{
				Pubkey:            pubkey,
				MetadataEventID:   "meta1",
				MetadataCreatedAt: 5,
				ProfileJSON:       json.RawMessage(`{"name":"ada"}`),
			}, nil
		},
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			if id != "meta1" {
				t.Fatalf("metadata id %s", id)
			}
			return json.RawMessage(`{"id":"meta1","pubkey":"` + pubkey + `","created_at":5,"kind":0,"tags":[],"content":"{\"name\":\"ada\"}","sig":"ab"}`), nil
		},
	}, WSGatewayOptions{Dialect: WSDialectPrimalV1})
	events, err := gateway.primalV1UserProfile(context.Background(), map[string]any{"pubkey": pubkey})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events: %#v", events)
	}
	raw, ok := events[0].(json.RawMessage)
	if !ok || rawKind(raw) != 0 {
		t.Fatalf("kind0: %#v", events[0])
	}
	stats, _ := events[1].(map[string]any)
	kind, _ := kindOf(stats["kind"])
	if kind != primalKindUserStats || !strings.Contains(stringValue(stats["content"]), `"pubkey":"`+pubkey+`"`) {
		t.Fatalf("stats: %#v", stats)
	}
}

func dialPrimal(t *testing.T, serverURL string, path string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + path
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func writeReq(t *testing.T, conn *websocket.Conn, subID string, filter map[string]any) {
	t.Helper()
	if err := conn.WriteJSON([]any{"REQ", subID, filter}); err != nil {
		t.Fatal(err)
	}
}

func readJSONFrame(t *testing.T, conn *websocket.Conn) []any {
	t.Helper()
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return frame
}
