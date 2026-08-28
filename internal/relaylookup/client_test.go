package relaylookup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/gorilla/websocket"
)

func TestValidateFetchedEvent_AcceptsValidFixture(t *testing.T) {
	raw := readNostrFixture(t, "valid/basic_text_note.json")
	evt, validatedRaw, err := validateFetchedEvent(raw)
	if err != nil {
		t.Fatalf("validate fetched event: %v", err)
	}
	if evt == nil || evt.ID == "" {
		t.Fatalf("expected validated event with id")
	}
	if len(validatedRaw) == 0 {
		t.Fatalf("expected retained validated raw json")
	}
}

func TestFilterRequestedValidatedEvents_RejectsInvalidAndMismatchedIDs(t *testing.T) {
	valid := readNostrFixture(t, "valid/basic_text_note.json")
	invalid := readNostrFixture(t, "invalid/bad_signature.json")

	var envelope struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(valid, &envelope); err != nil {
		t.Fatalf("decode valid fixture id: %v", err)
	}
	requested := map[string]struct{}{envelope.ID: {}}
	got := filterRequestedValidatedEvents([]json.RawMessage{invalid, valid}, requested, nil)
	if len(got) != 1 {
		t.Fatalf("expected exactly one validated requested event, got %d", len(got))
	}
	if _, ok := got[envelope.ID]; !ok {
		t.Fatalf("expected requested id %s in filtered output", envelope.ID)
	}

	notRequested := map[string]struct{}{"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff": {}}
	got = filterRequestedValidatedEvents([]json.RawMessage{valid}, notRequested, nil)
	if len(got) != 0 {
		t.Fatalf("expected non-requested event to be rejected")
	}
}

func TestQueryRelay_CollectsEventsUntilEOSE(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Fatalf("read req frame: %v", err)
		}
		if err := conn.WriteJSON([]any{"EVENT", "relay-sub", map[string]any{"id": "evt_1", "kind": 1}}); err != nil {
			t.Fatalf("write event frame: %v", err)
		}
		if err := conn.WriteJSON([]any{"EOSE", "relay-sub"}); err != nil {
			t.Fatalf("write eose frame: %v", err)
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	got, err := queryRelay(context.Background(), wsURL, map[string]any{"ids": []string{"evt_1"}}, 10)
	if err != nil {
		t.Fatalf("query relay: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one event from relay, got %d", len(got))
	}
}

func TestCollectFromRelays_AllowsPartialRelaySuccess(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Fatalf("read req frame: %v", err)
		}
		_ = conn.WriteJSON([]any{"EVENT", "relay-sub", map[string]any{"id": "evt_1", "kind": 1}})
		_ = conn.WriteJSON([]any{"EOSE", "relay-sub"})
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient([]string{wsURL, "ws://127.0.0.1:1"}, 500*time.Millisecond, 2)
	got, err := client.collectFromRelays(context.Background(), client.EventRelays(), map[string]any{"ids": []string{"evt_1"}}, 5)
	if err != nil {
		t.Fatalf("collect from relays should succeed with one healthy relay: %v", err)
	}
	if len(got) == 0 {
		t.Fatalf("expected events from healthy relay")
	}
}

func TestCollectFromRelays_AllRelayFailuresReturnError(t *testing.T) {
	client := NewClient([]string{"ws://127.0.0.1:1"}, 200*time.Millisecond, 1)
	_, err := client.collectFromRelays(context.Background(), client.EventRelays(), map[string]any{"ids": []string{"evt_1"}}, 1)
	if err == nil {
		t.Fatalf("expected all-relay-failure error")
	}
}

func TestSplitClient_UsesSeparateRelayLists(t *testing.T) {
	eventHits := 0
	profileHits := 0
	eventUpgrader := websocket.Upgrader{}
	profileUpgrader := websocket.Upgrader{}
	eventServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		eventHits++
		conn, err := eventUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade event relay: %v", err)
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteJSON([]any{"EOSE", "relay-sub"})
	}))
	defer eventServer.Close()
	profileServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		profileHits++
		conn, err := profileUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade profile relay: %v", err)
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteJSON([]any{"EOSE", "relay-sub"})
	}))
	defer profileServer.Close()

	client := NewSplitClient(Config{
		EventURLs:   []string{wsURLForServer(eventServer)},
		ProfileURLs: []string{wsURLForServer(profileServer)},
		Timeout:     time.Second,
		MaxFanout:   1,
	})
	if _, err := client.FetchEventsByIDs(context.Background(), []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}); err != nil {
		t.Fatalf("event fallback: %v", err)
	}
	if _, err := client.FetchProfilesByPubkeys(context.Background(), []string{
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}); err != nil {
		t.Fatalf("profile fallback: %v", err)
	}
	if eventHits != 1 || profileHits != 1 {
		t.Fatalf("expected one hit per list, got event=%d profile=%d", eventHits, profileHits)
	}
}

func TestNewSplitClient_DropsDirectoryRelaysFromEventList(t *testing.T) {
	client := NewSplitClient(Config{
		EventURLs:   []string{"wss://purplepag.es", "wss://nos.lol"},
		ProfileURLs: []string{"wss://purplepag.es"},
		Timeout:     time.Second,
		MaxFanout:   3,
	})
	if got := client.EventRelays(); !reflect.DeepEqual(got, []string{"wss://nos.lol"}) {
		t.Fatalf("event relays: got %#v", got)
	}
	if got := client.ProfileRelays(); !reflect.DeepEqual(got, []string{"wss://purplepag.es"}) {
		t.Fatalf("profile relays: got %#v", got)
	}
}

func TestFetchProfilesByPubkeys_PicksNewestAcrossRelays(t *testing.T) {
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatalf("new private key: %v", err)
	}
	pubkey := hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))
	oldEvt := buildSignedKind0Event(t, priv, 1700000000, `{"name":"old"}`)
	newEvt := buildSignedKind0Event(t, priv, 1700000010, `{"name":"new"}`)

	oldRelay := newProfileRelayServer(t, []map[string]any{oldEvt})
	defer oldRelay.Close()
	newRelay := newProfileRelayServer(t, []map[string]any{newEvt})
	defer newRelay.Close()

	client := NewClient([]string{wsURLForServer(oldRelay), wsURLForServer(newRelay)}, time.Second, 2)
	got, err := client.FetchProfilesByPubkeys(context.Background(), []string{pubkey})
	if err != nil {
		t.Fatalf("fetch profiles: %v", err)
	}
	winner, ok := got[pubkey]
	if !ok {
		t.Fatalf("expected profile winner for requested pubkey")
	}
	if winner.MetadataCreatedAt != 1700000010 {
		t.Fatalf("expected newest profile by created_at, got %d", winner.MetadataCreatedAt)
	}
	if winner.MetadataEventID != newEvt["id"].(string) {
		t.Fatalf("expected newer event id to win")
	}
}

func TestFetchProfilesByPubkeys_TieBreaksByEventID(t *testing.T) {
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatalf("new private key: %v", err)
	}
	pubkey := hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))
	createdAt := int64(1700000020)
	a := buildSignedKind0Event(t, priv, createdAt, `{"name":"alpha"}`)
	b := buildSignedKind0Event(t, priv, createdAt, `{"name":"beta"}`)

	relayA := newProfileRelayServer(t, []map[string]any{a})
	defer relayA.Close()
	relayB := newProfileRelayServer(t, []map[string]any{b})
	defer relayB.Close()

	client := NewClient([]string{wsURLForServer(relayA), wsURLForServer(relayB)}, time.Second, 2)
	got, err := client.FetchProfilesByPubkeys(context.Background(), []string{pubkey})
	if err != nil {
		t.Fatalf("fetch profiles: %v", err)
	}
	winner, ok := got[pubkey]
	if !ok {
		t.Fatalf("expected winner for requested pubkey")
	}
	expectedWinnerID := maxString(a["id"].(string), b["id"].(string))
	if winner.MetadataCreatedAt != createdAt {
		t.Fatalf("expected tied created_at to be preserved, got %d", winner.MetadataCreatedAt)
	}
	if winner.MetadataEventID != expectedWinnerID {
		t.Fatalf("expected lexical larger event id to win tie-break; got %s want %s", winner.MetadataEventID, expectedWinnerID)
	}
}

func TestFetchProfilesByPubkeys_SkipsInvalidWrongKindAndWrongPubkey(t *testing.T) {
	targetPriv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatalf("new target private key: %v", err)
	}
	targetPubkey := hex.EncodeToString(schnorr.SerializePubKey(targetPriv.PubKey()))
	otherPriv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatalf("new other private key: %v", err)
	}
	validTarget := buildSignedKind0Event(t, targetPriv, 1700000030, `{"name":"target"}`)
	wrongKind := buildSignedEvent(t, targetPriv, 1700000040, 1, `hello`)
	wrongPubkey := buildSignedKind0Event(t, otherPriv, 1700000050, `{"name":"other"}`)
	invalidSig := mustDecodeJSONMap(t, readNostrFixture(t, "invalid/bad_signature.json"))

	relay := newProfileRelayServer(t, []map[string]any{wrongKind, wrongPubkey, invalidSig, validTarget})
	defer relay.Close()

	client := NewClient([]string{wsURLForServer(relay)}, time.Second, 1)
	got, err := client.FetchProfilesByPubkeys(context.Background(), []string{
		targetPubkey,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	})
	if err != nil {
		t.Fatalf("fetch profiles: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected only one requested valid profile winner, got %d", len(got))
	}
	winner := got[targetPubkey]
	if winner.MetadataEventID != validTarget["id"].(string) {
		t.Fatalf("expected valid target event to win")
	}
}

func readNostrFixture(t *testing.T, relPath string) json.RawMessage {
	t.Helper()
	fullPath := filepath.Join("..", "nostr", "testdata", relPath)
	payload, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("read fixture %s: %v", relPath, err)
	}
	return json.RawMessage(payload)
}

func newProfileRelayServer(t *testing.T, events []map[string]any) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer conn.Close()

		if _, _, err := conn.ReadMessage(); err != nil {
			t.Fatalf("read req frame: %v", err)
		}
		for _, evt := range events {
			if err := conn.WriteJSON([]any{"EVENT", "relay-sub", evt}); err != nil {
				t.Fatalf("write event frame: %v", err)
			}
		}
		if err := conn.WriteJSON([]any{"EOSE", "relay-sub"}); err != nil {
			t.Fatalf("write eose frame: %v", err)
		}
	}))
}

func wsURLForServer(server *httptest.Server) string {
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func buildSignedKind0Event(t *testing.T, priv *btcec.PrivateKey, createdAt int64, content string) map[string]any {
	t.Helper()
	return buildSignedEvent(t, priv, createdAt, 0, content)
}

func buildSignedEvent(t *testing.T, priv *btcec.PrivateKey, createdAt int64, kind int, content string) map[string]any {
	t.Helper()
	pubkey := hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))
	tags := [][]string{}
	canonical, err := json.Marshal([]any{0, pubkey, createdAt, kind, tags, content})
	if err != nil {
		t.Fatalf("marshal canonical event: %v", err)
	}
	sum := sha256.Sum256(canonical)
	id := hex.EncodeToString(sum[:])
	sig, err := schnorr.Sign(priv, sum[:])
	if err != nil {
		t.Fatalf("sign event: %v", err)
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

func mustDecodeJSONMap(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode json fixture: %v", err)
	}
	return out
}

func maxString(a, b string) string {
	if a >= b {
		return a
	}
	return b
}

func TestCollectFromRelays_DeadRelayEntersDialCooldown(t *testing.T) {
	client := NewClient([]string{"ws://127.0.0.1:1"}, 200*time.Millisecond, 1)
	_, err := client.collectFromRelays(context.Background(), client.EventRelays(), map[string]any{"ids": []string{"evt_1"}}, 1)
	if err == nil {
		t.Fatalf("expected dial failure")
	}
	client.cooldownMu.Lock()
	until, ok := client.dialCooldownUntil["ws://127.0.0.1:1"]
	client.cooldownMu.Unlock()
	if !ok || !until.After(time.Now()) {
		t.Fatalf("expected dead relay to be in dial cooldown, got %v (ok=%v)", until, ok)
	}
}

func TestWithoutCooledDownRelays_SkipsAndRecovers(t *testing.T) {
	client := NewClient([]string{"ws://dead.example", "ws://alive.example"}, time.Second, 2)
	base := time.Now()
	client.now = func() time.Time { return base }
	client.markDialFailure("ws://dead.example")

	got := client.withoutCooledDownRelays([]string{"ws://dead.example", "ws://alive.example"})
	if len(got) != 1 || got[0] != "ws://alive.example" {
		t.Fatalf("expected cooled-down relay skipped, got %v", got)
	}

	// All candidates cooling down: fall back to the original list.
	client.markDialFailure("ws://alive.example")
	got = client.withoutCooledDownRelays([]string{"ws://dead.example", "ws://alive.example"})
	if len(got) != 2 {
		t.Fatalf("expected original list when all relays cooled down, got %v", got)
	}

	// Cooldown expiry re-admits the relay.
	client.now = func() time.Time { return base.Add(dialFailureCooldown + time.Second) }
	got = client.withoutCooledDownRelays([]string{"ws://dead.example", "ws://alive.example"})
	if len(got) != 2 {
		t.Fatalf("expected relays re-admitted after cooldown, got %v", got)
	}

	// A successful dial clears the cooldown immediately.
	client.now = func() time.Time { return base }
	client.markDialFailure("ws://dead.example")
	client.markDialSuccess("ws://dead.example")
	got = client.withoutCooledDownRelays([]string{"ws://dead.example"})
	if len(got) != 1 {
		t.Fatalf("expected relay re-admitted after successful dial, got %v", got)
	}
}
