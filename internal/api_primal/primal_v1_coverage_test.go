package api_primal

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/readmodel"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestPrimalV1HandlersExerciseStoredReads(t *testing.T) {
	pubkey := strings.Repeat("aa", 32)
	other := strings.Repeat("bb", 32)
	muted := strings.Repeat("cc", 32)
	note := json.RawMessage(`{"id":"note1","pubkey":"` + pubkey + `","kind":1,"created_at":50,"tags":[["p","` + other + `"]],"content":"hi"}`)
	repost := json.RawMessage(`{"id":"repost1","pubkey":"` + pubkey + `","kind":6,"created_at":60,"tags":[["e","note1"]],"content":""}`)
	relayList := json.RawMessage(`{"id":"relays1","pubkey":"` + pubkey + `","kind":10002,"tags":[["r","wss://relay.example","write"],["t","nope"]],"content":""}`)
	contact := json.RawMessage(`{"id":"contacts1","pubkey":"` + pubkey + `","kind":3,"tags":[["p","` + other + `"]],"content":"{\"wss://relay.example\":{\"read\":true,\"write\":true},\"wss://read.example\":{\"read\":true},\"wss://write.example\":{\"write\":true},\"wss://plain.example\":{}}"}`)
	followList := json.RawMessage(`{"id":"list1","pubkey":"` + pubkey + `","kind":39089,"tags":[["p","` + other + `"]],"content":""}`)

	gateway := mustNewWSGateway(t, fakeEventReader{
		getModerationFn: func(context.Context, string, int) ([]string, error) {
			return []string{muted, "  "}, nil
		},
		getEventRawsByIDs: func(context.Context, []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{"note1": note, "repost1": repost}, nil
		},
		getByKindPubkeyFn: func(_ context.Context, kind int, got string, _ int) ([]json.RawMessage, error) {
			if got != pubkey {
				return nil, nil
			}
			switch kind {
			case 10002:
				return []json.RawMessage{relayList}, nil
			case 3:
				return []json.RawMessage{contact}, nil
			case 1:
				return []json.RawMessage{note}, nil
			default:
				return nil, nil
			}
		},
		getParamEventFn: func(context.Context, string, int, string) (json.RawMessage, error) {
			return followList, nil
		},
		getProfilesByBatch: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{}, nil
		},
		isFollowingFn: func(context.Context, string, string) (bool, error) {
			return true, nil
		},
		getFollowersFn: func(context.Context, string, int) ([]json.RawMessage, error) {
			return []json.RawMessage{note}, nil
		},
		getRefsPubkeyFn: func(context.Context, string, int) ([]json.RawMessage, error) {
			return []json.RawMessage{note}, nil
		},
	}, WSGatewayOptions{Dialect: WSDialectPrimalV1})

	ctx := context.Background()
	kwargs := map[string]any{"pubkey": pubkey, "user_pubkey": other, "limit": 5}
	calls := []struct {
		name string
		fn   func() ([]any, error)
	}{
		{"infos", func() ([]any, error) {
			return gateway.primalV1UserInfos(ctx, map[string]any{"pubkey": pubkey, "pubkeys": []any{pubkey, "nope", other}})
		}},
		{"following", func() ([]any, error) { return gateway.primalV1IsUserFollowing(ctx, kwargs) }},
		{"contacts", func() ([]any, error) { return gateway.primalV1ContactList(ctx, kwargs) }},
		{"contacts-short", func() ([]any, error) {
			return gateway.primalV1ContactList(ctx, map[string]any{"pubkey": pubkey, "extended_response": false})
		}},
		{"relays", func() ([]any, error) { return gateway.primalV1UserRelays(ctx, kwargs) }},
		{"relays2", func() ([]any, error) {
			return gateway.primalV1UserRelays2(ctx, map[string]any{"pubkeys": []any{pubkey, "bad", other}})
		}},
		{"replaceable", func() ([]any, error) {
			return gateway.primalV1ReplaceableEvent(ctx, map[string]any{"pubkey": pubkey, "kind": 1})
		}},
		{"follow-list", func() ([]any, error) {
			return gateway.primalV1FollowList(ctx, map[string]any{"pubkey": pubkey, "identifier": "people"})
		}},
		{"followers", func() ([]any, error) { return gateway.primalV1Followers(ctx, kwargs) }},
		{"mentions", func() ([]any, error) { return gateway.primalV1Mentions(ctx, kwargs) }},
		{"events", func() ([]any, error) {
			return gateway.primalV1Events(ctx, map[string]any{"event_ids": []any{"note1", "missing"}, "extended_response": true})
		}},
		{"author-replies", func() ([]any, error) { return gateway.primalV1AuthorReplies(ctx, kwargs) }},
		{"search-empty", func() ([]any, error) { return gateway.primalV1UserSearch(ctx, map[string]any{"query": "lolicon"}) }},
		{"search-blank", func() ([]any, error) { return gateway.primalV1UserSearch(ctx, map[string]any{}) }},
	}
	for _, call := range calls {
		if _, err := call.fn(); err != nil && err.Error() != "error" {
			t.Fatalf("%s: %v", call.name, err)
		}
	}

	specs := []map[string]any{
		{"id": "video-reels"},
		{"dvm_pubkey": pubkey, "dvm_id": "1"},
		{"id": "search"},
		{"id": "feed", "notes": "other", "kind": "notes"},
		{"id": "feed", "notes": "authored", "pubkey": "bad"},
		{"id": "global-trending", "hours": 4},
		{"id": "most-zapped", "timeframe": "7d"},
		{"id": "advsearch", "query": "nostr"},
		{"id": "latest", "user_pubkey": pubkey},
		{"id": "nostr-reads-feed"},
		{"kind": "reads"},
	}
	for _, spec := range specs {
		raw, _ := json.Marshal(spec)
		_, _ = gateway.routeMegaFeed(ctx, map[string]any{"pubkey": pubkey, "user_pubkey": pubkey, "spec": string(raw), "limit": 5}, spec)
	}

	mutedKey := strings.ToLower(muted)
	score := kindObject(primalKindUserScore, map[string]any{mutedKey: 1, pubkey: 2})
	rng := kindObject(primalKindRange, map[string]any{"elements": []any{"note1", "gone", "kept"}})
	moderated := gateway.moderatePrimalEvents(ctx, map[string]any{
		"user_pubkey": pubkey,
		"notes":       "authored",
		"spec":        `{"notes":"replies","pubkey":"` + pubkey + `"}`,
	}, []any{
		note,
		map[string]any{"id": "gone", "pubkey": muted, "kind": 1},
		[]byte(`not-json`),
		score,
		rng,
	})
	if len(moderated) == 0 {
		t.Fatal("expected moderated events")
	}

	if notes, author := notesMode(map[string]any{"spec": "{"}); notes != "" || author != "" {
		t.Fatalf("bad spec notes=%q author=%q", notes, author)
	}
	if got := filterRawEvents([]json.RawMessage{note, json.RawMessage(`{"kind":7,"created_at":10}`)}, []int{1}, 40, 80); len(got) != 1 {
		t.Fatalf("filtered %#v", got)
	}
	if trendingHours(map[string]any{"timeframe": "4h"}) != 4 || trendingHours(map[string]any{"since": 12}) != 12 || trendingHours(map[string]any{}) != 24 {
		t.Fatal("trending hours")
	}
	repaired := gateway.repairReposts(ctx, []json.RawMessage{note, repost})
	if len(repaired) != 2 {
		t.Fatalf("repaired %#v", repaired)
	}
	if !repostContentReady(note) || repostTarget(note) != "" || rewriteRepostContent(json.RawMessage(`{`), note) != nil {
		t.Fatal("repost helpers")
	}
	if len(jsonRawEvents([]any{note, []byte(note), "skip"})) != 2 {
		t.Fatal("json raw events")
	}

	merged := mergeJSONObjects(json.RawMessage(`{"a":1`), json.RawMessage(`{"b":2}`))
	if !strings.Contains(string(merged), `"b"`) {
		t.Fatalf("merge %s", merged)
	}
	if keys := objectKeysInOrder(`{"z":1,"a":{"n":1}}`); len(keys) != 2 || keys[0] != "z" {
		t.Fatalf("keys %#v", keys)
	}
	if objectKeysInOrder(`[]`) != nil || len(rawEventIDs([]any{note, "nope"})) != 1 {
		t.Fatal("frame helpers")
	}
	ranged := attachRangeElements([]any{note, kindObject(primalKindRange, map[string]any{"elements": []any{}})}, []string{"note1"})
	if len(ranged) != 2 {
		t.Fatal("range attach")
	}

	if !publicHTTPS("https://cdn.example/a.png") || publicHTTPS("http://cdn.example/a.png") || publicHTTPS("https://localhost/a") || publicHTTPS("https://10.0.0.8/a") || publicHTTPS("https://8.8.8.8/a") == false {
		t.Fatal("public https")
	}
	title, desc, image := ogTags(`<html><meta property="og:title" content="Hello"><meta name="og:description" content="World"><meta property="og:image" content="https://cdn.example/a.png">`)
	if title != "Hello" || desc != "World" || image == "" {
		t.Fatalf("og %s %s %s", title, desc, image)
	}
	if metaContent("nope", "nope", "og:title") != "" {
		t.Fatal("missing meta")
	}

	body := map[string]any{"event_id": "note1", "user": other, "amount_sats": 21, "reply": "note1"}
	for _, typ := range []int{3, 4, 5, 6, 7, 8} {
		canonicalNotificationArgs(typ, body)
	}
	if len(notificationRefIDs(body)) == 0 || notificationTypes(map[string]any{"type_group": "mentions"}) == nil {
		t.Fatal("notification args")
	}
	if notificationTypes(map[string]any{"type_group": "zaps"})[0] != 3 {
		t.Fatal("zap types")
	}
	stats := notificationStats(pubkey, []readmodel.NotificationCount{{Type: 1, Cnt: 2}})
	if stats["1"] != int64(2) && stats["1"] != 2 {
		t.Fatalf("stats %#v", stats)
	}
	_ = notificationStats2(pubkey, []readmodel.NotificationCount{{Type: 1, Cnt: 2}})
	if !knownSubkey("user-home-feeds") || knownSubkey("nope") || len(defaultSubsettings("user-reads-feeds")) == 0 || len(defaultSubsettings("missing")) == 0 {
		t.Fatal("subsettings")
	}
	if subsettingsEvent("", nil)["kind"] != primalKindSubsettings {
		t.Fatal("subsettings event")
	}
	if settingsClient(&nostr.Event{Tags: [][]string{{"d", "web"}}}) != "web" || settingsClient(&nostr.Event{}) != "primal-ios" {
		t.Fatal("settings client")
	}
	if _, err := parseSignedUserEvent(map[string]any{"kind": 1}, 1); err == nil {
		t.Fatal("expected invalid signed event")
	}
	if _, err := signedSettingsEvent(map[string]any{}); err == nil {
		t.Fatal("expected missing settings event")
	}

	if len(relayTagsFromEvent(json.RawMessage(`{"tags":[["r","wss://ok"],["p","x"],["r",""]]}`))) != 1 {
		t.Fatal("relay tags")
	}
	if tags, err := gateway.relayTagsForPubkey(ctx, other); err != nil || len(tags) != 0 {
		t.Fatalf("fallback relays %#v %v", tags, err)
	}
}
