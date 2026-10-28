package api_primal

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/btcsuite/btcd/btcutil/bech32"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestEnrichFeedEventsIncludesQuotedNotes(t *testing.T) {
	quotedID := strings.Repeat("ab", 32)
	quotedAuthor := strings.Repeat("cd", 32)
	quoter := strings.Repeat("11", 32)
	metaID := strings.Repeat("ef", 32)
	quotedRaw := mustRawEvent(t, map[string]any{
		"id":         quotedID,
		"pubkey":     quotedAuthor,
		"created_at": 9,
		"kind":       1,
		"tags":       []any{},
		"content":    "original note",
		"sig":        "sig",
	})
	metaRaw := mustRawEvent(t, map[string]any{
		"id":         metaID,
		"pubkey":     quotedAuthor,
		"created_at": 1,
		"kind":       0,
		"tags":       []any{},
		"content":    `{"name":"quoted"}`,
		"sig":        "sig",
	})
	note1 := mustEncodeBech32(t, "note", mustHex(t, quotedID))
	nevent := mustEncodeBech32(t, "nevent", append([]byte{0, 32}, mustHex(t, quotedID)...))

	quotes := []struct {
		name string
		note map[string]any
	}{
		{
			name: "q tag",
			note: map[string]any{
				"id": quoter[:8] + "q", "pubkey": quoter, "created_at": 20, "kind": 1, "sig": "sig",
				"tags":    []any{[]any{"q", quotedID, "wss://relay.example", quotedAuthor}},
				"content": "quoting",
			},
		},
		{
			name: "e mention",
			note: map[string]any{
				"id": quoter[:8] + "e", "pubkey": quoter, "created_at": 20, "kind": 1, "sig": "sig",
				"tags":    []any{[]any{"e", quotedID, "", "mention"}},
				"content": "quoting",
			},
		},
		{
			name: "note1 in content",
			note: map[string]any{
				"id": quoter[:8] + "n", "pubkey": quoter, "created_at": 20, "kind": 1, "sig": "sig",
				"tags":    []any{},
				"content": "see nostr:" + note1,
			},
		},
		{
			name: "nevent in content",
			note: map[string]any{
				"id": quoter[:8] + "v", "pubkey": quoter, "created_at": 20, "kind": 1, "sig": "sig",
				"tags":    []any{},
				"content": "see nostr:" + nevent,
			},
		},
	}

	for _, quote := range quotes {
		t.Run(quote.name, func(t *testing.T) {
			events := enrichWithQuotedNote(t, mustRawEvent(t, quote.note), quotedID, quotedAuthor, metaID, quotedRaw, metaRaw)
			mention := findMention(t, events, quotedID)
			if !strings.Contains(mention, "original note") {
				t.Fatalf("mention content %s", mention)
			}
			if pubkey := findKind0Pubkey(t, events, quotedAuthor); pubkey != quotedAuthor {
				t.Fatalf("quoted author profile %q", pubkey)
			}
		})
	}
}

func TestEnrichFeedEventsKeepsQuoteAheadOfReplyParents(t *testing.T) {
	quotedID := strings.Repeat("ab", 32)
	quotedAuthor := strings.Repeat("cd", 32)
	quoter := strings.Repeat("11", 32)
	quotedRaw := mustRawEvent(t, map[string]any{
		"id": quotedID, "pubkey": quotedAuthor, "created_at": 9, "kind": 1,
		"tags": []any{}, "content": "original note", "sig": "sig",
	})
	posts := make([]json.RawMessage, 0, 70)
	for i := 0; i < 70; i++ {
		posts = append(posts, mustRawEvent(t, map[string]any{
			"id": "reply-" + strings.Repeat("0", 3) + string(rune('a'+i%26)), "pubkey": quoter, "created_at": 20 + i, "kind": 1, "sig": "sig",
			"tags":    []any{[]any{"e", "parent-" + strings.Repeat("p", 4) + string(rune('a'+i%26)), "", "reply"}},
			"content": "reply",
		}))
	}
	posts = append(posts, mustRawEvent(t, map[string]any{
		"id": "quote-last", "pubkey": quoter, "created_at": 100, "kind": 1, "sig": "sig",
		"tags":    []any{[]any{"q", quotedID, "", quotedAuthor}},
		"content": "quoting",
	}))
	events := enrichWithQuotedNote(t, nil, quotedID, quotedAuthor, "", quotedRaw, nil, posts...)
	findMention(t, events, quotedID)
}

func TestEnrichFeedEventsSynthesizesQuotedAuthorProfile(t *testing.T) {
	quotedID := strings.Repeat("ab", 32)
	quotedAuthor := strings.Repeat("cd", 32)
	quoter := strings.Repeat("11", 32)
	quotedRaw := mustRawEvent(t, map[string]any{
		"id": quotedID, "pubkey": quotedAuthor, "created_at": 9, "kind": 1,
		"tags": []any{}, "content": "original note", "sig": "sig",
	})
	note := mustRawEvent(t, map[string]any{
		"id": "quote", "pubkey": quoter, "created_at": 20, "kind": 1, "sig": "sig",
		"tags":    []any{[]any{"q", quotedID, "", quotedAuthor}},
		"content": "quoting",
	})
	gateway := mustNewWSGateway(t, fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			out := map[string]json.RawMessage{}
			for _, id := range ids {
				if id == quotedID {
					out[id] = quotedRaw
				}
			}
			return out, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			out := map[string]store.ProfileProjection{}
			for _, pubkey := range pubkeys {
				if pubkey == quotedAuthor {
					out[pubkey] = store.ProfileProjection{
						Pubkey:      pubkey,
						ProfileJSON: json.RawMessage(`{"name":"quoted"}`),
					}
				}
			}
			return out, nil
		},
	}, WSGatewayOptions{Dialect: WSDialectPrimalV1})
	events := gateway.enrichFeedEvents(context.Background(), "", []json.RawMessage{note}, "created_at", nil, nil)
	findMention(t, events, quotedID)
	payload := findKind0(t, events, quotedAuthor)
	if !strings.Contains(stringValue(payload["content"]), `"name":"quoted"`) {
		t.Fatalf("profile content %#v", payload["content"])
	}
}

func enrichWithQuotedNote(t *testing.T, note json.RawMessage, quotedID, quotedAuthor, metaID string, quotedRaw, metaRaw json.RawMessage, extra ...json.RawMessage) []any {
	t.Helper()
	posts := extra
	if len(note) > 0 {
		posts = append([]json.RawMessage{note}, extra...)
	}
	gateway := mustNewWSGateway(t, fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			out := map[string]json.RawMessage{}
			for _, id := range ids {
				switch id {
				case quotedID:
					out[id] = quotedRaw
				case metaID:
					if len(metaRaw) > 0 {
						out[id] = metaRaw
					}
				default:
					out[id] = json.RawMessage(`{"id":"` + id + `","pubkey":"` + quotedAuthor + `","created_at":1,"kind":1,"tags":[],"content":"parent","sig":"sig"}`)
				}
			}
			return out, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			out := map[string]store.ProfileProjection{}
			for _, pubkey := range pubkeys {
				if pubkey == quotedAuthor && metaID != "" {
					out[pubkey] = store.ProfileProjection{
						Pubkey:          pubkey,
						MetadataEventID: metaID,
						ProfileJSON:     json.RawMessage(`{"name":"quoted"}`),
					}
				}
			}
			return out, nil
		},
	}, WSGatewayOptions{Dialect: WSDialectPrimalV1})
	return gateway.enrichFeedEvents(context.Background(), "", posts, "created_at", nil, nil)
}

func findMention(t *testing.T, events []any, eventID string) string {
	t.Helper()
	for _, event := range events {
		kind, content, _ := decodeFeedEvent(t, event)
		if kind != primalKindMentions {
			continue
		}
		var mentioned map[string]any
		if err := json.Unmarshal([]byte(content), &mentioned); err != nil {
			t.Fatalf("mention content: %v", err)
		}
		if mentioned["id"] == eventID {
			return content
		}
	}
	t.Fatalf("missing mention for %s", eventID)
	return ""
}

func findKind0Pubkey(t *testing.T, events []any, pubkey string) string {
	t.Helper()
	payload := findKind0(t, events, pubkey)
	got, _ := payload["pubkey"].(string)
	return got
}

func findKind0(t *testing.T, events []any, pubkey string) map[string]any {
	t.Helper()
	for _, event := range events {
		kind, _, payload := decodeFeedEvent(t, event)
		if kind != 0 {
			continue
		}
		if payload["pubkey"] == pubkey {
			return payload
		}
	}
	t.Fatalf("missing kind 0 for %s", pubkey)
	return nil
}

func decodeFeedEvent(t *testing.T, event any) (int, string, map[string]any) {
	t.Helper()
	switch typed := event.(type) {
	case map[string]any:
		kind, _ := kindOf(typed["kind"])
		return kind, stringValue(typed["content"]), typed
	case json.RawMessage:
		var payload map[string]any
		if err := json.Unmarshal(typed, &payload); err != nil {
			t.Fatal(err)
		}
		kind, _ := kindOf(payload["kind"])
		return kind, stringValue(payload["content"]), payload
	default:
		t.Fatalf("event %T", event)
		return 0, "", nil
	}
}

func mustRawEvent(t *testing.T, event map[string]any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mustHex(t *testing.T, value string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mustEncodeBech32(t *testing.T, hrp string, data []byte) string {
	t.Helper()
	words, err := bech32.ConvertBits(data, 8, 5, true)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := bech32.Encode(hrp, words)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
