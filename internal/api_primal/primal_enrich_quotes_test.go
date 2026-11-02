package api_primal

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/btcsuite/btcd/btcutil/bech32"

	"github.com/xdzczk/nostrmash/internal/readmodel"
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

func TestEnrichFeedEventsQuotesNoteAlsoOnThePage(t *testing.T) {
	quotedID := strings.Repeat("ab", 32)
	quotedAuthor := strings.Repeat("cd", 32)
	quoter := strings.Repeat("11", 32)
	note1 := mustEncodeBech32(t, "note", mustHex(t, quotedID))
	quoted := mustRawEvent(t, map[string]any{
		"id": quotedID, "pubkey": quotedAuthor, "created_at": 9, "kind": 1,
		"tags": []any{}, "content": "original note", "sig": "sig",
	})
	quoting := mustRawEvent(t, map[string]any{
		"id": strings.Repeat("e1", 32), "pubkey": quoter, "created_at": 20, "kind": 1, "sig": "sig",
		"tags":    []any{[]any{"q", quotedID, "", quotedAuthor}},
		"content": "see nostr:" + note1,
	})
	events := enrichFeedPage(t, []json.RawMessage{quoted, quoting}, nil, map[string]string{quotedAuthor: "quoted"})
	mention := findMention(t, events, quotedID)
	if !strings.Contains(mention, "original note") {
		t.Fatalf("mention content %s", mention)
	}
	if pubkey := findKind0Pubkey(t, events, quotedAuthor); pubkey != quotedAuthor {
		t.Fatalf("quoted author profile %q", pubkey)
	}
}

func TestEnrichFeedEventsQuotesNoteInsideRepost(t *testing.T) {
	quotedID := strings.Repeat("ab", 32)
	quotedAuthor := strings.Repeat("cd", 32)
	reposter := strings.Repeat("11", 32)
	innerAuthor := strings.Repeat("22", 32)
	innerID := strings.Repeat("e1", 32)
	quoted := mustRawEvent(t, map[string]any{
		"id": quotedID, "pubkey": quotedAuthor, "created_at": 9, "kind": 1,
		"tags": []any{}, "content": "original note", "sig": "sig",
	})
	inner := mustRawEvent(t, map[string]any{
		"id": innerID, "pubkey": innerAuthor, "created_at": 15, "kind": 1, "sig": "sig",
		"tags":    []any{[]any{"q", quotedID, "", quotedAuthor}},
		"content": "quoting",
	})
	repost := mustRawEvent(t, map[string]any{
		"id": strings.Repeat("f6", 32), "pubkey": reposter, "created_at": 20, "kind": 6, "sig": "sig",
		"tags":    []any{[]any{"e", innerID}, []any{"p", innerAuthor}},
		"content": string(inner),
	})
	events := enrichFeedPage(t, []json.RawMessage{repost}, map[string]json.RawMessage{quotedID: quoted}, map[string]string{quotedAuthor: "quoted"})
	mention := findMention(t, events, quotedID)
	if !strings.Contains(mention, "original note") {
		t.Fatalf("mention content %s", mention)
	}

	t.Run("quoted note is also a post", func(t *testing.T) {
		note1 := mustEncodeBech32(t, "note", mustHex(t, quotedID))
		inner := mustRawEvent(t, map[string]any{
			"id": innerID, "pubkey": innerAuthor, "created_at": 15, "kind": 1, "sig": "sig",
			"tags": []any{}, "content": "see nostr:" + note1,
		})
		repost := mustRawEvent(t, map[string]any{
			"id": strings.Repeat("f6", 32), "pubkey": reposter, "created_at": 20, "kind": 6, "sig": "sig",
			"tags":    []any{[]any{"e", innerID}, []any{"p", innerAuthor}},
			"content": string(inner),
		})
		got := enrichFeedPage(t, []json.RawMessage{quoted, repost}, nil, map[string]string{quotedAuthor: "quoted"})
		mention := findMention(t, got, quotedID)
		if !strings.Contains(mention, "original note") {
			t.Fatalf("mention content %s", mention)
		}
	})
}

func TestEnrichFeedEventsRepostStatsUseOriginalNote(t *testing.T) {
	originalID := strings.Repeat("ab", 32)
	originalAuthor := strings.Repeat("cd", 32)
	reposter := strings.Repeat("11", 32)
	repostID := strings.Repeat("e6", 32)
	original := mustRawEvent(t, map[string]any{
		"id": originalID, "pubkey": originalAuthor, "created_at": 9, "kind": 1,
		"tags": []any{}, "content": "hello from ada", "sig": "sig",
	})
	repost := mustRawEvent(t, map[string]any{
		"id": repostID, "pubkey": reposter, "created_at": 20, "kind": 6, "sig": "sig",
		"tags":    []any{[]any{"e", originalID}, []any{"p", originalAuthor}},
		"content": string(original),
	})
	events := enrichFeedPage(t, []json.RawMessage{repost}, nil, map[string]string{originalAuthor: "ada"})
	stats := noteStatIDs(t, events)
	if _, ok := stats[originalID]; !ok {
		t.Fatalf("stats ids %#v", stats)
	}
	if _, ok := stats[repostID]; !ok {
		t.Fatalf("repost stats ids %#v", stats)
	}
	if pubkey := findKind0Pubkey(t, events, originalAuthor); pubkey != originalAuthor {
		t.Fatalf("original author profile %q", pubkey)
	}
}

func TestEnrichFeedEventsKeepsQuoteAuthorOnAFullPage(t *testing.T) {
	quotedID := strings.Repeat("ab", 32)
	quotedAuthor := strings.Repeat("cd", 32)
	quoted := mustRawEvent(t, map[string]any{
		"id": quotedID, "pubkey": quotedAuthor, "created_at": 9, "kind": 1,
		"tags": []any{}, "content": "original note", "sig": "sig",
	})
	posts := make([]json.RawMessage, 0, maxFeedAuthors)
	for i := 0; i < maxFeedAuthors; i++ {
		note := map[string]any{
			"id": fmt.Sprintf("%064x", i+1), "pubkey": fmt.Sprintf("%064x", i+1000),
			"created_at": 20 + i, "kind": 1, "tags": []any{}, "content": "note", "sig": "sig",
		}
		if i == maxFeedAuthors-1 {
			note["tags"] = []any{[]any{"q", quotedID, "", quotedAuthor}}
			note["content"] = "quoting"
		}
		posts = append(posts, mustRawEvent(t, note))
	}
	events := enrichFeedPage(t, posts, map[string]json.RawMessage{quotedID: quoted}, map[string]string{quotedAuthor: "quoted"})
	findMention(t, events, quotedID)
	findKind0(t, events, quotedAuthor)
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

func TestEnrichFeedEventsIncludesTopZapperProfile(t *testing.T) {
	author := strings.Repeat("11", 32)
	zapper := strings.Repeat("ab", 32)
	other := strings.Repeat("22", 32)
	wallet := strings.Repeat("cd", 32)
	noteID := strings.Repeat("e1", 32)
	receiptID := strings.Repeat("f2", 32)
	otherReceiptID := strings.Repeat("f3", 32)
	picture := "https://cdn.example/zapper.png"
	profileJSON := json.RawMessage(`{"name":"ada","display_name":"Ada Lovelace","picture":"` + picture + `"}`)

	receiptWithDescription := func(id, sender string) json.RawMessage {
		t.Helper()
		return mustRawEvent(t, map[string]any{
			"id": id, "pubkey": wallet, "kind": 9735, "created_at": 30, "sig": "sig", "content": "",
			"tags": []any{
				[]any{"description", `{"id":"req","pubkey":"` + sender + `","kind":9734,"content":"nice","tags":[]}`},
				[]any{"p", author},
				[]any{"e", noteID},
				[]any{"amount", "21000000"},
			},
		})
	}
	events := enrichFeedWithZaps(t, []json.RawMessage{mustRawEvent(t, map[string]any{
		"id": noteID, "pubkey": author, "created_at": 20, "kind": 1, "tags": []any{}, "content": "hello", "sig": "sig",
	})}, map[string]json.RawMessage{
		receiptID:      receiptWithDescription(receiptID, zapper),
		otherReceiptID: receiptWithDescription(otherReceiptID, other),
	}, profileJSON, zapper, []readmodel.EventZap{
		{EventID: noteID, CreatedAt: 20, Sender: wallet, Receiver: author, AmountSats: 21, ZapReceiptID: otherReceiptID},
		{EventID: noteID, CreatedAt: 30, Sender: wallet, Receiver: author, AmountSats: 21000, ZapReceiptID: receiptID},
	})
	if sender := postZapSender(t, events, noteID, 21000); sender != zapper {
		t.Fatalf("top zap sender %s", sender)
	}
	if sender := postZapSender(t, events, noteID, 21); sender != other {
		t.Fatalf("smaller zap sender %s", sender)
	}
	content := stringValue(findKind0(t, events, zapper)["content"])
	if !strings.Contains(content, `"name":"ada"`) || !strings.Contains(content, `"display_name":"Ada Lovelace"`) || !strings.Contains(content, picture) {
		t.Fatalf("zapper profile %s", content)
	}
	if kind0Present(events, wallet) {
		t.Fatal("wallet profile was attached instead of the zapper")
	}

	t.Run("p tag", func(t *testing.T) {
		receipt := mustRawEvent(t, map[string]any{
			"id": receiptID, "pubkey": wallet, "kind": 9735, "created_at": 30, "sig": "sig", "content": "",
			"tags": []any{
				[]any{"description", `{"id":"req","kind":9734,"content":"nice","tags":[]}`},
				[]any{"P", zapper},
				[]any{"p", author},
				[]any{"e", noteID},
			},
		})
		got := enrichFeedWithZaps(t, []json.RawMessage{mustRawEvent(t, map[string]any{
			"id": noteID, "pubkey": author, "created_at": 20, "kind": 1, "tags": []any{}, "content": "hello", "sig": "sig",
		})}, map[string]json.RawMessage{receiptID: receipt}, profileJSON, zapper, []readmodel.EventZap{
			{EventID: noteID, CreatedAt: 30, Sender: wallet, Receiver: author, AmountSats: 5, ZapReceiptID: receiptID},
		})
		if sender := postZapSender(t, got, noteID, 5); sender != zapper {
			t.Fatalf("sender %s", sender)
		}
		findKind0(t, got, zapper)
	})

	t.Run("survives a full author page", func(t *testing.T) {
		posts := make([]json.RawMessage, 0, maxFeedAuthors)
		for i := 0; i < maxFeedAuthors; i++ {
			id := noteID
			pubkey := author
			if i > 0 {
				id = fmt.Sprintf("%064x", i+1)
				pubkey = fmt.Sprintf("%064x", i+1000)
			}
			posts = append(posts, mustRawEvent(t, map[string]any{
				"id": id, "pubkey": pubkey, "created_at": 20 + i, "kind": 1, "tags": []any{}, "content": "note", "sig": "sig",
			}))
		}
		got := enrichFeedWithZaps(t, posts, map[string]json.RawMessage{
			receiptID: receiptWithDescription(receiptID, zapper),
		}, profileJSON, zapper, []readmodel.EventZap{
			{EventID: noteID, CreatedAt: 30, Sender: wallet, Receiver: author, AmountSats: 21000, ZapReceiptID: receiptID},
		})
		findKind0(t, got, zapper)
		if sender := postZapSender(t, got, noteID, 21000); sender != zapper {
			t.Fatalf("sender %s", sender)
		}
	})
}

func enrichFeedWithZaps(t *testing.T, posts []json.RawMessage, receipts map[string]json.RawMessage, profileJSON json.RawMessage, zapper string, zaps []readmodel.EventZap) []any {
	t.Helper()
	gateway := mustNewWSGateway(t, fakeEventReader{
		getTopZapsByEventsFn: func(context.Context, []string, int) ([]readmodel.EventZap, error) {
			return zaps, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			out := map[string]json.RawMessage{}
			for _, id := range ids {
				if raw, ok := receipts[id]; ok {
					out[id] = raw
				}
			}
			return out, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			out := map[string]store.ProfileProjection{}
			for _, pubkey := range pubkeys {
				if pubkey == zapper {
					out[pubkey] = store.ProfileProjection{Pubkey: pubkey, ProfileJSON: profileJSON}
				}
			}
			return out, nil
		},
	}, WSGatewayOptions{Dialect: WSDialectPrimalV1})
	return gateway.enrichFeedEvents(context.Background(), "", posts, "created_at", nil, nil)
}

func postZapSender(t *testing.T, events []any, eventID string, sats int64) string {
	t.Helper()
	for _, event := range events {
		kind, content, _ := decodeFeedEvent(t, event)
		if kind != primalKindPostZaps {
			continue
		}
		var body map[string]any
		if err := json.Unmarshal([]byte(content), &body); err != nil {
			t.Fatal(err)
		}
		if body["event_id"] != eventID || toInt64(body["amount_sats"], 0) != sats {
			continue
		}
		return stringValue(body["sender"])
	}
	t.Fatalf("missing zap for %s sats %d", eventID, sats)
	return ""
}

func kind0Present(events []any, pubkey string) bool {
	for _, event := range events {
		payload, ok := event.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := kindOf(payload["kind"])
		if kind == 0 && payload["pubkey"] == pubkey {
			return true
		}
	}
	return false
}

func enrichFeedPage(t *testing.T, posts []json.RawMessage, events map[string]json.RawMessage, names map[string]string) []any {
	t.Helper()
	gateway := mustNewWSGateway(t, fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			out := map[string]json.RawMessage{}
			for _, id := range ids {
				if raw, ok := events[id]; ok {
					out[id] = raw
				}
			}
			return out, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			out := map[string]store.ProfileProjection{}
			for _, pubkey := range pubkeys {
				name, ok := names[pubkey]
				if !ok {
					continue
				}
				out[pubkey] = store.ProfileProjection{
					Pubkey:      pubkey,
					ProfileJSON: json.RawMessage(`{"name":"` + name + `"}`),
				}
			}
			return out, nil
		},
	}, WSGatewayOptions{Dialect: WSDialectPrimalV1})
	return gateway.enrichFeedEvents(context.Background(), "", posts, "created_at", nil, nil)
}

func noteStatIDs(t *testing.T, events []any) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, event := range events {
		kind, content, _ := decodeFeedEvent(t, event)
		if kind != primalKindNoteStats {
			continue
		}
		var body struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal([]byte(content), &body); err != nil {
			t.Fatal(err)
		}
		out[body.EventID] = true
	}
	return out
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
