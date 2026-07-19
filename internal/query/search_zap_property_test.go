package query

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/readmodel"
	"pgregory.net/rapid"
)

// TestSplitSearchDocumentsPartitionProperty asserts splitSearchDocuments is a
// sound, complete, deduplicating partition of the recognized entity rows: every
// eligible unique entity lands in exactly its typed bucket, nothing is invented,
// and no bucket contains duplicates.
func TestSplitSearchDocumentsPartitionProperty(t *testing.T) {
	entityTypeGen := rapid.SampledFrom([]string{"hashtag", "relay", "identity", "note", "profile", ""})
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 40).Draw(t, "n")
		rows := make([]SearchDocument, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, readmodel.SearchDocument{
				EntityType: entityTypeGen.Draw(t, "type"),
				EntityID:   rapid.String().Draw(t, "id"),
				Popularity: float64(rapid.IntRange(0, 1_000_000).Draw(t, "pop")),
			})
		}
		limit := rapid.IntRange(0, 40).Draw(t, "limit")

		hashtags, relays, identities := splitSearchDocuments(rows, limit)

		// Expected buckets, replicating the trim/dedup contract.
		expHashtags := orderedUnique(rows, "hashtag", func(id string) string {
			return strings.TrimSpace(strings.TrimPrefix(id, "#"))
		})
		expRelays := orderedUnique(rows, "relay", strings.TrimSpace)
		expIdentities := orderedUnique(rows, "identity", strings.TrimSpace)

		if len(hashtags) != len(expHashtags) {
			t.Fatalf("hashtag count: got %d want %d", len(hashtags), len(expHashtags))
		}
		for i, h := range hashtags {
			if h.Hashtag != expHashtags[i] {
				t.Fatalf("hashtag[%d]: got %q want %q", i, h.Hashtag, expHashtags[i])
			}
		}
		assertEqualStrings(t, "relays", relays, expRelays)
		assertEqualStrings(t, "identities", identities, expIdentities)

		// Soundness: total output never exceeds the input row count.
		if total := len(hashtags) + len(relays) + len(identities); total > len(rows) {
			t.Fatalf("partition produced more entries (%d) than input rows (%d)", total, len(rows))
		}
	})
}

func orderedUnique(rows []SearchDocument, entityType string, norm func(string) string) []string {
	seen := make(map[string]struct{}, len(rows))
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if r.EntityType != entityType {
			continue
		}
		v := norm(r.EntityID)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func assertEqualStrings(t *rapid.T, label string, got, want []string) {
	if len(got) != len(want) {
		t.Fatalf("%s count: got %d want %d", label, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d]: got %q want %q", label, i, got[i], want[i])
		}
	}
}

// TestParseZapAmountMillisatsTotalProperty asserts the tag amount parser is a
// total function: it never panics, is never negative, and applies the
// sats->msats scaling rule deterministically.
func TestParseZapAmountMillisatsTotalProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		raw := rapid.String().Draw(t, "raw")
		got := parseZapAmountMillisatsFromTag(raw)
		if got < 0 {
			t.Fatalf("negative msats %d for %q", got, raw)
		}
	})

	// Numeric inputs follow the documented scaling rule.
	rapid.Check(t, func(t *rapid.T) {
		amount := rapid.Int64Range(1, 1_000_000_000).Draw(t, "amount")
		raw := itoa(amount)
		got := parseZapAmountMillisatsFromTag(raw)
		want := amount
		if amount < 1000 {
			want = amount * 1000
		}
		if got != want {
			t.Fatalf("scaling rule: parse(%q)=%d want %d", raw, got, want)
		}
	})
}

// TestExtractZapRequestDetailsTotalProperty asserts extraction over arbitrary
// bytes is total (no panic) and never yields negative msats.
func TestExtractZapRequestDetailsTotalProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		raw := rapid.SliceOfN(rapid.Byte(), 0, 128).Draw(t, "bytes")
		msats, _ := extractZapRequestDetailsFromEvent(json.RawMessage(raw))
		if msats < 0 {
			t.Fatalf("negative msats %d", msats)
		}
	})

	// Well-formed zap-receipt shapes parse deterministically.
	rapid.Check(t, func(t *rapid.T) {
		amount := rapid.Int64Range(1, 1_000_000_000).Draw(t, "amount")
		content := rapid.String().Draw(t, "content")
		desc, err := json.Marshal(struct {
			Content string     `json:"content"`
			Tags    [][]string `json:"tags"`
		}{Content: content, Tags: [][]string{{"amount", itoa(amount)}}})
		if err != nil {
			t.Fatalf("marshal description: %v", err)
		}
		event, err := json.Marshal(struct {
			Tags [][]string `json:"tags"`
		}{Tags: [][]string{{"description", string(desc)}}})
		if err != nil {
			t.Fatalf("marshal event: %v", err)
		}
		msats, zapText := extractZapRequestDetailsFromEvent(event)
		wantMsats := amount
		if amount < 1000 {
			wantMsats = amount * 1000
		}
		if msats != wantMsats {
			t.Fatalf("msats: got %d want %d", msats, wantMsats)
		}
		if zapText != strings.TrimSpace(content) {
			t.Fatalf("zap text: got %q want %q", zapText, strings.TrimSpace(content))
		}
	})
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}
