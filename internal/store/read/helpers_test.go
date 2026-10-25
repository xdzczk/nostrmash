package read

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestNormalizeLanguageFilter(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"  UND ", "und"},
		{"UND-UNCERTAIN", "und-uncertain"},
		{"en", "en"},
		{"EN-US", invalidLanguageFilter}, // hyphen rejected
		{"toolonglang", invalidLanguageFilter},
		{"x", invalidLanguageFilter},
		{"e1", invalidLanguageFilter},
	}
	for _, tc := range cases {
		if got := normalizeLanguageFilter(tc.in); got != tc.want {
			t.Fatalf("normalizeLanguageFilter(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := nullableLanguageFilter(""); got != nil {
		t.Fatalf("nullableLanguageFilter(\"\") = %#v, want nil", got)
	}
	if got := nullableLanguageFilter("en"); got != "en" {
		t.Fatalf("nullableLanguageFilter(en) = %#v", got)
	}
}

func TestConversationVelocity_SmallGroupsStopGainingFromReplyVolume(t *testing.T) {
	if got := conversationVelocity(1, 30, 30); got != 0 {
		t.Fatalf("one author scores 0, got %v", got)
	}
	if got := conversationVelocity(3, 12, 99); got != 2 {
		t.Fatalf("three authors inside the reply allowance score 2, got %v", got)
	}
	got := conversationVelocity(3, 215, 222.5)
	want := 2 * (24.0 / 215.0)
	if got < want-1e-9 || got > want+1e-9 {
		t.Fatalf("three-author pile-on = %v, want %v", got, want)
	}
	if got := conversationVelocity(2, 48, 50); got != 16.0/48.0 {
		t.Fatalf("two-author pile-on = %v", got)
	}
	if got := conversationVelocity(15, 68, 14); got != 14 {
		t.Fatalf("broad thread keeps reply weight, got %v", got)
	}
	if got := conversationVelocity(6, 200, 5.75); got != 5.75 {
		t.Fatalf("six authors keep the broad score, got %v", got)
	}
}

func TestNormalizeStoreHashtag(t *testing.T) {
	if got := normalizeStoreHashtag(" #Nostr "); got != "nostr" {
		t.Fatalf("normalizeStoreHashtag = %q", got)
	}
	if got := normalizeStoreHashtag("!!!"); got != "" {
		t.Fatalf("invalid hashtag should be empty, got %q", got)
	}
	if got := normalizeStoreHashtag("   "); got != "" {
		t.Fatalf("blank hashtag should be empty, got %q", got)
	}
	if got := normalizeStoreHashtag("#1"); got != "" {
		t.Fatalf("all-numeric hashtag should be empty, got %q", got)
	}
	if got := normalizeStoreHashtag("42"); got != "" {
		t.Fatalf("all-numeric hashtag should be empty, got %q", got)
	}
}

func TestResolveWindowHelpers(t *testing.T) {
	for _, window := range []string{"24h", "7d", "30d", "all"} {
		if _, _, err := resolveHashtagNotesWindow(window); err != nil {
			t.Fatalf("resolveHashtagNotesWindow(%q): %v", window, err)
		}
		if _, urlsFilter, _, err := resolveDomainNotesWindow(window); err != nil || urlsFilter == "" {
			t.Fatalf("resolveDomainNotesWindow(%q) = %q %v", window, urlsFilter, err)
		}
	}
	if _, _, err := resolveHashtagNotesWindow("1h"); err == nil {
		t.Fatal("expected unsupported hashtag window error")
	}
	if _, _, _, err := resolveDomainNotesWindow("1h"); err == nil {
		t.Fatal("expected unsupported domain window error")
	}

	if col, dur, err := resolveTrendingWindow(24 * time.Hour); err != nil || col != "score_24h" || dur != 24*time.Hour {
		t.Fatalf("resolveTrendingWindow(24h) = %q %v %v", col, dur, err)
	}
	if col, dur, err := resolveTrendingWindow(7 * 24 * time.Hour); err != nil || col != "score_7d" || dur != 7*24*time.Hour {
		t.Fatalf("resolveTrendingWindow(7d) = %q %v %v", col, dur, err)
	}
	if _, _, err := resolveTrendingWindow(time.Hour); err == nil {
		t.Fatal("expected unsupported trending window error")
	}

	if col, weightCol, dur, err := resolveConversationWindow(24 * time.Hour); err != nil || col != "replies_24h" || weightCol != "reply_weight_24h" || dur != 24*time.Hour {
		t.Fatalf("resolveConversationWindow(24h) = %q %q %v %v", col, weightCol, dur, err)
	}
	if col, weightCol, dur, err := resolveConversationWindow(7 * 24 * time.Hour); err != nil || col != "replies_7d" || weightCol != "reply_weight_7d" || dur != 7*24*time.Hour {
		t.Fatalf("resolveConversationWindow(7d) = %q %q %v %v", col, weightCol, dur, err)
	}
	if _, _, _, err := resolveConversationWindow(time.Hour); err == nil {
		t.Fatal("expected unsupported conversation window error")
	}
}

func TestDBResultFromErrAndUndefinedRelation(t *testing.T) {
	if got := dbResultFromErr(nil); got != "ok" {
		t.Fatalf("nil = %q", got)
	}
	if got := dbResultFromErr(ErrNotFound); got != "not_found" {
		t.Fatalf("not found = %q", got)
	}
	if got := dbResultFromErr(errors.New("boom")); got != "error" {
		t.Fatalf("other = %q", got)
	}
	if !isUndefinedRelationError(&pgconn.PgError{Code: "42P01"}) {
		t.Fatal("expected undefined relation for 42P01")
	}
	if isUndefinedRelationError(errors.New("nope")) {
		t.Fatal("non-pg error should not be undefined relation")
	}
}

func TestGroupedNotesCTE(t *testing.T) {
	hashtagCTE, hashtagArgs := groupedNotesCTE(GroupedNoteAnalyticsQuery{
		Pubkey:    "pk",
		GroupKind: "hashtag",
		GroupKey:  "nostr",
	}, 100)
	if hashtagCTE == "" || !reflect.DeepEqual(hashtagArgs, []any{"pk", int64(100), "nostr"}) {
		t.Fatalf("hashtag CTE args = %#v", hashtagArgs)
	}
	if !strings.Contains(hashtagCTE, "INNER JOIN event_hashtags") {
		t.Fatalf("hashtag CTE missing join: %s", hashtagCTE)
	}

	tagCTE, tagArgs := groupedNotesCTE(GroupedNoteAnalyticsQuery{
		Pubkey:      "pk",
		GroupKind:   "client",
		GroupKey:    "damus",
		MetadataTag: "client",
	}, 50)
	if !reflect.DeepEqual(tagArgs, []any{"pk", int64(50), "client", "damus"}) {
		t.Fatalf("tag CTE args = %#v", tagArgs)
	}
	if !strings.Contains(tagCTE, "FROM event_tags") {
		t.Fatalf("tag CTE missing EXISTS path: %s", tagCTE)
	}
}
