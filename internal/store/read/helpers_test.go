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
}

func TestResolveWindowHelpers(t *testing.T) {
	for _, window := range []string{"24h", "7d", "30d", "all"} {
		if _, _, err := resolveHashtagNotesWindow(window); err != nil {
			t.Fatalf("resolveHashtagNotesWindow(%q): %v", window, err)
		}
		if _, _, err := resolveDomainNotesWindow(window); err != nil {
			t.Fatalf("resolveDomainNotesWindow(%q): %v", window, err)
		}
	}
	if _, _, err := resolveHashtagNotesWindow("1h"); err == nil {
		t.Fatal("expected unsupported hashtag window error")
	}
	if _, _, err := resolveDomainNotesWindow("1h"); err == nil {
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
