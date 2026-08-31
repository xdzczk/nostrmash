package derivation

import (
	"testing"
	"time"
)

func TestComputeTrendingScore(t *testing.T) {
	const window = 24 * time.Hour
	const now = int64(1_000_000)

	t.Run("zero engagement yields zero", func(t *testing.T) {
		if got := computeTrendingScore(window, now, now, 0, 0, 0, 0, 0); got != 0 {
			t.Fatalf("want 0, got %v", got)
		}
	})

	t.Run("note older than window is dropped", func(t *testing.T) {
		created := now - int64((window+time.Hour)/time.Second)
		if got := computeTrendingScore(window, now, created, 100, 100, 100, 100, 100); got != 0 {
			t.Fatalf("want 0 for out-of-window note, got %v", got)
		}
	})

	t.Run("non-positive window yields zero", func(t *testing.T) {
		if got := computeTrendingScore(0, now, now, 10, 10, 10, 10, 10); got != 0 {
			t.Fatalf("want 0 for zero window, got %v", got)
		}
	})

	t.Run("fresh note scores higher than an older one with identical engagement", func(t *testing.T) {
		fresh := computeTrendingScore(window, now, now, 5, 2, 10, 1, 0)
		older := computeTrendingScore(window, now, now-int64(window/time.Second)/2, 5, 2, 10, 1, 0)
		if fresh <= older {
			t.Fatalf("expected decay: fresh %v should exceed older %v", fresh, older)
		}
		if older <= 0 {
			t.Fatalf("older note should still score positive, got %v", older)
		}
	})

	t.Run("future note clamps age to zero (no decay boost)", func(t *testing.T) {
		future := computeTrendingScore(window, now, now+5000, 1, 0, 0, 0, 0)
		atNow := computeTrendingScore(window, now, now, 1, 0, 0, 0, 0)
		if future != atNow {
			t.Fatalf("future note should clamp to now: future %v atNow %v", future, atNow)
		}
	})

	t.Run("result is rounded to three decimals", func(t *testing.T) {
		got := computeTrendingScore(window, now, now, 1, 0, 0, 0, 0)
		scaled := got * 1000
		if scaled != float64(int64(scaled)) {
			t.Fatalf("expected 3-decimal rounding, got %v", got)
		}
	})
}

func TestComputeProfileTrendingScore(t *testing.T) {
	const window = 7 * 24 * time.Hour
	const now = int64(2_000_000)
	activity := now - 3600

	t.Run("nil recent activity yields zero", func(t *testing.T) {
		if got := computeProfileTrendingScore(window, now, nil, 10, 5, 100, 5000, 4); got != 0 {
			t.Fatalf("want 0 for nil activity, got %v", got)
		}
	})

	t.Run("activity older than window yields zero", func(t *testing.T) {
		old := now - int64((window+time.Hour)/time.Second)
		if got := computeProfileTrendingScore(window, now, &old, 10, 5, 100, 5000, 4); got != 0 {
			t.Fatalf("want 0 for stale profile, got %v", got)
		}
	})

	t.Run("more engagement raises the score", func(t *testing.T) {
		low := computeProfileTrendingScore(window, now, &activity, 10, 5, 10, 0, 4)
		high := computeProfileTrendingScore(window, now, &activity, 10, 5, 500, 0, 4)
		if high <= low {
			t.Fatalf("expected engagement to raise score: low %v high %v", low, high)
		}
	})

	t.Run("negative inputs are clamped, not panicking", func(t *testing.T) {
		got := computeProfileTrendingScore(window, now, &activity, -5, -5, -5, -5, -5)
		if got < 0 {
			t.Fatalf("score must never be negative, got %v", got)
		}
	})

	t.Run("zero engagement is ineligible even with posts and consistency", func(t *testing.T) {
		// A profile that only posted (or only received zaps recorded as
		// volume, or was active for several days) must not fill a
		// "Profiles in motion" slot. Engagement is the eligibility floor.
		got := computeProfileTrendingScore(window, now, &activity, 12, 4, 0, 9_000_000, 6)
		if got != 0 {
			t.Fatalf("want 0 for zero-engagement profile, got %v", got)
		}
	})

	t.Run("high engagement-per-post outranks high-volume low-engagement-per-post", func(t *testing.T) {
		// Similar totals (posts+engagement in the same ballpark), but one
		// profile earns its engagement from few posts (high ratio) and the
		// other spreads the same engagement across many more posts (low
		// ratio). The "Profiles in motion" score should favor the former.
		highRatio := computeProfileTrendingScore(window, now, &activity, 5, 0, 400, 0, 4)
		lowRatio := computeProfileTrendingScore(window, now, &activity, 80, 0, 400, 0, 4)
		if highRatio <= lowRatio {
			t.Fatalf("expected high engagement-per-post to outrank high-volume/low-ratio: highRatio %v lowRatio %v", highRatio, lowRatio)
		}
	})
}

func TestComputeProfileRisingScore(t *testing.T) {
	t.Run("no trending, no credited growth, and no engagement yields zero", func(t *testing.T) {
		if got := computeProfileRisingScore(0, 100, 2, 0, 5, 5, 3); got != 0 {
			t.Fatalf("want 0 when every rising input is empty/noise, got %v", got)
		}
	})

	t.Run("meaningful follower growth scores without a trending score", func(t *testing.T) {
		// A small account gaining 60 followers with no measured
		// engagement must still be eligible for "Up and coming" — the
		// trending engagement floor must not also lock rising.
		got := computeProfileRisingScore(0, 400, 60, 0, 5, 0, 5)
		if got <= 0 {
			t.Fatalf("expected credited follower growth to produce a rising score without trending, got %v", got)
		}
	})

	t.Run("new followers increase rising score", func(t *testing.T) {
		few := computeProfileRisingScore(10, 1000, 1, 100, 10, 10, 5)
		many := computeProfileRisingScore(10, 1000, 200, 100, 10, 10, 5)
		if many <= few {
			t.Fatalf("expected new followers to raise score: few %v many %v", few, many)
		}
	})

	t.Run("larger established audience is penalized", func(t *testing.T) {
		small := computeProfileRisingScore(10, 100, 20, 100, 10, 10, 5)
		large := computeProfileRisingScore(10, 1_000_000, 20, 100, 10, 10, 5)
		if large >= small {
			t.Fatalf("expected audience penalty: small %v large %v", small, large)
		}
	})

	t.Run("small account with high relative engagement scores well without new followers", func(t *testing.T) {
		// Zero new followers, but engagement is large relative to a tiny
		// audience (50 followers) — the relative-engagement path should
		// still produce a meaningfully positive score.
		zeroFollowers := computeProfileRisingScore(10, 50, 0, 200, 10, 5, 5)
		if zeroFollowers <= 0 {
			t.Fatalf("expected relative engagement to raise score above zero with no new followers, got %v", zeroFollowers)
		}
		// The same relative-engagement profile should score higher than an
		// otherwise-identical profile with negligible engagement.
		negligibleEngagement := computeProfileRisingScore(10, 50, 0, 1, 10, 5, 5)
		if zeroFollowers <= negligibleEngagement {
			t.Fatalf("expected high relative engagement to raise score: high %v low %v", zeroFollowers, negligibleEngagement)
		}
	})

	t.Run("same engagement counts for more with a smaller audience", func(t *testing.T) {
		smallAudience := computeProfileRisingScore(10, 50, 0, 200, 10, 5, 5)
		largeAudience := computeProfileRisingScore(10, 50_000, 0, 200, 10, 5, 5)
		if smallAudience <= largeAudience {
			t.Fatalf("expected relative engagement to matter less for a larger audience: small %v large %v", smallAudience, largeAudience)
		}
	})

	t.Run("substantial absolute growth outranks a trivial handful of new followers", func(t *testing.T) {
		// A 400-follower account gaining 60 new followers should rank above
		// a 5-follower account gaining 5 new followers: the former is a
		// meaningfully larger absolute jump, even though the latter's
		// growth looks bigger as a raw percentage of its (near-empty)
		// audience. Both accounts have been posting for several days
		// (activeDays=5) with no measured engagement, so the follower
		// momentum term alone must carry this ordering.
		substantialGrowth := computeProfileRisingScore(10, 400, 60, 0, 5, 0, 5)
		trivialGrowth := computeProfileRisingScore(10, 5, 5, 0, 5, 0, 5)
		if substantialGrowth <= trivialGrowth {
			t.Fatalf("expected substantial absolute growth to outrank trivial growth: substantial %v trivial %v", substantialGrowth, trivialGrowth)
		}
	})

	t.Run("a couple of new followers is treated as noise", func(t *testing.T) {
		// Below the noise floor, extra "new followers" should barely move
		// the score, so a brand-new account with 1-2 followers doesn't
		// crowd out accounts with real, credited growth.
		none := computeProfileRisingScore(10, 5, 0, 0, 5, 0, 1)
		trivial := computeProfileRisingScore(10, 5, 2, 0, 5, 0, 1)
		if trivial != none {
			t.Fatalf("expected new followers at/below the noise floor not to change the score: none %v trivial %v", none, trivial)
		}
	})

	t.Run("accounts above the small-audience band are penalized much more steeply", func(t *testing.T) {
		// Same +60 follower jump: a still-small 400-follower account
		// should clearly outrank a 5,000-follower account. The hinge at
		// 500 is what keeps "Up and coming" mostly sub-500.
		small := computeProfileRisingScore(10, 400, 60, 0, 5, 0, 5)
		mid := computeProfileRisingScore(10, 5_000, 60, 0, 5, 0, 5)
		if small <= mid {
			t.Fatalf("expected 400-follower growth to outrank the same jump on a 5k account: small %v mid %v", small, mid)
		}
	})

	t.Run("follower momentum is not diluted by days of sustained activity", func(t *testing.T) {
		// A profile active for several days shouldn't lose follower-growth
		// credit relative to an identical profile active for a single day
		// -- multi-day sustained growth is if anything more meaningful, not
		// less.
		oneDayActive := computeProfileRisingScore(10, 400, 60, 0, 5, 0, 1)
		fiveDaysActive := computeProfileRisingScore(10, 400, 60, 0, 5, 0, 5)
		if fiveDaysActive != oneDayActive {
			t.Fatalf("expected sustained activity not to change a purely follower-driven score: oneDay %v fiveDays %v", oneDayActive, fiveDaysActive)
		}
	})
}

func TestMaxInt64(t *testing.T) {
	cases := []struct {
		a, b, want int64
	}{
		{1, 2, 2},
		{5, 3, 5},
		{-1, 0, 0},
		{-5, -9, -5},
		{7, 7, 7},
	}
	for _, tc := range cases {
		if got := maxInt64(tc.a, tc.b); got != tc.want {
			t.Fatalf("maxInt64(%d,%d)=%d want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestParseRelationMarker(t *testing.T) {
	cases := []struct {
		in      string
		wantRel string
		wantOK  bool
	}{
		{"root", "root", true},
		{"reply", "reply", true},
		{"mention", "mention", true},
		{"  ROOT ", "root", true},
		{"Reply", "reply", true},
		{"", "", false},
		{"quote", "", false},
		{"unknown", "", false},
	}
	for _, tc := range cases {
		gotRel, gotOK := ParseRelationMarker(tc.in)
		if gotRel != tc.wantRel || gotOK != tc.wantOK {
			t.Fatalf("ParseRelationMarker(%q)=(%q,%t) want (%q,%t)", tc.in, gotRel, gotOK, tc.wantRel, tc.wantOK)
		}
	}
}

func TestDeriveEventReferences(t *testing.T) {
	t.Run("marked markers are preserved with relay hints", func(t *testing.T) {
		tags := [][]string{
			{"e", "root_evt", "wss://relay.one", "root"},
			{"p", "somepubkey"},
			{"e", "reply_evt", "wss://relay.two", "reply"},
		}
		refs := deriveEventReferences("src", tags)
		if len(refs) != 2 {
			t.Fatalf("expected 2 refs, got %d: %+v", len(refs), refs)
		}
		if got := firstReferenceByRelation(refs, "root"); got != "root_evt" {
			t.Fatalf("root ref = %q want root_evt", got)
		}
		if got := firstReferenceByRelation(refs, "reply"); got != "reply_evt" {
			t.Fatalf("reply ref = %q want reply_evt", got)
		}
		if refs[0].RelayHint != "wss://relay.one" {
			t.Fatalf("relay hint not captured: %q", refs[0].RelayHint)
		}
	})

	t.Run("legacy positional: single unmarked e-tag becomes root", func(t *testing.T) {
		tags := [][]string{{"e", "only_evt"}}
		refs := deriveEventReferences("src", tags)
		if len(refs) != 1 || refs[0].Relation != "root" {
			t.Fatalf("expected single root ref, got %+v", refs)
		}
		if got := ReplyParentEventID(tags); got != "only_evt" {
			t.Fatalf("ReplyParentEventID single unmarked = %q want only_evt", got)
		}
	})

	t.Run("ReplyParentEventID prefers reply over root", func(t *testing.T) {
		tags := [][]string{
			{"e", "root_evt", "", "root"},
			{"e", "reply_evt", "", "reply"},
		}
		if got := ReplyParentEventID(tags); got != "reply_evt" {
			t.Fatalf("ReplyParentEventID = %q want reply_evt", got)
		}
	})

	t.Run("legacy positional: first is root, last is reply, middle mention", func(t *testing.T) {
		tags := [][]string{
			{"e", "first"},
			{"e", "middle"},
			{"e", "last"},
		}
		refs := deriveEventReferences("src", tags)
		if len(refs) != 3 {
			t.Fatalf("expected 3 refs, got %d", len(refs))
		}
		if firstReferenceByRelation(refs, "root") != "first" {
			t.Fatalf("first should be root: %+v", refs)
		}
		if firstReferenceByRelation(refs, "reply") != "last" {
			t.Fatalf("last should be reply: %+v", refs)
		}
		if firstReferenceByRelation(refs, "mention") != "middle" {
			t.Fatalf("middle should be mention: %+v", refs)
		}
	})

	t.Run("blank and short tags are skipped", func(t *testing.T) {
		tags := [][]string{
			{"e"},
			{"e", "   "},
			{"e", "good"},
		}
		refs := deriveEventReferences("src", tags)
		if len(refs) != 1 || refs[0].Referenced != "good" {
			t.Fatalf("expected only the good ref, got %+v", refs)
		}
	})

	t.Run("no e-tags yields empty, non-nil slice", func(t *testing.T) {
		refs := deriveEventReferences("src", [][]string{{"p", "x"}})
		if refs == nil {
			t.Fatal("expected non-nil slice")
		}
		if len(refs) != 0 {
			t.Fatalf("expected empty slice, got %+v", refs)
		}
	})
}

func TestNormalizeUniqueIDs(t *testing.T) {
	t.Run("dedupes, trims, drops blanks and sorts", func(t *testing.T) {
		got := normalizeUniqueIDs([]string{" b ", "a", "b", "", "  ", "a"})
		want := []string{"a", "b"}
		if len(got) != len(want) {
			t.Fatalf("got %v want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %v want %v", got, want)
			}
		}
	})

	t.Run("nil input returns nil; all-blank returns empty", func(t *testing.T) {
		if got := normalizeUniqueIDs(nil); got != nil {
			t.Fatalf("want nil, got %v", got)
		}
		if got := normalizeUniqueIDs([]string{"  ", ""}); len(got) != 0 {
			t.Fatalf("want empty for all-blank input, got %v", got)
		}
	})
}

func TestFirstTagValue(t *testing.T) {
	tags := [][]string{
		{"p"},
		{"e", "  evt  "},
		{"t", "hashtag"},
		{"t", "second"},
	}
	if got := firstTagValue(tags, "e"); got != "evt" {
		t.Fatalf("e value = %q want evt", got)
	}
	if got := firstTagValue(tags, "t"); got != "hashtag" {
		t.Fatalf("t value = %q want hashtag (first match)", got)
	}
	if got := firstTagValue(tags, "missing"); got != "" {
		t.Fatalf("missing tag should be empty, got %q", got)
	}
}

func TestNullIfBlank(t *testing.T) {
	if got := nullIfBlank("   "); got != nil {
		t.Fatalf("blank should be nil, got %q", *got)
	}
	if got := nullIfBlank("  v "); got == nil || *got != "v" {
		t.Fatalf("expected trimmed 'v', got %v", got)
	}
}

func TestParseZapAmountSats(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"   ", 0},
		{"notanumber", 0},
		{"0", 0},
		{"-5", 0},
		{"500", 500},
		{"999", 999},
		{"1000", 1},
		{"21000", 21},
		{" 5000 ", 5},
	}
	for _, tc := range cases {
		if got := parseZapAmountSats(tc.in); got != tc.want {
			t.Fatalf("parseZapAmountSats(%q)=%d want %d", tc.in, got, tc.want)
		}
	}
}
