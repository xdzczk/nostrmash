package query

import "testing"

func TestTrustedNoteRowsByMode_BoostIsNoOpAtZeroWeight(t *testing.T) {
	t.Parallel()
	rank1 := int64(1)
	rank9 := int64(9)
	rows := []trustedNoteCandidate{
		{note: TrendingNote{EventID: "a"}, trusted: true, rank: &rank9},
		{note: TrendingNote{EventID: "b"}, trusted: true, rank: &rank1},
		{note: TrendingNote{EventID: "c"}, trusted: false, rank: &rank1},
	}
	out := trustedNoteRowsByMode(rows, trustModePreferTrusted, 0)
	if len(out) != 3 || out[0].EventID != "a" || out[1].EventID != "b" || out[2].EventID != "c" {
		t.Fatalf("expected bucket split with preserved engagement order, got %#v", out)
	}
}

func TestTrustedNoteRowsByMode_BoostPrefersBetterRank(t *testing.T) {
	t.Parallel()
	rank1 := int64(1)
	rank9 := int64(9)
	rows := []trustedNoteCandidate{
		{note: TrendingNote{EventID: "a"}, trusted: true, rank: &rank9},
		{note: TrendingNote{EventID: "b"}, trusted: true, rank: &rank1},
		{note: TrendingNote{EventID: "c"}, trusted: false, rank: &rank1},
	}
	out := trustedNoteRowsByMode(rows, trustModePreferTrusted, 3)
	if len(out) != 3 || out[0].EventID != "b" || out[1].EventID != "a" || out[2].EventID != "c" {
		t.Fatalf("expected better-ranked trusted note first, got %#v", out)
	}
}

func TestNormalizedTrustRank_UsesScoreFallback(t *testing.T) {
	t.Parallel()
	high := 10.0
	low := 2.0
	peers := []trustedNoteCandidate{
		{score: &high},
		{score: &low},
	}
	gotHigh := normalizedTrustRank(&high, nil, peers)
	gotLow := normalizedTrustRank(&low, nil, peers)
	if gotHigh >= gotLow {
		t.Fatalf("expected higher score to normalize lower (better), high=%v low=%v", gotHigh, gotLow)
	}
	if gotHigh != 0 {
		t.Fatalf("expected max score to normalize to 0, got %v", gotHigh)
	}
}
