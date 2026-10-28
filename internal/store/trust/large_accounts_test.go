package trust

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestMergeFrontierCandidatesReservesTrustSlots(t *testing.T) {
	t.Parallel()
	popular := []TrustPubkeyCandidate{
		{Pubkey: "big", Rank: -5000},
		{Pubkey: "also-big", Rank: -2000},
		{Pubkey: "mid", Rank: -1500},
	}
	trustRows := []TrustPubkeyCandidate{
		{Pubkey: "trusted", Rank: 1, Score: 0.9, SourceRunID: 7},
		{Pubkey: "big", Rank: 4, Score: 0.2, SourceRunID: 7},
		{Pubkey: "other", Rank: 8, Score: 0.1, SourceRunID: 7},
	}

	got := mergeFrontierCandidates(trustRows, popular, 4)
	if len(got) != 4 {
		t.Fatalf("merged len = %d, want 4 (%+v)", len(got), got)
	}
	if got[0].Pubkey != "big" || got[0].Rank != -5000 {
		t.Fatalf("largest account should keep the negative rank, got %+v", got[0])
	}
	if got[1].Pubkey != "also-big" {
		t.Fatalf("second slot = %+v", got[1])
	}
	pubkeys := map[string]TrustPubkeyCandidate{}
	for _, candidate := range got {
		pubkeys[candidate.Pubkey] = candidate
	}
	if _, ok := pubkeys["trusted"]; !ok {
		t.Fatalf("reserved trust slot missing: %+v", got)
	}
	if candidate := pubkeys["big"]; candidate.SourceRunID != 0 {
		t.Fatalf("overlapping account should stay on the large-account row, got %+v", candidate)
	}
}

func TestMergeFrontierCandidatesFillsFromTrustWhenNoPopular(t *testing.T) {
	t.Parallel()
	trustRows := []TrustPubkeyCandidate{
		{Pubkey: "a", Rank: 1},
		{Pubkey: "b", Rank: 2},
	}
	got := mergeFrontierCandidates(trustRows, nil, 2)
	if len(got) != 2 || got[0].Pubkey != "a" || got[1].Pubkey != "b" {
		t.Fatalf("got %+v", got)
	}
}

func TestLargeAccountFollowerFloorMatchesIndex(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../../migrations/000105_profile_large_follower_index.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	needle := "follower_count >= " + strconv.FormatInt(LargeAccountFollowerFloor, 10)
	if !strings.Contains(string(data), needle) {
		t.Fatalf("migration index predicate must match %s", needle)
	}
	if !strings.Contains(largeAccountFollowerPredicate(), needle) {
		t.Fatalf("query predicate = %q, want %q", largeAccountFollowerPredicate(), needle)
	}
}
