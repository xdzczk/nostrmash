package relaydiscovery

import (
	"testing"
)

func TestSortCandidates(t *testing.T) {
	candidates := []relayCandidateAgg{
		{NormalizedURL: "wss://a.com", DistinctUsers: 5},
		{NormalizedURL: "wss://b.com", DistinctUsers: 20},
		{NormalizedURL: "wss://c.com", DistinctUsers: 10},
	}
	sortCandidates(candidates)
	if candidates[0].NormalizedURL != "wss://b.com" {
		t.Fatalf("expected most popular first, got %s", candidates[0].NormalizedURL)
	}
	if candidates[1].NormalizedURL != "wss://c.com" {
		t.Fatalf("expected second most popular second, got %s", candidates[1].NormalizedURL)
	}
	if candidates[2].NormalizedURL != "wss://a.com" {
		t.Fatalf("expected least popular last, got %s", candidates[2].NormalizedURL)
	}
}

func TestSortCandidates_Empty(t *testing.T) {
	sortCandidates(nil)
	sortCandidates([]relayCandidateAgg{})
}

func TestSortCandidates_Single(t *testing.T) {
	candidates := []relayCandidateAgg{
		{NormalizedURL: "wss://only.com", DistinctUsers: 1},
	}
	sortCandidates(candidates)
	if candidates[0].NormalizedURL != "wss://only.com" {
		t.Fatal("single element sort should preserve it")
	}
}
