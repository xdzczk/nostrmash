package trust

import (
	"reflect"
	"testing"
	"time"
)

func TestNormalizePubkeys(t *testing.T) {
	got := normalizePubkeys([]string{"  a  ", "", "b", "a", " b "})
	want := []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizePubkeys = %#v, want %#v", got, want)
	}
}

func TestNormalizeSeedPubkeys(t *testing.T) {
	got := normalizeSeedPubkeys([]string{"  AbC  ", "", "abc", "DEF", " def "})
	want := []string{"abc", "def"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizeSeedPubkeys = %#v, want %#v", got, want)
	}
}

func TestNormalizeTrustPolicy(t *testing.T) {
	got := normalizeTrustPolicy(TrustQualificationPolicy{MaxHops: -3, MinimumScore: -1.5})
	if got.MaxHops != 0 || got.MinimumScore != 0 {
		t.Fatalf("normalizeTrustPolicy = %+v, want non-negative clamps", got)
	}
}

func TestTrustStateTrusted(t *testing.T) {
	hop := func(v int) *int { return &v }
	score := func(v float64) *float64 { return &v }

	cases := []struct {
		name   string
		state  TrustState
		policy TrustQualificationPolicy
		want   bool
	}{
		{
			name:  "seed always trusted",
			state: TrustState{Pubkey: "seed", IsSeed: true},
			want:  true,
		},
		{
			name:  "unqualified non-seed",
			state: TrustState{Pubkey: "x"},
			want:  false,
		},
		{
			name:   "hop beyond max",
			state:  TrustState{Pubkey: "x", HopDistance: hop(3)},
			policy: TrustQualificationPolicy{MaxHops: 2},
			want:   false,
		},
		{
			name:   "score below minimum",
			state:  TrustState{Pubkey: "x", Score: score(0.5)},
			policy: TrustQualificationPolicy{MinimumScore: 1},
			want:   false,
		},
		{
			name:   "qualified non-seed",
			state:  TrustState{Pubkey: "x", HopDistance: hop(1), Score: score(2)},
			policy: TrustQualificationPolicy{MaxHops: 2, MinimumScore: 1},
			want:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := trustStateTrusted(tc.state, tc.policy); got != tc.want {
				t.Fatalf("trustStateTrusted = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTrustHopBucketAndTier(t *testing.T) {
	hop := func(v int) *int { return &v }

	if got := trustHopBucket(nil); got != "unknown" {
		t.Fatalf("nil hops = %q", got)
	}
	for _, tc := range []struct {
		hops int
		want string
	}{
		{0, "0"}, {1, "1"}, {2, "2"}, {3, "3"}, {9, "4_plus"},
	} {
		if got := trustHopBucket(hop(tc.hops)); got != tc.want {
			t.Fatalf("trustHopBucket(%d) = %q, want %q", tc.hops, got, tc.want)
		}
	}

	if got := trustTierFromState(TrustState{IsSeed: true}); got != "seed" {
		t.Fatalf("seed tier = %q", got)
	}
	if got := trustTierFromState(TrustState{}); got != "unknown" {
		t.Fatalf("unknown tier = %q", got)
	}
	if got := trustTierFromState(TrustState{HopDistance: hop(1)}); got != "core" {
		t.Fatalf("core tier = %q", got)
	}
	if got := trustTierFromState(TrustState{HopDistance: hop(3)}); got != "near" {
		t.Fatalf("near tier = %q", got)
	}
	if got := trustTierFromState(TrustState{HopDistance: hop(4)}); got != "outer" {
		t.Fatalf("outer tier = %q", got)
	}
}

func TestTrustQualificationFromState(t *testing.T) {
	hop := 1
	score := 2.5
	rank := int64(7)
	runID := int64(99)
	now := time.Unix(1_700_000_000, 0).UTC()
	state := TrustState{
		Pubkey:       "pk",
		IsSeed:       false,
		HopDistance:  &hop,
		Score:        &score,
		Rank:         &rank,
		GenerationID: &runID,
		ComputedAt:   &now,
	}
	got := trustQualificationFromState(state, TrustQualificationPolicy{MaxHops: 2, MinimumScore: 1})
	if !got.Trusted || got.Pubkey != "pk" || got.DistanceHops == nil || *got.DistanceHops != 1 {
		t.Fatalf("unexpected qualification: %+v", got)
	}
	if got.Score == nil || *got.Score != 2.5 || got.Rank == nil || *got.Rank != 7 || got.SourceRunID == nil || *got.SourceRunID != 99 {
		t.Fatalf("unexpected qualification fields: %+v", got)
	}
}

func TestNormalizeRelayURLsAndSort(t *testing.T) {
	normalized, order := NormalizeRelayURLs([]string{
		" wss://A ",
		"wss://b",
		"wss://a",
		"",
		" WSS://B ",
		"wss://c",
	})
	if len(normalized) != 3 || normalized[0] != "wss://a" || normalized[1] != "wss://b" || normalized[2] != "wss://c" {
		t.Fatalf("unexpected normalized values: %#v", normalized)
	}
	if order["wss://a"] != 0 || order["wss://b"] != 1 || order["wss://c"] != 5 {
		t.Fatalf("unexpected base order map: %#v", order)
	}

	sorted := SortRelaysByWeights(
		[]string{"wss://a", "wss://b", "wss://c"},
		map[string]int{"wss://a": 0, "wss://b": 1, "wss://c": 2},
		map[string]float64{"wss://c": 10, "wss://a": 5},
	)
	if sorted[0] != "wss://c" || sorted[1] != "wss://a" || sorted[2] != "wss://b" {
		t.Fatalf("unexpected sorted order: %#v", sorted)
	}
}
