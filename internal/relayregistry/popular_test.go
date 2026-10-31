package relayregistry_test

import (
	"testing"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

func TestListPopularRelays_RanksByUserCount(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)

	rows := []struct {
		url    string
		policy string
		state  string
		users  int
	}{
		{url: "wss://zz.example", policy: "none", state: "active", users: 50},
		{url: "wss://aa.example", policy: "none", state: "inactive", users: 50},
		{url: "wss://top.example", policy: "none", state: "candidate", users: 100},
		{url: "wss://drained.example", policy: "drained", state: "active", users: 80},
		{url: "wss://blocked.example", policy: "blocked", state: "active", users: 8000},
		{url: "wss://purplepag.es", policy: "none", state: "active", users: 9000},
		{url: "wss://10.1.2.3", policy: "none", state: "active", users: 7000},
		{url: "ws://cleartext.example", policy: "none", state: "active", users: 6000},
		{url: "wss://rare.example", policy: "none", state: "candidate", users: 2},
		{url: "wss://floor.example", policy: "none", state: "candidate", users: 3},
		{url: "wss://never.example", policy: "none", state: "candidate", users: 40},
	}
	for _, row := range rows {
		if _, err := pool.Exec(ctx, `
			INSERT INTO relay_registry (
				url_key, normalized_url, manual_policy, admission_state, distinct_user_ref_count
			) VALUES ($1, $2, $3, $4, $5)
		`, row.url, row.url, row.policy, row.state, row.users); err != nil {
			t.Fatalf("insert %s: %v", row.url, err)
		}
	}

	got, total, err := s.ListPopularRelays(ctx, 3, 3)
	if err != nil {
		t.Fatalf("list popular relays: %v", err)
	}
	want := []string{"wss://top.example", "wss://drained.example", "wss://aa.example"}
	if len(got) != len(want) {
		t.Fatalf("page len = %d, want %d (%v)", len(got), len(want), urlsOf(got))
	}
	for i, url := range want {
		if got[i].NormalizedURL != url {
			t.Fatalf("rank[%d] = %s, want %s", i, got[i].NormalizedURL, url)
		}
	}
	if got[0].DistinctUserRefCount != 100 || got[0].AdmissionState != relayregistry.AdmissionCandidate {
		t.Fatalf("top relay = %+v", got[0])
	}
	// Eligible relays above the cutoff, including the ones past the page limit:
	// top, drained, aa, zz, never, floor.
	if total != 6 {
		t.Fatalf("total = %d, want 6", total)
	}
}
