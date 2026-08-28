package relayregistry_test

import (
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

// TestListRelaysForProbing_SplitsBudgetAndRotatesByStaleness locks in the two
// selection properties whose absence froze relay discovery in production:
//
//  1. Candidates get a share of every cycle's budget even when the live tier
//     (probation/active/pinned) has more than enough relays to fill it.
//  2. Within a pool, staleness beats popularity: a freshly-probed relay must
//     yield its slot to a stale one regardless of ref counts. (The previous
//     ordering put popularity first, so the same top-N most-referenced
//     relays were re-probed every cycle forever.)
func TestListRelaysForProbing_SplitsBudgetAndRotatesByStaleness(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)

	now := time.Now().UTC()
	insert := func(urlKey, state string, refCount int, lastProbeAt *time.Time) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO relay_registry (
				url_key, normalized_url, source_seed, source_manual,
				manual_policy, admission_state, distinct_user_ref_count, last_probe_at
			) VALUES ($1, 'wss://' || $1, FALSE, FALSE, 'none', $2, $3, $4)
		`, urlKey, state, refCount, lastProbeAt); err != nil {
			t.Fatalf("insert relay %s: %v", urlKey, err)
		}
	}
	recent := now.Add(-1 * time.Minute)
	stale := now.Add(-24 * time.Hour)

	// Live pool: the popular relay was just probed, the unpopular one is
	// stale. More live relays than the live budget (2 of limit 4).
	insert("live.popular.fresh", "probation", 1000, &recent)
	insert("live.unpopular.stale", "probation", 1, &stale)
	insert("live.mid.fresh", "active", 500, &recent)

	// Discovery pool: never-probed candidates plus a freshly probed one.
	insert("cand.never.a", "candidate", 10, nil)
	insert("cand.never.b", "candidate", 5, nil)
	insert("cand.fresh", "candidate", 2000, &recent)

	relays, err := s.ListRelaysForProbing(ctx, 4)
	if err != nil {
		t.Fatalf("list relays for probing: %v", err)
	}
	got := make(map[string]bool, len(relays))
	for _, r := range relays {
		got[r.URLKey] = true
	}
	if len(relays) != 4 {
		t.Fatalf("expected 4 relays, got %d (%v)", len(relays), got)
	}
	// Live budget (2): stalest first regardless of popularity.
	if !got["live.unpopular.stale"] {
		t.Fatalf("stale live relay must be selected over fresh popular ones, got %v", got)
	}
	// Discovery budget (2): never-probed candidates beat the fresh popular one.
	if !got["cand.never.a"] || !got["cand.never.b"] {
		t.Fatalf("never-probed candidates must fill the discovery budget, got %v", got)
	}
	if got["cand.fresh"] {
		t.Fatalf("freshly-probed candidate must not displace never-probed ones, got %v", got)
	}
}
