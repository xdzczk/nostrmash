package relayregistry_test

import (
	"reflect"
	"testing"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

func TestListFastHealthyLookupRelays_OrdersByLatencyNotScore(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_registry (
			url_key, normalized_url, admission_state, manual_policy, score,
			last_probe_status, last_connect_ok, last_eose_ok,
			avg_connect_latency_ms, avg_eose_latency_ms
		) VALUES
			('slow-popular', 'wss://relay.primal.net', 'active', 'none', 900,
			 'ok', TRUE, TRUE, 599, 40),
			('fast-mom', 'wss://nostr.mom', 'active', 'none', 50,
			 'ok', TRUE, TRUE, 7, 1),
			('directory', 'wss://purplepag.es', 'active', 'none', 134,
			 'ok', TRUE, TRUE, 91, 27),
			('blocked-fast', 'wss://blocked.example', 'active', 'blocked', 10,
			 'ok', TRUE, TRUE, 1, 1),
			('candidate-fast', 'wss://candidate.example', 'candidate', 'none', 10,
			 'ok', TRUE, TRUE, 2, 1),
			('healthy-pinned', 'wss://twinkle.lol', 'pinned', 'none', 20,
			 'ok', TRUE, TRUE, 100, 94),
			('flaky-connect-only', 'wss://flaky.example', 'active', 'none', 999,
			 'eose_timeout', TRUE, FALSE, 1, NULL)
	`); err != nil {
		t.Fatalf("seed lookup relays: %v", err)
	}

	got, err := s.ListFastHealthyLookupRelays(ctx, 3)
	if err != nil {
		t.Fatalf("list fast healthy lookup relays: %v", err)
	}
	// flaky-connect-only must never appear: its most recent probe failed EOSE,
	// so it has no real avg_eose_latency_ms despite a high popularity score.
	want := []string{"wss://nostr.mom", "wss://purplepag.es", "wss://twinkle.lol", "wss://relay.primal.net"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestListFastHealthyLookupRelays_ExcludesNeverProbedRelays(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_registry (
			url_key, normalized_url, admission_state, manual_policy, score
		) VALUES
			('never-probed', 'wss://never-probed.example', 'active', 'none', 500)
	`); err != nil {
		t.Fatalf("seed never-probed relay: %v", err)
	}

	got, err := s.ListFastHealthyLookupRelays(ctx, 3)
	if err != nil {
		t.Fatalf("list fast healthy lookup relays: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected never-probed relay to be excluded, got %#v", got)
	}
}
