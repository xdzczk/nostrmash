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
			last_probe_status, last_connect_ok,
			avg_connect_latency_ms, avg_eose_latency_ms
		) VALUES
			('slow-popular', 'wss://relay.primal.net', 'active', 'none', 900,
			 'ok', TRUE, 599, 40),
			('fast-mom', 'wss://nostr.mom', 'active', 'none', 50,
			 'ok', TRUE, 7, 1),
			('directory', 'wss://purplepag.es', 'active', 'none', 134,
			 'ok', TRUE, 91, 27),
			('blocked-fast', 'wss://blocked.example', 'active', 'blocked', 10,
			 'ok', TRUE, 1, 1),
			('candidate-fast', 'wss://candidate.example', 'candidate', 'none', 10,
			 'ok', TRUE, 2, 1),
			('connect-only', 'wss://twinkle.lol', 'pinned', 'none', 20,
			 'connect_failed', TRUE, 194, NULL)
	`); err != nil {
		t.Fatalf("seed lookup relays: %v", err)
	}

	got, err := s.ListFastHealthyLookupRelays(ctx, 3)
	if err != nil {
		t.Fatalf("list fast healthy lookup relays: %v", err)
	}
	want := []string{"wss://nostr.mom", "wss://purplepag.es", "wss://twinkle.lol", "wss://relay.primal.net"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}
