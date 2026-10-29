package relayregistry_test

import (
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/relayurl"
)

func TestNIP11FetchEligible(t *testing.T) {
	if !relayregistry.NIP11FetchEligible("wss://relay.damus.io") {
		t.Fatal("public wss relay should be fetched")
	}
	if !relayregistry.NIP11FetchEligible("wss://purplepag.es") {
		t.Fatal("directory relays still have a NIP-11 document")
	}
	for _, raw := range []string{"ws://relay.damus.io", "wss://127.0.0.1", "wss://10.1.2.3"} {
		if relayregistry.NIP11FetchEligible(raw) {
			t.Fatalf("%s should not be fetched", raw)
		}
	}
}

func TestListRelaysForNIP11_SkipsFreshAndPrivate(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	store := relayregistry.NewStore(pool)
	now := time.Now().UTC()
	fresh := `{"fetched_at":"` + now.Format(time.RFC3339) + `","ok":true,"info":{"name":"Fresh"}}`
	staleAt := now.Add(-48 * time.Hour).Format(time.RFC3339)
	stale := `{"fetched_at":"` + staleAt + `","ok":false,"error":"http_404"}`

	insert := func(url, summary string) {
		t.Helper()
		var doc any
		if summary != "" {
			doc = summary
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO relay_registry (url_key, normalized_url, manual_policy, capability_summary_json)
			VALUES ($1, $2, 'none', $3::jsonb)
		`, relayurl.CanonicalKey(url), url, doc); err != nil {
			t.Fatalf("insert %s: %v", url, err)
		}
	}
	insert("wss://fresh.example", fresh)
	insert("wss://stale.example", stale)
	insert("wss://missing.example", "")
	insert("wss://127.0.0.1", "")
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_registry (url_key, normalized_url, manual_policy)
		VALUES ($1, 'wss://blocked.example', 'blocked')
	`, relayurl.CanonicalKey("wss://blocked.example")); err != nil {
		t.Fatal(err)
	}

	got, err := store.ListRelaysForNIP11(ctx, now.Add(-24*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, rec := range got {
		seen[rec.NormalizedURL] = true
	}
	if seen["wss://fresh.example"] || seen["wss://127.0.0.1"] || seen["wss://blocked.example"] {
		t.Fatalf("unexpected relays: %#v", seen)
	}
	if !seen["wss://stale.example"] || !seen["wss://missing.example"] {
		t.Fatalf("missing stale relays: %#v", seen)
	}

	if err := store.UpdateCapabilitySummary(ctx, relayurl.CanonicalKey("wss://missing.example"), []byte(fresh)); err != nil {
		t.Fatal(err)
	}
	again, err := store.ListRelaysForNIP11(ctx, now.Add(-24*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range again {
		if rec.NormalizedURL == "wss://missing.example" {
			t.Fatal("freshly stored document was selected again")
		}
	}
}
