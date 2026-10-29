package relayregistry_test

import (
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

func TestCatalogRelayEligible(t *testing.T) {
	if !relayregistry.CatalogRelayEligible("wss://relay.damus.io") {
		t.Fatal("public wss relay should be eligible")
	}
	for _, raw := range []string{
		"ws://relay.damus.io",
		"wss://127.0.0.1",
		"wss://localhost",
		"wss://10.1.2.3",
		"wss://purplepag.es",
		"wss://user.kindpag.es",
		"wss://a.purplepag.es",
	} {
		if relayregistry.CatalogRelayEligible(raw) {
			t.Fatalf("%s should be ineligible", raw)
		}
	}
}

func TestListRelayCatalog_FiltersAndRanks(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)
	now := time.Now().UTC()
	fresh := now.Add(-5 * time.Minute)
	stale := now.Add(-2 * time.Hour)

	type row struct {
		url     string
		policy  string
		state   string
		score   float64
		users   int
		probed  *time.Time
		status  string
		eoseOK  *bool
		connect *float64
		eose    *float64
	}
	eoseTrue := true
	eoseFalse := false
	connect := 12.5
	eose := 40.0
	rows := []row{
		{url: "wss://a.example", policy: "none", state: "inactive", score: 1, users: 100, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://b.example", policy: "none", state: "candidate", score: 90, users: 50, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://zz.example", policy: "none", state: "active", score: 10, users: 50, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://aa.example", policy: "none", state: "pinned", score: 10, users: 50, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://candidate.example", policy: "none", state: "candidate", score: 5, users: 40, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://purplepag.es", policy: "none", state: "active", score: 1, users: 10000, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://user.kindpag.es", policy: "none", state: "active", score: 1, users: 9000, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://blocked.example", policy: "blocked", state: "active", score: 1, users: 8000, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://drained.example", policy: "drained", state: "active", score: 1, users: 7000, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://10.1.2.3", policy: "none", state: "active", score: 1, users: 6500, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://127.0.0.1", policy: "none", state: "active", score: 1, users: 6000, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "ws://cleartext.example", policy: "none", state: "active", score: 1, users: 5500, probed: &fresh, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://stale.example", policy: "none", state: "active", score: 1, users: 5000, probed: &stale, status: "ok", eoseOK: &eoseTrue, connect: &connect, eose: &eose},
		{url: "wss://noeose.example", policy: "none", state: "probation", score: 1, users: 4000, probed: &fresh, status: "eose_timeout", eoseOK: &eoseFalse},
		{url: "wss://okfalse.example", policy: "none", state: "active", score: 1, users: 30, probed: &fresh, status: "ok", eoseOK: &eoseFalse},
		{url: "wss://never.example", policy: "none", state: "candidate", score: 1, users: 3},
	}
	for _, row := range rows {
		var probeStatus any
		if row.status != "" {
			probeStatus = row.status
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO relay_registry (
				url_key, normalized_url, manual_policy, admission_state,
				score, distinct_user_ref_count,
				last_probe_at, last_probe_status, last_eose_ok,
				avg_connect_latency_ms, avg_eose_latency_ms
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`, row.url, row.url, row.policy, row.state, row.score, row.users, row.probed, probeStatus, row.eoseOK, row.connect, row.eose); err != nil {
			t.Fatalf("insert %s: %v", row.url, err)
		}
	}

	got, err := s.ListRelayCatalog(ctx, now.Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("list catalog: %v", err)
	}
	want := []string{
		"wss://a.example",
		"wss://b.example",
		"wss://aa.example",
		"wss://zz.example",
		"wss://candidate.example",
	}
	if len(got) != len(want) {
		urls := make([]string, len(got))
		for i, rec := range got {
			urls[i] = rec.NormalizedURL
		}
		t.Fatalf("catalog = %v, want %v", urls, want)
	}
	for i, url := range want {
		if got[i].NormalizedURL != url {
			t.Fatalf("catalog[%d] = %s, want %s", i, got[i].NormalizedURL, url)
		}
	}
	if got[0].AdmissionState != relayregistry.AdmissionInactive {
		t.Fatalf("inactive relay was dropped from the catalog")
	}
	if got[0].Score != 1 || got[0].DistinctUserRefCount != 100 {
		t.Fatalf("top relay score/users = %v/%d", got[0].Score, got[0].DistinctUserRefCount)
	}
	if got[0].AvgConnectLatency == nil || *got[0].AvgConnectLatency != 12.5 {
		t.Fatalf("top relay connect latency = %v", got[0].AvgConnectLatency)
	}

	one, err := s.ListRelayCatalog(ctx, now.Add(-time.Hour), 1)
	if err != nil {
		t.Fatalf("list catalog limit 1: %v", err)
	}
	if len(one) != 1 || one[0].NormalizedURL != "wss://a.example" {
		t.Fatalf("limit 1 = %#v, want a.example", one)
	}
}

func TestListCatalogProbeTargets_StalestOfHead(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)
	now := time.Now().UTC()
	fresh := now.Add(-1 * time.Minute)
	older := now.Add(-3 * time.Hour)
	stale := now.Add(-2 * time.Hour)

	insert := func(url, policy, state string, users int, probed *time.Time, status string) {
		t.Helper()
		var probeStatus any
		if status != "" {
			probeStatus = status
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO relay_registry (
				url_key, normalized_url, manual_policy, admission_state,
				distinct_user_ref_count, last_probe_at, last_probe_status
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, url, url, policy, state, users, probed, probeStatus); err != nil {
			t.Fatalf("insert %s: %v", url, err)
		}
	}
	insert("wss://purplepag.es", "none", "active", 10000, &fresh, "ok")
	insert("wss://blocked.example", "blocked", "active", 9000, &fresh, "ok")
	insert("wss://drained.example", "drained", "active", 8000, &fresh, "ok")
	insert("wss://127.0.0.1", "none", "active", 7000, &fresh, "ok")
	insert("wss://stale.example", "none", "inactive", 5000, &stale, "ok")
	insert("wss://noeose.example", "none", "probation", 4000, &older, "eose_timeout")
	insert("wss://fresh.example", "none", "active", 100, &fresh, "ok")
	insert("wss://never.example", "none", "candidate", 3, nil, "")

	got, err := s.ListCatalogProbeTargets(ctx, 2, 1)
	if err != nil {
		t.Fatalf("list catalog probe targets: %v", err)
	}
	if len(got) != 1 || got[0].NormalizedURL != "wss://noeose.example" {
		t.Fatalf("stalest of head = %#v, want noeose.example", urlsOf(got))
	}

	both, err := s.ListCatalogProbeTargets(ctx, 2, 2)
	if err != nil {
		t.Fatalf("list catalog probe targets limit 2: %v", err)
	}
	if len(both) != 2 || both[0].NormalizedURL != "wss://noeose.example" || both[1].NormalizedURL != "wss://stale.example" {
		t.Fatalf("head of 2 = %v, want noeose then stale", urlsOf(both))
	}
	if both[1].AdmissionState != relayregistry.AdmissionInactive {
		t.Fatalf("probe head dropped an inactive popular relay")
	}

	withNever, err := s.ListCatalogProbeTargets(ctx, 4, 1)
	if err != nil {
		t.Fatalf("list catalog probe targets including never: %v", err)
	}
	// Head of 4 eligible relays is noeose, stale, fresh, never. Never probed wins.
	if len(withNever) != 1 || withNever[0].NormalizedURL != "wss://never.example" {
		t.Fatalf("never-probed head member = %v, want never.example", urlsOf(withNever))
	}
}

func urlsOf(records []relayregistry.RelayRecord) []string {
	out := make([]string, len(records))
	for i, rec := range records {
		out[i] = rec.NormalizedURL
	}
	return out
}
