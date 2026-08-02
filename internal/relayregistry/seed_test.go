package relayregistry_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/dbmigrate"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

func setupRelayRegistryPool(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay_registry_seed"), "relay_registry_seed")
	if err := dbmigrate.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool, ctx
}

func readRelayRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, urlKey string) (sourceSeed, sourceManual bool, policy, state string) {
	t.Helper()
	if err := pool.QueryRow(ctx, `
		SELECT source_seed, source_manual, manual_policy, admission_state
		FROM relay_registry
		WHERE url_key = $1
	`, urlKey).Scan(&sourceSeed, &sourceManual, &policy, &state); err != nil {
		t.Fatalf("read relay %s: %v", urlKey, err)
	}
	return sourceSeed, sourceManual, policy, state
}

func TestUpsertSeedRelay_NewSeedIsActiveNotPinned(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)

	if err := s.UpsertSeedRelay(ctx, "relay.primal.net", "wss://relay.primal.net"); err != nil {
		t.Fatalf("upsert seed: %v", err)
	}
	sourceSeed, sourceManual, policy, state := readRelayRow(t, ctx, pool, "relay.primal.net")
	if !sourceSeed {
		t.Fatal("expected source_seed=true")
	}
	if sourceManual {
		t.Fatal("seed upsert must not set source_manual")
	}
	if policy != "none" || state != "active" {
		t.Fatalf("expected none/active, got %s/%s", policy, state)
	}
}

func TestUpsertSeedRelay_ClearsLegacySeedDerivedPin(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_registry (
			url_key, normalized_url, source_seed, source_manual, manual_policy, admission_state
		) VALUES ('nos.lol', 'wss://nos.lol', TRUE, FALSE, 'pinned', 'pinned')
	`); err != nil {
		t.Fatalf("seed legacy pin: %v", err)
	}

	if err := s.UpsertSeedRelay(ctx, "nos.lol", "wss://nos.lol"); err != nil {
		t.Fatalf("upsert seed: %v", err)
	}
	_, _, policy, state := readRelayRow(t, ctx, pool, "nos.lol")
	if policy != "none" || state != "active" {
		t.Fatalf("legacy seed pin should clear to none/active, got %s/%s", policy, state)
	}
}

func TestUpsertSeedRelay_PreservesBlockedAndSourceManualPin(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_registry (
			url_key, normalized_url, source_seed, source_manual, manual_policy, admission_state
		) VALUES
			('blocked.example', 'wss://blocked.example', FALSE, TRUE, 'blocked', 'blocked'),
			('ops-pin.example', 'wss://ops-pin.example', FALSE, TRUE, 'pinned', 'pinned')
	`); err != nil {
		t.Fatalf("seed protected rows: %v", err)
	}

	if err := s.UpsertSeedRelay(ctx, "blocked.example", "wss://blocked.example"); err != nil {
		t.Fatalf("upsert blocked seed: %v", err)
	}
	sourceSeed, _, policy, state := readRelayRow(t, ctx, pool, "blocked.example")
	if !sourceSeed || policy != "blocked" || state != "blocked" {
		t.Fatalf("blocked policy must be preserved, got seed=%v %s/%s", sourceSeed, policy, state)
	}

	if err := s.UpsertSeedRelay(ctx, "ops-pin.example", "wss://ops-pin.example"); err != nil {
		t.Fatalf("upsert ops-pin seed: %v", err)
	}
	sourceSeed, sourceManual, policy, state := readRelayRow(t, ctx, pool, "ops-pin.example")
	if !sourceSeed || !sourceManual || policy != "pinned" || state != "pinned" {
		t.Fatalf("source_manual pin must be preserved, got seed=%v manual=%v %s/%s", sourceSeed, sourceManual, policy, state)
	}
}

func TestClearMissingSeedRelays_ClearsLegacySeedPinPreservesManualPin(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_registry (
			url_key, normalized_url, source_seed, source_manual, manual_policy, admission_state
		) VALUES
			('legacy-seed', 'wss://legacy-seed', TRUE, FALSE, 'pinned', 'pinned'),
			('ops-pin', 'wss://ops-pin', TRUE, TRUE, 'pinned', 'pinned'),
			('keep-seed', 'wss://keep-seed', TRUE, FALSE, 'none', 'active')
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	cleared, err := s.ClearMissingSeedRelays(ctx, []string{"keep-seed"})
	if err != nil {
		t.Fatalf("clear missing seeds: %v", err)
	}
	if cleared != 2 {
		t.Fatalf("expected 2 cleared rows, got %d", cleared)
	}

	sourceSeed, _, policy, state := readRelayRow(t, ctx, pool, "legacy-seed")
	if sourceSeed || policy != "none" || state != "inactive" {
		t.Fatalf("legacy seed pin should clear to inactive/none, got seed=%v %s/%s", sourceSeed, policy, state)
	}

	sourceSeed, sourceManual, policy, state := readRelayRow(t, ctx, pool, "ops-pin")
	if sourceSeed || !sourceManual || policy != "pinned" || state != "pinned" {
		t.Fatalf("source_manual pin must survive seed clear, got seed=%v manual=%v %s/%s", sourceSeed, sourceManual, policy, state)
	}

	sourceSeed, _, policy, state = readRelayRow(t, ctx, pool, "keep-seed")
	if !sourceSeed || policy != "none" || state != "active" {
		t.Fatalf("kept seed must remain, got seed=%v %s/%s", sourceSeed, policy, state)
	}
}

func TestSetManualPolicy_MarksSourceManual(t *testing.T) {
	pool, ctx := setupRelayRegistryPool(t)
	s := relayregistry.NewStore(pool)

	if err := s.EnsureRelayExists(ctx, "manual.example", "wss://manual.example"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := s.SetManualPolicy(ctx, "manual.example", relayregistry.ManualPolicyPinned); err != nil {
		t.Fatalf("set pinned: %v", err)
	}
	_, sourceManual, policy, _ := readRelayRow(t, ctx, pool, "manual.example")
	if !sourceManual || policy != "pinned" {
		t.Fatalf("expected source_manual pin, got manual=%v policy=%s", sourceManual, policy)
	}

	if err := s.SetManualPolicy(ctx, "manual.example", relayregistry.ManualPolicyNone); err != nil {
		t.Fatalf("clear policy: %v", err)
	}
	_, sourceManual, policy, _ = readRelayRow(t, ctx, pool, "manual.example")
	if sourceManual || policy != "none" {
		t.Fatalf("clearing policy should clear source_manual, got manual=%v policy=%s", sourceManual, policy)
	}
}
