package store

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
	"github.com/xdzczk/nostrmash/migrations"
)

func TestMigrateFreshBootstrapAndRerunSafe(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)

	pool := setupSchemaPool(t, ctx, dbURL)

	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("first migrate failed: %v", err)
	}

	expectedTables := []string{
		"author_recent_events",
		"contact_lists_latest",
		"curated_creator_paid_tiers",
		"curated_featured_authors",
		"curated_reads_topics",
		"curated_recommended_reads",
		"deletion_events",
		"derivation_active_versions",
		"derivation_versions",
		"dm_unread_counts",
		"dm_read_cursors",
		"event_references",
		"events",
		"event_relays",
		"event_tags",
		"follower_edges",
		"invalid_events",
		"ingest_checkpoints",
		"ingest_pubkey_frontier",
		"jobs",
		"profiles_latest",
		"projection_rebuild_runs",
		"pubkey_references",
		"reaction_events",
		"reaction_count_contributions",
		"reaction_counts",
		"relay_lists_latest",
		"reply_count_contributions",
		"reply_counts",
		"repost_events",
		"repost_count_contributions",
		"repost_counts",
		"replaceable_state",
		"schema_migrations_audit",
		"thread_edges",
		"trust_runs",
		"trust_relay_suggestions",
		"trust_scores_global",
		"trust_scores_global_stage",
		"trust_seeds",
		"unresolved_thread_references",
		"zap_receipts",
	}
	for _, tableName := range expectedTables {
		var exists bool
		err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, tableName).Scan(&exists)
		if err != nil {
			t.Fatalf("check table %q existence: %v", tableName, err)
		}
		if !exists {
			t.Fatalf("expected table %q to exist", tableName)
		}
	}

	var firstRunCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations_audit`).Scan(&firstRunCount); err != nil {
		t.Fatalf("count audit rows after first run: %v", err)
	}
	expectedMigrationCount, err := embeddedMigrationCount()
	if err != nil {
		t.Fatalf("count embedded migrations: %v", err)
	}
	if firstRunCount != expectedMigrationCount {
		t.Fatalf("expected %d audit rows after first run, got %d", expectedMigrationCount, firstRunCount)
	}

	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("second migrate failed: %v", err)
	}

	var secondRunCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations_audit`).Scan(&secondRunCount); err != nil {
		t.Fatalf("count audit rows after second run: %v", err)
	}
	if secondRunCount != firstRunCount {
		t.Fatalf("expected audit row count to stay %d, got %d", firstRunCount, secondRunCount)
	}
}

func TestMigrateDetectsChecksumDrift(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)

	pool := setupSchemaPool(t, ctx, dbURL)

	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("seed migrate failed: %v", err)
	}

	_, err := pool.Exec(ctx, `
		UPDATE schema_migrations_audit
		SET checksum = 'tampered'
		WHERE migration_id = 'migrations/000002_events.sql'`)
	if err != nil {
		t.Fatalf("tamper checksum: %v", err)
	}

	err = Migrate(ctx, pool, "test-v1")
	if err == nil {
		t.Fatalf("expected checksum mismatch error")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got: %v", err)
	}
}

func TestMigrateTrustSchedulingSchemaGuards(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)

	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	assertColumnsExist := func(tableName string, columns ...string) {
		t.Helper()
		rows, err := pool.Query(ctx, `
			SELECT column_name
			FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = $1
		`, tableName)
		if err != nil {
			t.Fatalf("list columns for %s: %v", tableName, err)
		}
		defer rows.Close()

		existing := make(map[string]bool, len(columns))
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan column for %s: %v", tableName, err)
			}
			existing[name] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read columns for %s: %v", tableName, err)
		}
		for _, column := range columns {
			if !existing[column] {
				t.Fatalf("expected column %s.%s to exist", tableName, column)
			}
		}
	}

	assertColumnsExist(
		"ingest_pubkey_frontier",
		"pubkey",
		"source_run_id",
		"state",
		"first_seen_at",
		"next_eligible_at",
		"fetch_attempts",
		"success_count",
		"last_error",
	)
	assertColumnsExist(
		"trust_relay_suggestions",
		"relay_url",
		"weighted_score",
		"supporting_pubkeys_sample",
		"source_run_id",
		"first_seen_at",
		"last_seen_at",
		"is_recommended",
	)

	var sampleType string
	if err := pool.QueryRow(ctx, `
		SELECT udt_name
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'trust_relay_suggestions'
		  AND column_name = 'supporting_pubkeys_sample'
	`).Scan(&sampleType); err != nil {
		t.Fatalf("query supporting_pubkeys_sample type: %v", err)
	}
	if sampleType != "jsonb" {
		t.Fatalf("expected trust_relay_suggestions.supporting_pubkeys_sample to be jsonb, got %q", sampleType)
	}

	var frontierStateConstraint string
	if err := pool.QueryRow(ctx, `
		SELECT pg_get_constraintdef(c.oid)
		FROM pg_constraint c
		INNER JOIN pg_class tbl ON tbl.oid = c.conrelid
		INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
		WHERE ns.nspname = current_schema()
		  AND tbl.relname = 'ingest_pubkey_frontier'
		  AND c.conname = 'ingest_pubkey_frontier_state_chk'
	`).Scan(&frontierStateConstraint); err != nil {
		t.Fatalf("query ingest_pubkey_frontier state constraint: %v", err)
	}
	for _, expectedState := range []string{"candidate", "active", "cooldown", "failed"} {
		if !strings.Contains(frontierStateConstraint, expectedState) {
			t.Fatalf("expected state constraint to include %q, got %q", expectedState, frontierStateConstraint)
		}
	}

	for _, indexName := range []string{
		"idx_ingest_pubkey_frontier_state_eligibility",
		"idx_trust_relay_suggestions_recommended",
	} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, indexName).Scan(&exists); err != nil {
			t.Fatalf("check index %s existence: %v", indexName, err)
		}
		if !exists {
			t.Fatalf("expected index %q to exist", indexName)
		}
	}
}

func setupSchemaPool(t *testing.T, ctx context.Context, dbURL string) *pgxpool.Pool {
	t.Helper()
	return dbtest.SetupSchemaPool(t, ctx, dbURL, "migrate")
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	return dbtest.DatabaseURL(t, "migration")
}

func embeddedMigrationCount() (int, error) {
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		return 0, fmt.Errorf("read embedded migrations dir: %w", err)
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".sql") {
			count++
		}
	}
	return count, nil
}
