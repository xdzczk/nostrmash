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
		"trust_scores_global",
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
