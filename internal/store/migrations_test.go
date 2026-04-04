package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
		"deletion_events",
		"derivation_active_versions",
		"derivation_versions",
		"event_references",
		"events",
		"event_relays",
		"event_tags",
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
		"unresolved_thread_references",
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
	if firstRunCount != 12 {
		t.Fatalf("expected 12 audit rows after first run, got %d", firstRunCount)
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

	adminPool, err := OpenPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schemaName := fmt.Sprintf("test_migrate_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	if _, err := adminPool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, quotedSchema)); err != nil {
		adminPool.Close()
		t.Fatalf("create schema %s: %v", schemaName, err)
	}

	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		adminPool.Close()
		t.Fatalf("parse pool config: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schemaName

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		adminPool.Close()
		t.Fatalf("open schema pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, quotedSchema))
		adminPool.Close()
	})

	return pool
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()

	candidates := []string{
		os.Getenv("TEST_DATABASE_URL"),
		os.Getenv("DATABASE_URL"),
	}
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		return candidate
	}

	t.Skip("set TEST_DATABASE_URL or DATABASE_URL to run migration integration tests")
	return ""
}
