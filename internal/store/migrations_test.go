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

	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	expectedTables := []string{
		"applied_stat_deltas",
		"author_activity_daily",
		"author_activity_windows",
		"author_engagement_stats",
		"author_hashtag_daily",
		"author_hourly_activity",
		"author_media_daily",
		"author_recent_events",
		"author_media_mix_stats",
		"author_posting_patterns",
		"author_topic_stats",
		"follower_gains_daily",
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
		"event_hashtags",
		"event_urls",
		"events",
		"event_relays",
		"event_tags",
		"follower_edges",
		"invalid_events",
		"ingest_checkpoints",
		"ingest_pubkey_frontier",
		"jobs",
		"note_discovery_stats",
		"pending_author_analytics_recomputes",
		"pending_meilisearch_syncs",
		"pending_profile_stats_recomputes",
		"profile_discovery_recent_activity",
		"profile_discovery_stats",
		"profile_public_stats",
		"profiles_latest",
		"projection_rebuild_runs",
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
		"thread_summaries",
		"trust_runs",
		"trust_relay_suggestions",
		"trust_graph_snapshot",
		"trusted_note_discovery_candidates",
		"trusted_profile_discovery_candidates",
		"trusted_discovery_projection_state",
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

	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	var secondRunCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations_audit`).Scan(&secondRunCount); err != nil {
		t.Fatalf("count audit rows after second run: %v", err)
	}
	if secondRunCount != firstRunCount {
		t.Fatalf("expected audit row count to stay %d, got %d", firstRunCount, secondRunCount)
	}

	var canonicalDomainNullable string
	if err := pool.QueryRow(ctx, `
		SELECT is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'event_urls'
		  AND column_name = 'canonical_domain'
	`).Scan(&canonicalDomainNullable); err != nil {
		t.Fatalf("query event_urls canonical_domain column: %v", err)
	}
	if canonicalDomainNullable != "NO" {
		t.Fatalf("expected event_urls.canonical_domain to be NOT NULL, got is_nullable=%q", canonicalDomainNullable)
	}

	var canonicalDomainIndexExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('idx_event_urls_canonical_domain_created_at') IS NOT NULL`).Scan(&canonicalDomainIndexExists); err != nil {
		t.Fatalf("check canonical domain index: %v", err)
	}
	if !canonicalDomainIndexExists {
		t.Fatal("expected canonical domain index to exist")
	}
}

func TestCanonicalDomainMigrationBackfillsExistingAliases(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	if _, err := pool.Exec(ctx, `
		ALTER TABLE event_urls ALTER COLUMN canonical_domain DROP NOT NULL;
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES
			('migration_alias_1', 'author_a', 1, 1, 'sig', '', '{}'),
			('migration_alias_2', 'author_b', 2, 1, 'sig', '', '{}');
		INSERT INTO event_urls (
			event_id, author_pubkey, created_at, url, domain, canonical_domain, derivation_version
		)
		VALUES
			('migration_alias_1', 'author_a', 1, 'https://youtu.be/a', 'youtu.be', NULL, 1),
			('migration_alias_2', 'author_b', 2, 'https://www.example.com/b', 'www.example.com', NULL, 1);
	`); err != nil {
		t.Fatalf("seed pre-canonical event URLs: %v", err)
	}

	migrationSQL, err := migrations.Files.ReadFile("000060_event_urls_canonical_domain.sql")
	if err != nil {
		t.Fatalf("read canonical-domain migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
		t.Fatalf("rerun canonical-domain migration: %v", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT canonical_domain
		FROM event_urls
		WHERE event_id LIKE 'migration_alias_%'
		ORDER BY event_id
	`)
	if err != nil {
		t.Fatalf("query canonical-domain backfill: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var domain string
		if err := rows.Scan(&domain); err != nil {
			t.Fatalf("scan canonical-domain backfill: %v", err)
		}
		got = append(got, domain)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read canonical-domain backfill: %v", err)
	}
	want := []string{"youtube.com", "example.com"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected canonical-domain backfill: got=%v want=%v", got, want)
	}
}

func TestMigrateDetectsChecksumDrift(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)

	pool := setupSchemaPool(t, ctx, dbURL)

	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

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

	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

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

func TestMigratePgTrgmExtensionIsPublicAcrossSchemas(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)

	firstPool := dbtest.SetupSchemaPool(t, ctx, dbURL, "migrate_trgm_first")
	if err := Migrate(ctx, firstPool, "test-v1"); err != nil {
		t.Fatalf("first schema migrate failed: %v", err)
	}

	secondPool := dbtest.SetupSchemaPool(t, ctx, dbURL, "migrate_trgm_second")
	if err := Migrate(ctx, secondPool, "test-v1"); err != nil {
		t.Fatalf("second schema migrate failed: %v", err)
	}

	var extensionSchema string
	if err := secondPool.QueryRow(ctx, `
		SELECT ns.nspname
		FROM pg_extension ext
		JOIN pg_namespace ns ON ns.oid = ext.extnamespace
		WHERE ext.extname = 'pg_trgm'
	`).Scan(&extensionSchema); err != nil {
		t.Fatalf("query pg_trgm schema: %v", err)
	}
	if extensionSchema != "public" {
		t.Fatalf("expected pg_trgm extension schema to be public, got %q", extensionSchema)
	}

	var trigramComparable bool
	if err := secondPool.QueryRow(ctx, `SELECT 'nostr'::text % 'nostrmash'::text`).Scan(&trigramComparable); err != nil {
		t.Fatalf("evaluate trigram operator from second schema: %v", err)
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
