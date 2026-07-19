package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/migrations"
)

// TestMigrateUpgradesFromPreviousHead proves the N-1 -> N upgrade path: it
// freezes a database at the second-newest migration (as if it had been running
// the previous release), then runs Migrate to head and asserts the final schema
// is identical to a fresh full bootstrap. This complements the fresh-bootstrap
// and checksum-drift tests, which only ever exercise applying every migration
// against an empty database.
func TestMigrateUpgradesFromPreviousHead(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)

	names := sortedMigrationNames(t)
	if len(names) < 2 {
		t.Skipf("need at least 2 migrations to exercise an upgrade path, have %d", len(names))
	}
	frozen := names[:len(names)-1]

	// Pool A: frozen at N-1, then upgraded to head via Migrate.
	upgraded := setupSchemaPool(t, ctx, dbURL)
	applyMigrationsForTest(t, ctx, upgraded, frozen)

	// The frozen database must not yet know about the newest migration.
	newestID := path.Join("migrations", names[len(names)-1])
	var appliedBeforeUpgrade int
	if err := upgraded.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations_audit WHERE migration_id = $1`, newestID).Scan(&appliedBeforeUpgrade); err != nil {
		t.Fatalf("count newest migration before upgrade: %v", err)
	}
	if appliedBeforeUpgrade != 0 {
		t.Fatalf("expected newest migration %q to be absent in frozen db", newestID)
	}

	if err := Migrate(ctx, upgraded, "upgrade-test"); err != nil {
		t.Fatalf("upgrade migrate to head: %v", err)
	}

	expectedCount, err := embeddedMigrationCount()
	if err != nil {
		t.Fatalf("count embedded migrations: %v", err)
	}
	var auditCount int
	if err := upgraded.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations_audit`).Scan(&auditCount); err != nil {
		t.Fatalf("count audit rows after upgrade: %v", err)
	}
	if auditCount != expectedCount {
		t.Fatalf("expected %d audit rows after upgrade, got %d", expectedCount, auditCount)
	}

	// Re-running Migrate must be a no-op (idempotent) after upgrade.
	if err := Migrate(ctx, upgraded, "upgrade-test"); err != nil {
		t.Fatalf("re-run migrate after upgrade: %v", err)
	}

	// Pool B: fresh full bootstrap for schema equivalence comparison.
	fresh := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, fresh, "fresh-test"); err != nil {
		t.Fatalf("fresh migrate: %v", err)
	}

	upgradedTables := schemaTableSet(t, ctx, upgraded)
	freshTables := schemaTableSet(t, ctx, fresh)
	if diff := symmetricDiff(upgradedTables, freshTables); len(diff) != 0 {
		t.Fatalf("upgraded schema differs from fresh bootstrap; differing tables: %v", diff)
	}
}

func sortedMigrationNames(t *testing.T) []string {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		t.Fatalf("read migrations fs: %v", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

// applyMigrationsForTest applies the given migration files in order, mirroring
// Migrate's per-migration search_path handling and audit bookkeeping (including
// matching sha256 checksums) so a subsequent Migrate treats them as applied.
func applyMigrationsForTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, names []string) {
	t.Helper()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin frozen migration tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentSchema string
	if err := tx.QueryRow(ctx, `SELECT current_schema()`).Scan(&currentSchema); err != nil {
		t.Fatalf("resolve current schema: %v", err)
	}
	if _, err := tx.Exec(ctx, bootstrapAuditSQL); err != nil {
		t.Fatalf("bootstrap audit table: %v", err)
	}

	for _, name := range names {
		data, err := migrations.Files.ReadFile(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		includePublic := name == "000016_search_hardening.sql"
		if err := setMigrationSearchPath(ctx, tx, currentSchema, includePublic); err != nil {
			t.Fatalf("set search_path for %s: %v", name, err)
		}
		if _, err := tx.Exec(ctx, string(data)); err != nil {
			t.Fatalf("apply frozen migration %s: %v", name, err)
		}
		sum := sha256.Sum256(data)
		if _, err := tx.Exec(ctx, `
			INSERT INTO schema_migrations_audit (migration_id, app_version, checksum)
			VALUES ($1, $2, $3)
			ON CONFLICT (migration_id) DO UPDATE
			SET app_version = EXCLUDED.app_version, checksum = EXCLUDED.checksum
		`, path.Join("migrations", name), "frozen-nminus1", hex.EncodeToString(sum[:])); err != nil {
			t.Fatalf("audit frozen migration %s: %v", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit frozen migrations: %v", err)
	}
}

func schemaTableSet(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]struct{} {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = current_schema()
		  AND table_type = 'BASE TABLE'
	`)
	if err != nil {
		t.Fatalf("query schema tables: %v", err)
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		out[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read schema tables: %v", err)
	}
	return out
}

func symmetricDiff(a, b map[string]struct{}) []string {
	var diff []string
	for name := range a {
		if _, ok := b[name]; !ok {
			diff = append(diff, fmt.Sprintf("only-in-upgraded:%s", name))
		}
	}
	for name := range b {
		if _, ok := a[name]; !ok {
			diff = append(diff, fmt.Sprintf("only-in-fresh:%s", name))
		}
	}
	sort.Strings(diff)
	return diff
}
