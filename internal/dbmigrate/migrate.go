// Package dbmigrate applies embedded SQL schema migrations.
//
// It is intentionally separate from internal/store so store subpackages
// (account/retention/trust/read) and their tests can migrate a database without
// creating an import cycle through store.PostgresStore.
package dbmigrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/migrations"
)

// BootstrapAuditSQL creates the migration audit table used by Migrate.
const BootstrapAuditSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations_audit (
    migration_id TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    app_version TEXT,
    checksum TEXT NOT NULL
);
`

const migrationAdvisoryLockID int64 = 428871060619501584

// Migrate applies embedded SQL migrations in lexical order and records audit rows.
func Migrate(ctx context.Context, pool *pgxpool.Pool, appVersion string) error {
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		return fmt.Errorf("read migrations fs: %w", err)
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

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentSchema string
	if err := tx.QueryRow(ctx, `SELECT current_schema()`).Scan(&currentSchema); err != nil {
		return fmt.Errorf("resolve current schema: %w", err)
	}

	// Serialize migration execution across concurrent process startups.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	// Keep migration DDL scoped to the active schema to avoid collisions with
	// same-named relations in public when integration tests run against a shared DB.
	if err := SetMigrationSearchPath(ctx, tx, currentSchema, false); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, BootstrapAuditSQL); err != nil {
		return fmt.Errorf("bootstrap migration audit: %w", err)
	}

	for _, name := range names {
		fullID := path.Join("migrations", name)
		data, err := migrations.Files.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		sum := sha256.Sum256(data)
		checksum := hex.EncodeToString(sum[:])

		var appliedChecksum string
		err = tx.QueryRow(ctx,
			`SELECT checksum FROM schema_migrations_audit WHERE migration_id = $1`,
			fullID,
		).Scan(&appliedChecksum)
		if err == nil {
			if appliedChecksum != checksum {
				return fmt.Errorf("migration checksum mismatch for %s", name)
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("check applied %s: %w", name, err)
		}

		includePublic := name == "000016_search_hardening.sql"
		if err := SetMigrationSearchPath(ctx, tx, currentSchema, includePublic); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, string(data)); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO schema_migrations_audit (migration_id, app_version, checksum)
			VALUES ($1, $2, $3)
			ON CONFLICT (migration_id) DO UPDATE
			SET app_version = EXCLUDED.app_version,
				checksum = EXCLUDED.checksum`,
			fullID, appVersion, checksum,
		)
		if err != nil {
			return fmt.Errorf("audit %s: %w", name, err)
		}
	}

	return tx.Commit(ctx)
}

// Pending returns the embedded migration files not yet recorded in the audit
// table, in apply order. A missing audit table means every migration is
// pending. Checksum mismatches on already-applied migrations are reported as
// errors so a waiting process fails loudly instead of serving against a
// diverged schema.
func Pending(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations fs: %w", err)
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

	var auditExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = 'schema_migrations_audit'
		)
	`).Scan(&auditExists)
	if err != nil {
		return nil, fmt.Errorf("check audit table: %w", err)
	}
	if !auditExists {
		return names, nil
	}

	applied := make(map[string]string, len(names))
	rows, err := pool.Query(ctx, `SELECT migration_id, checksum FROM schema_migrations_audit`)
	if err != nil {
		return nil, fmt.Errorf("load applied migrations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, checksum string
		if err := rows.Scan(&id, &checksum); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[id] = checksum
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}

	var pending []string
	for _, name := range names {
		fullID := path.Join("migrations", name)
		appliedChecksum, ok := applied[fullID]
		if !ok {
			pending = append(pending, name)
			continue
		}
		data, err := migrations.Files.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		sum := sha256.Sum256(data)
		if appliedChecksum != hex.EncodeToString(sum[:]) {
			return nil, fmt.Errorf("migration checksum mismatch for %s", name)
		}
	}
	return pending, nil
}

// WaitUntilApplied blocks until every embedded migration is recorded as
// applied, polling at pollInterval. It exists for processes booting with
// MIGRATE_ON_BOOT=false: they must not serve against a stale schema, but they
// also must not run migrations themselves (a separate migrate step owns
// that). Returns the context's error on cancellation/timeout, annotated with
// the migrations still pending so operators see what the process is stuck on.
func WaitUntilApplied(ctx context.Context, pool *pgxpool.Pool, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	var lastPending []string
	for {
		pending, err := Pending(ctx, pool)
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			return nil
		}
		lastPending = pending
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"schema not at head after wait (%d migrations pending, first %s): %w",
				len(lastPending), lastPending[0], ctx.Err(),
			)
		case <-time.After(pollInterval):
		}
	}
}

// SetMigrationSearchPath scopes migration DDL to schemaName, optionally also
// including public (needed for extensions created by specific migrations).
func SetMigrationSearchPath(ctx context.Context, tx pgx.Tx, schemaName string, includePublic bool) error {
	schema := pgx.Identifier{schemaName}.Sanitize()
	searchPath := schema
	if includePublic {
		searchPath = searchPath + ", public"
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`SET LOCAL search_path = %s`, searchPath)); err != nil {
		return fmt.Errorf("set migration search_path: %w", err)
	}
	return nil
}
