package store

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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/migrations"
)

const bootstrapAuditSQL = `
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

	// Serialize migration execution across concurrent process startups.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	if _, err := tx.Exec(ctx, bootstrapAuditSQL); err != nil {
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

// Ping checks database connectivity.
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	return pool.Ping(ctx)
}
