package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/dbmigrate"
)

// Migrate applies embedded SQL migrations in lexical order and records audit rows.
func Migrate(ctx context.Context, pool *pgxpool.Pool, appVersion string) error {
	return dbmigrate.Migrate(ctx, pool, appVersion)
}

// schemaWaitTimeout bounds how long a process booting with
// MIGRATE_ON_BOOT=false waits for a separate migrate step to bring the schema
// to head before giving up with a descriptive error.
const schemaWaitTimeout = 5 * time.Minute

// EnsureSchemaReady brings the process to a servable schema state. With
// migrateOnBoot it applies pending migrations directly (historical behavior);
// without it, it only waits for the schema to reach head — production deploys
// run the one-shot cmd/migrate binary first so a slow migration can never
// wedge a health-checked serving container (see migration 000086's
// postmortem).
func EnsureSchemaReady(ctx context.Context, pool *pgxpool.Pool, appVersion string, migrateOnBoot bool) error {
	if migrateOnBoot {
		return Migrate(ctx, pool, appVersion)
	}
	waitCtx, cancel := context.WithTimeout(ctx, schemaWaitTimeout)
	defer cancel()
	return dbmigrate.WaitUntilApplied(waitCtx, pool, 2*time.Second)
}

// Ping checks database connectivity.
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	return pool.Ping(ctx)
}
