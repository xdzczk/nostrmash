package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/dbmigrate"
)

// Migrate applies embedded SQL migrations in lexical order and records audit rows.
func Migrate(ctx context.Context, pool *pgxpool.Pool, appVersion string) error {
	return dbmigrate.Migrate(ctx, pool, appVersion)
}

// Ping checks database connectivity.
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	return pool.Ping(ctx)
}
