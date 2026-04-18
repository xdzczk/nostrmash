package store

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/derivation"
)

func mustMigrateAndSeedDerivations(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appVersion string) {
	t.Helper()
	if err := Migrate(ctx, pool, appVersion); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := derivation.EnsureRegisteredDerivations(ctx, pool); err != nil {
		t.Fatalf("ensure registered derivations: %v", err)
	}
}
