// Package derivationbootstrap seeds derivation version metadata for integration tests.
// Production binaries register derivations at worker/ingestor startup; tests that only call
// store/dbmigrate.Migrate must also run derivation.EnsureRegisteredDerivations.
package derivationbootstrap

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/dbmigrate"
	"github.com/xdzczk/nostrmash/internal/derivation"
)

// MustMigrate runs schema migrations then registers all derivations (same as worker/ingestor startup).
func MustMigrate(t testing.TB, ctx context.Context, pool *pgxpool.Pool, appVersion string) {
	t.Helper()
	if err := dbmigrate.Migrate(ctx, pool, appVersion); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	MustEnsureRegisteredDerivations(t, ctx, pool)
}

// MustEnsureRegisteredDerivations reconciles derivation_versions / derivation_active_versions.
func MustEnsureRegisteredDerivations(t testing.TB, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if err := derivation.EnsureRegisteredDerivations(ctx, pool); err != nil {
		t.Fatalf("ensure registered derivations: %v", err)
	}
}
