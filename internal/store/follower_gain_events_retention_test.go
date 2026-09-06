package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertFollowerGainEventRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, followed, follower string, gainedAt int64, createdAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO follower_gain_events (followed_pubkey, follower_pubkey, gained_at, derivation_version, created_at)
		VALUES ($1, $2, $3, 1, $4)
	`, followed, follower, gainedAt, createdAt.UTC()); err != nil {
		t.Fatalf("insert follower gain event %s<-%s: %v", followed, follower, err)
	}
}

// TestPruneExpiredFollowerGainEvents verifies the retention prune keys off
// the row's insert time (created_at), not the event-supplied gained_at: a
// post-dated gained_at must not let a row outlive the horizon, and a fresh
// row must survive even if its gained_at is old.
func TestPruneExpiredFollowerGainEvents(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	createdBefore := ref.Add(-8 * 24 * time.Hour)
	ancient := createdBefore.Add(-time.Hour)

	// Aged past the horizon: pruned, even though its gained_at claims to be
	// far in the future.
	insertFollowerGainEventRow(t, ctx, pool, "pk_a", "pk_old", ref.Add(365*24*time.Hour).Unix(), ancient)
	// Aged past the horizon with a normal gained_at: pruned.
	insertFollowerGainEventRow(t, ctx, pool, "pk_a", "pk_old2", ancient.Unix(), ancient)
	// Inserted recently: survives regardless of an old gained_at.
	insertFollowerGainEventRow(t, ctx, pool, "pk_a", "pk_fresh", ancient.Unix(), ref)

	deleted, err := NewPostgresStore(pool).PruneExpiredFollowerGainEvents(ctx, createdBefore, 100)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deletions, got %d", deleted)
	}

	var remaining string
	if err := pool.QueryRow(ctx, `SELECT follower_pubkey FROM follower_gain_events`).Scan(&remaining); err != nil {
		t.Fatalf("query remaining rows: %v", err)
	}
	if remaining != "pk_fresh" {
		t.Fatalf("expected only pk_fresh to survive, got %s", remaining)
	}
}
