package store

import (
	"context"
	"testing"

	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

func TestCollectStorageStats_UsesEstimatedOrExactRowCounts(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "storage_stats_modes"), "storage_stats_modes")

	if _, err := pool.Exec(ctx, `
		CREATE TABLE stats_small (
			id BIGSERIAL PRIMARY KEY,
			v  TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create stats_small: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO stats_small (v) VALUES ('a'),('b'),('c')
	`); err != nil {
		t.Fatalf("seed stats_small: %v", err)
	}

	estimated, err := CollectStorageStats(ctx, pool, []string{"stats_small"}, StorageStatsOptions{
		ExactRowCountMaxBytes: 0,
	})
	if err != nil {
		t.Fatalf("collect estimated storage stats: %v", err)
	}
	if len(estimated.Tables) != 1 {
		t.Fatalf("expected one table in estimated stats, got %d", len(estimated.Tables))
	}
	if estimated.Tables[0].RowCountMode != "estimated" {
		t.Fatalf("expected estimated row count mode, got %q", estimated.Tables[0].RowCountMode)
	}

	exact, err := CollectStorageStats(ctx, pool, []string{"stats_small"}, StorageStatsOptions{
		ExactRowCountMaxBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("collect exact storage stats: %v", err)
	}
	if len(exact.Tables) != 1 {
		t.Fatalf("expected one table in exact stats, got %d", len(exact.Tables))
	}
	if exact.Tables[0].RowCountMode != "exact" {
		t.Fatalf("expected exact row count mode, got %q", exact.Tables[0].RowCountMode)
	}
	if exact.Tables[0].RowCount != 3 {
		t.Fatalf("expected exact row count 3, got %d", exact.Tables[0].RowCount)
	}
}

func TestCollectStorageStats_RejectsUnsafeTableNameForExactCount(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "storage_stats_ident"), "storage_stats_ident")

	_, err := CollectStorageStats(ctx, pool, []string{"bad-name"}, StorageStatsOptions{
		ExactRowCountMaxBytes: 1 << 30,
	})
	if err == nil {
		t.Fatalf("expected invalid table name error")
	}
}
