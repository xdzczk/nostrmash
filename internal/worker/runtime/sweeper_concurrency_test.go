package runtime

import (
	"testing"

	"github.com/xdzczk/nostrmash/internal/config"
)

func TestFitWorkerSweeperConcurrency_LeavesRoomWhenPoolIsLarge(t *testing.T) {
	t.Parallel()
	cfg := sweeperLoadConfig(8, 8, 2, 12, 16, 1)
	got := fitWorkerSweeperConcurrency(cfg, 100)
	if got.AuthorAnalyticsSweeper.Concurrency != 12 || got.ProfileStatsSweeper.Concurrency != 16 || got.MeilisearchSweeper.Concurrency != 1 {
		t.Fatalf("expected configured concurrency to stand, got author %d profile %d meili %d",
			got.AuthorAnalyticsSweeper.Concurrency, got.ProfileStatsSweeper.Concurrency, got.MeilisearchSweeper.Concurrency)
	}
}

func TestFitWorkerSweeperConcurrency_ShrinksToThePool(t *testing.T) {
	t.Parallel()
	// Production shape: bundle workers already use 18 of 24 connections,
	// and the sweeper env asks for 29 more.
	cfg := sweeperLoadConfig(6, 6, 6, 12, 16, 1)
	got := fitWorkerSweeperConcurrency(cfg, 24)
	if got.MeilisearchSweeper.Concurrency != 1 {
		t.Fatalf("meili concurrency = %d, want 1", got.MeilisearchSweeper.Concurrency)
	}
	if got.AuthorAnalyticsSweeper.Concurrency != 2 || got.ProfileStatsSweeper.Concurrency != 2 {
		t.Fatalf("heavy sweepers = author %d profile %d, want 2 and 2",
			got.AuthorAnalyticsSweeper.Concurrency, got.ProfileStatsSweeper.Concurrency)
	}
	used := 6 + 6 + 6 + got.AuthorAnalyticsSweeper.Concurrency + got.ProfileStatsSweeper.Concurrency + got.MeilisearchSweeper.Concurrency + 1
	if used > 24 {
		t.Fatalf("bundle + sweepers + spare = %d, exceeds pool 24", used)
	}
}

func TestFitWorkerSweeperConcurrency_ZeroPoolIsUnchanged(t *testing.T) {
	t.Parallel()
	cfg := sweeperLoadConfig(6, 6, 6, 12, 16, 1)
	got := fitWorkerSweeperConcurrency(cfg, 0)
	if got.AuthorAnalyticsSweeper.Concurrency != 12 {
		t.Fatalf("author concurrency = %d, want 12", got.AuthorAnalyticsSweeper.Concurrency)
	}
}

func sweeperLoadConfig(bundle, live, backfill, author, profile, meili int) config.WorkerConfig {
	return config.WorkerConfig{
		Concurrency:         bundle,
		LiveConcurrency:     live,
		BackfillConcurrency: backfill,
		AuthorAnalyticsSweeper: config.WorkerAuthorAnalyticsSweeperConfig{
			Enabled:     true,
			Concurrency: author,
		},
		ProfileStatsSweeper: config.WorkerProfileStatsSweeperConfig{
			Enabled:     true,
			Concurrency: profile,
		},
		MeilisearchSweeper: config.WorkerMeilisearchSweeperConfig{
			Enabled:     true,
			Concurrency: meili,
		},
	}
}
