package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

// Stored discovery scores are only recomputed when an event dirty-marks the
// pubkey, so the re-score enqueue is what propagates formula changes and
// freshness decay to quiet profiles. It must target exactly the rows that
// can still be served: stale-scored AND active within the 7d serving window.
func TestEnqueueStaleProfileDiscoveryRescores(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	insert := func(pubkey string, activityAt time.Time, scoredAt time.Time) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO profile_discovery_stats (
				pubkey, score_24h, score_7d, rising_score_24h, rising_score_7d,
				recent_post_count, recent_reply_count, recent_engagement_received,
				recent_zap_volume_msats, recent_active_days, recent_activity_at,
				last_scored_at, derivation_version
			) VALUES ($1, 1, 1, 1, 1, 1, 0, 2, 0, 1, $2, $3, 1)
		`, pubkey, activityAt.Unix(), scoredAt); err != nil {
			t.Fatalf("insert profile_discovery_stats for %s: %v", pubkey, err)
		}
	}

	// Stale score, still inside the 7d serving window: must be enqueued.
	insert("stale_in_window", now.Add(-24*time.Hour), now.Add(-3*time.Hour))
	// Stale score but outside the serving window: can never be served, so
	// re-scoring is wasted work and it must be skipped.
	insert("stale_out_of_window", now.Add(-8*24*time.Hour), now.Add(-8*24*time.Hour))
	// Freshly scored: nothing to do.
	insert("fresh", now.Add(-1*time.Hour), now.Add(-10*time.Minute))

	enqueued, err := handlers.EnqueueStaleProfileDiscoveryRescores(ctx, 2*time.Hour, 100)
	if err != nil {
		t.Fatalf("EnqueueStaleProfileDiscoveryRescores: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("expected exactly the stale in-window profile to be enqueued, got %d", enqueued)
	}
	var pending string
	if err := pool.QueryRow(ctx, `
		SELECT pubkey FROM pending_profile_stats_recomputes
	`).Scan(&pending); err != nil {
		t.Fatalf("read pending queue: %v", err)
	}
	if pending != "stale_in_window" {
		t.Fatalf("expected stale_in_window in the pending queue, got %q", pending)
	}

	// Re-running must not bump marked_at for the already-pending row
	// (ON CONFLICT DO NOTHING) and must report zero new rows.
	var markedBefore time.Time
	if err := pool.QueryRow(ctx, `
		SELECT marked_at FROM pending_profile_stats_recomputes WHERE pubkey = 'stale_in_window'
	`).Scan(&markedBefore); err != nil {
		t.Fatalf("read marked_at: %v", err)
	}
	again, err := handlers.EnqueueStaleProfileDiscoveryRescores(ctx, 2*time.Hour, 100)
	if err != nil {
		t.Fatalf("second EnqueueStaleProfileDiscoveryRescores: %v", err)
	}
	if again != 0 {
		t.Fatalf("expected no new enqueues on second pass, got %d", again)
	}
	var markedAfter time.Time
	if err := pool.QueryRow(ctx, `
		SELECT marked_at FROM pending_profile_stats_recomputes WHERE pubkey = 'stale_in_window'
	`).Scan(&markedAfter); err != nil {
		t.Fatalf("read marked_at after second pass: %v", err)
	}
	if !markedAfter.Equal(markedBefore) {
		t.Fatalf("marked_at must not change on conflict: before=%v after=%v", markedBefore, markedAfter)
	}

	// Draining the queue re-scores with the current code. This synthetic
	// row has no backing events at all, so the recompute must remove it —
	// exactly what a re-score should do to a stale phantom entry whose
	// stored score no longer reflects any derivable activity.
	drainPendingProfileStatsForTest(t, ctx, handlers)
	var remaining bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM profile_discovery_stats WHERE pubkey = 'stale_in_window')
	`).Scan(&remaining); err != nil {
		t.Fatalf("check re-scored row: %v", err)
	}
	if remaining {
		t.Fatalf("expected the re-score to remove a row with no derivable activity")
	}
}
