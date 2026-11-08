package derivation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

// projectionlessPendingBatch is how many pending pubkeys one purge
// statement locks. The delete then keeps any pubkey that still has
// something to aggregate. Cost is one index probe per locked pubkey,
// not a scan of events.
const projectionlessPendingBatch = 5000

// sweeperActiveQueryLimit is the number of other active backends at which
// sweeper batches stand down. The derive bundle and API queries share the
// same Postgres; a hot pending queue must not pile more aggregates on top
// of an already saturated server.
const sweeperActiveQueryLimit = 32

func (h *Handlers) databaseSaturated(ctx context.Context) (bool, error) {
	if h == nil || h.pool == nil {
		return false, fmt.Errorf("handlers are not initialized")
	}
	var active int
	if err := h.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND state = 'active'
		  AND pid <> pg_backend_pid()
		  AND (
		    current_setting('application_name') = ''
		    OR application_name IS DISTINCT FROM current_setting('application_name')
		  )
	`).Scan(&active); err != nil {
		return false, fmt.Errorf("count active queries: %w", err)
	}
	return active >= sweeperActiveQueryLimit, nil
}

func setSweeperStatementTimeout(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '45s'`); err != nil {
		return fmt.Errorf("set sweeper statement timeout: %w", err)
	}
	return nil
}

// pubkeyNeedsProjectionRebuild is false for a pubkey whose only stored
// events are kind-1059 gift wraps. Those keys are ephemeral and have no
// notes, reactions, reposts, or zaps for the sweepers to aggregate.
// Anyone who authored something else, or who is the target or source of
// a reply, reaction, repost, or zap, still rebuilds.
func pubkeyNeedsProjectionRebuild(ctx context.Context, tx pgx.Tx, pubkey string) (bool, error) {
	var needed bool
	if err := tx.QueryRow(ctx, `
		SELECT
			EXISTS (
				SELECT 1 FROM events
				WHERE pubkey = $1 AND kind <> 1059
			)
			OR EXISTS (
				SELECT 1 FROM reaction_events
				WHERE target_pubkey = $1 OR reactor_pubkey = $1
			)
			OR EXISTS (
				SELECT 1 FROM repost_events
				WHERE target_pubkey = $1 OR reposter_pubkey = $1
			)
			OR EXISTS (
				SELECT 1 FROM zap_receipts
				WHERE receiver_pubkey = $1 OR sender_pubkey = $1
			)
			OR EXISTS (
				SELECT 1 FROM reply_count_contributions
				WHERE target_pubkey = $1 OR source_pubkey = $1
			)
	`, pubkey).Scan(&needed); err != nil {
		return false, fmt.Errorf("check projection inputs for %s: %w", pubkey, err)
	}
	return needed, nil
}

func (h *Handlers) pendingRecomputeBacklog(ctx context.Context, query string) (int64, float64, error) {
	if h == nil || h.pool == nil {
		return 0, 0, fmt.Errorf("handlers are not initialized")
	}
	var count int64
	var oldest *time.Time
	if err := h.pool.QueryRow(ctx, query).Scan(&count, &oldest); err != nil {
		return 0, 0, fmt.Errorf("read pending recompute backlog: %w", err)
	}
	if oldest == nil {
		return count, 0, nil
	}
	age := time.Since(oldest.UTC()).Seconds()
	if age < 0 {
		age = 0
	}
	return count, age, nil
}

// PurgeProjectionlessPending deletes a bounded batch of pending
// author-analytics and profile-stats pubkeys that have nothing to
// aggregate: no event other than a kind-1059 gift wrap, and no reply,
// reaction, repost, or zap that names them. Claimed rows are left for
// the sweeper that holds them. Returns how many rows were deleted.
// A zero result means the oldest page is all real authors, so the
// caller should stop and let the per-pubkey rebuild take over.
func (h *Handlers) PurgeProjectionlessPending(ctx context.Context, limit int) (int64, error) {
	if h == nil || h.pool == nil {
		return 0, fmt.Errorf("handlers are not initialized")
	}
	if limit <= 0 {
		limit = projectionlessPendingBatch
	}
	started := time.Now()
	author, err := h.purgeProjectionlessPendingTable(ctx, limit, purgeProjectionlessAuthorAnalytics)
	if err != nil {
		metrics.ObserveDBOperation("purge_projectionless_pending", "error", time.Since(started))
		return author, err
	}
	profile, err := h.purgeProjectionlessPendingTable(ctx, limit, purgeProjectionlessProfileStats)
	metrics.ObserveDBOperation("purge_projectionless_pending", dbResultLabel(err), time.Since(started))
	return author + profile, err
}

func dbResultLabel(err error) string {
	if err != nil && !errors.Is(err, ErrBackgroundBusy) {
		return "error"
	}
	return "ok"
}

const purgeProjectionlessAuthorAnalytics = `
WITH page AS MATERIALIZED (
	SELECT pubkey
	FROM pending_author_analytics_recomputes
	WHERE claimed_at IS NULL
	ORDER BY marked_at ASC, pubkey ASC
	LIMIT $1
	FOR UPDATE SKIP LOCKED
)
DELETE FROM pending_author_analytics_recomputes p
USING page
WHERE p.pubkey = page.pubkey
  AND NOT ` + projectionlessPubkeyPredicate

const purgeProjectionlessProfileStats = `
WITH page AS MATERIALIZED (
	SELECT pubkey
	FROM pending_profile_stats_recomputes
	WHERE claimed_at IS NULL
	ORDER BY marked_at ASC, pubkey ASC
	LIMIT $1
	FOR UPDATE SKIP LOCKED
)
DELETE FROM pending_profile_stats_recomputes p
USING page
WHERE p.pubkey = page.pubkey
  AND NOT ` + projectionlessPubkeyPredicate

// projectionlessPubkeyPredicate is the inverse of
// pubkeyNeedsProjectionRebuild. Each branch is an index lookup on the
// pubkey (events by pubkey+kind, and the engagement target/source
// indexes). The page CTE bounds how many pubkeys are probed.
const projectionlessPubkeyPredicate = `(
	EXISTS (
		SELECT 1 FROM events e
		WHERE e.pubkey = p.pubkey AND e.kind <> 1059
	)
	OR EXISTS (
		SELECT 1 FROM reaction_events r
		WHERE r.target_pubkey = p.pubkey OR r.reactor_pubkey = p.pubkey
	)
	OR EXISTS (
		SELECT 1 FROM repost_events r
		WHERE r.target_pubkey = p.pubkey OR r.reposter_pubkey = p.pubkey
	)
	OR EXISTS (
		SELECT 1 FROM zap_receipts z
		WHERE z.receiver_pubkey = p.pubkey OR z.sender_pubkey = p.pubkey
	)
	OR EXISTS (
		SELECT 1 FROM reply_count_contributions c
		WHERE c.target_pubkey = p.pubkey OR c.source_pubkey = p.pubkey
	)
)`

func (h *Handlers) purgeProjectionlessPendingTable(ctx context.Context, limit int, query string) (int64, error) {
	tag, err := h.pool.Exec(ctx, query, limit)
	if err != nil {
		return 0, fmt.Errorf("purge projectionless pending: %w", err)
	}
	return tag.RowsAffected(), nil
}
