package derivation

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

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
