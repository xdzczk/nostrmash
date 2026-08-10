package trust

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// trustHeavyStatementTimeout covers BFS snapshot rebuild + trusted-discovery
// projection refresh. Those statements scan follower_edges / large candidate
// tables and legitimately exceed the production 15s API guardrail.
const trustHeavyStatementTimeout = 30 * time.Minute

func setTrustHeavyStatementTimeout(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, fmt.Sprintf(
		"SET LOCAL statement_timeout = %d",
		trustHeavyStatementTimeout.Milliseconds(),
	))
	if err != nil {
		return fmt.Errorf("set trust statement_timeout: %w", err)
	}
	return nil
}

type TrustGraphSnapshotRefreshResult struct {
	RowsUpserted int
	SourceRunID  *int64
}

func (s *Trust) RefreshTrustGraphSnapshot(ctx context.Context, maxHops int) (TrustGraphSnapshotRefreshResult, error) {
	if s == nil || s.pool == nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("store is not initialized")
	}
	if maxHops <= 0 {
		maxHops = 3
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("begin trust graph snapshot refresh: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setTrustHeavyStatementTimeout(ctx, tx); err != nil {
		return TrustGraphSnapshotRefreshResult{}, err
	}

	var sourceRunID *int64
	if err := tx.QueryRow(ctx, `
		SELECT id
		FROM trust_runs
		WHERE status = 'succeeded'
		ORDER BY finished_at DESC NULLS LAST, id DESC
		LIMIT 1
	`).Scan(&sourceRunID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("select latest succeeded trust run: %w", err)
		}
		sourceRunID = nil
	}

	// DELETE, not TRUNCATE: TRUNCATE takes an AccessExclusiveLock that is held
	// until commit, blocking every trust_graph_snapshot reader (ingest gate,
	// discovery queries) for the whole rebuild. DELETE takes only a row lock;
	// under MVCC concurrent readers keep seeing the previous snapshot until
	// this transaction commits. The table is small enough (one row per
	// reachable pubkey) that the vacuum churn is negligible.
	if _, err := tx.Exec(ctx, `DELETE FROM trust_graph_snapshot`); err != nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("clear trust graph snapshot: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		WITH RECURSIVE reachable(pubkey, hops) AS (
			SELECT pubkey, 0::integer AS hops
			FROM trust_seeds
			WHERE is_active = true
			UNION
			SELECT fe.followed_pubkey, reachable.hops + 1
			FROM reachable
			INNER JOIN follower_edges fe ON fe.follower_pubkey = reachable.pubkey
			WHERE reachable.hops < $1
		),
		agg AS (
			SELECT
				pubkey,
				MIN(hops)::integer AS min_hops,
				BOOL_OR(hops = 0) AS is_seed
			FROM reachable
			GROUP BY pubkey
		)
		INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed, source_run_id, refreshed_at)
		SELECT pubkey, min_hops, is_seed, $2, now()
		FROM agg
	`, maxHops, sourceRunID)
	if err != nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("rebuild trust graph snapshot: %w", err)
	}

	// Keep hop+score denormalization atomic with the snapshot rebuild so
	// GetTrustStates readers never observe a new hop set without matching
	// trust_pubkeys_latest rows.
	if err := RefreshTrustPubkeysLatestTx(ctx, tx); err != nil {
		return TrustGraphSnapshotRefreshResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("commit trust graph snapshot refresh: %w", err)
	}

	// The discovery projections refresh in their own transactions, after the
	// snapshot has committed. They bulk-delete and upsert candidate tables
	// that can take a long time on large datasets; folding them into the
	// snapshot transaction meant one multi-hour transaction whose locks
	// blocked the retention pipeline (observed in production). If a
	// projection refresh fails, the snapshot is already live and the next
	// loop iteration retries the projections.
	if err := s.runProjectionRefresh(ctx, "trusted note discovery", refreshTrustedNoteDiscoveryProjectionTx); err != nil {
		return TrustGraphSnapshotRefreshResult{}, err
	}
	if err := s.runProjectionRefresh(ctx, "trusted profile discovery", refreshTrustedProfileDiscoveryProjectionTx); err != nil {
		return TrustGraphSnapshotRefreshResult{}, err
	}

	return TrustGraphSnapshotRefreshResult{
		RowsUpserted: int(tag.RowsAffected()),
		SourceRunID:  sourceRunID,
	}, nil
}

// runProjectionRefresh wraps one trusted-discovery projection rebuild in its
// own transaction so its runtime and locks never extend the snapshot rebuild
// transaction.
func (s *Trust) runProjectionRefresh(
	ctx context.Context,
	name string,
	refresh func(context.Context, pgx.Tx, int) error,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s projection refresh: %w", name, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setTrustHeavyStatementTimeout(ctx, tx); err != nil {
		return err
	}
	if err := refresh(ctx, tx, 1); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s projection refresh: %w", name, err)
	}
	return nil
}
