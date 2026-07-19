package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type TrustGraphSnapshotRefreshResult struct {
	RowsUpserted int
	SourceRunID  *int64
}

func (s *PostgresStore) RefreshTrustGraphSnapshot(ctx context.Context, maxHops int) (TrustGraphSnapshotRefreshResult, error) {
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

	if _, err := tx.Exec(ctx, `TRUNCATE trust_graph_snapshot`); err != nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("truncate trust graph snapshot: %w", err)
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
	if err := refreshTrustedNoteDiscoveryProjectionTx(ctx, tx, 1); err != nil {
		return TrustGraphSnapshotRefreshResult{}, err
	}
	if err := refreshTrustedProfileDiscoveryProjectionTx(ctx, tx, 1); err != nil {
		return TrustGraphSnapshotRefreshResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("commit trust graph snapshot refresh: %w", err)
	}
	return TrustGraphSnapshotRefreshResult{
		RowsUpserted: int(tag.RowsAffected()),
		SourceRunID:  sourceRunID,
	}, nil
}
