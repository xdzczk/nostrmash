package trust

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type TrustPubkeyLatest = readmodel.TrustPubkeyLatest

// RefreshTrustPubkeysLatestTx rebuilds trust_pubkeys_latest inside an existing
// transaction. Used by trust promote so score publication and denormalization
// commit together. Callers must already hold a consistent view of
// trust_graph_snapshot and trust_scores_global in that transaction.
func RefreshTrustPubkeysLatestTx(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return fmt.Errorf("refresh trust_pubkeys_latest: tx is nil")
	}
	if _, err := tx.Exec(ctx, `DELETE FROM trust_pubkeys_latest`); err != nil {
		return fmt.Errorf("clear trust_pubkeys_latest: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trust_pubkeys_latest (
			pubkey,
			min_hops,
			is_seed,
			score,
			rank,
			source_run_id,
			computed_at,
			updated_at
		)
		SELECT
			COALESCE(snapshot.pubkey, scores.pubkey) AS pubkey,
			snapshot.min_hops,
			COALESCE(snapshot.is_seed, false) AS is_seed,
			scores.score,
			scores.rank,
			COALESCE(snapshot.source_run_id, scores.run_id) AS source_run_id,
			COALESCE(scores.computed_at, snapshot.refreshed_at) AS computed_at,
			now() AS updated_at
		FROM trust_graph_snapshot snapshot
		FULL OUTER JOIN trust_scores_global scores ON scores.pubkey = snapshot.pubkey
	`); err != nil {
		return fmt.Errorf("rebuild trust_pubkeys_latest: %w", err)
	}
	return nil
}

// RefreshTrustPubkeysLatest rebuilds trust_pubkeys_latest in its own transaction.
func (s *Trust) RefreshTrustPubkeysLatest(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin trust_pubkeys_latest refresh: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setTrustHeavyStatementTimeout(ctx, tx); err != nil {
		return err
	}
	if err := RefreshTrustPubkeysLatestTx(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust_pubkeys_latest refresh: %w", err)
	}
	return nil
}

func (s *Trust) GetTrustPubkeyLatest(ctx context.Context, pubkey string) (TrustPubkeyLatest, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return TrustPubkeyLatest{}, fmt.Errorf("pubkey is required")
	}
	rows, err := s.GetTrustPubkeysLatest(ctx, []string{pubkey})
	if err != nil {
		return TrustPubkeyLatest{}, err
	}
	row, ok := rows[pubkey]
	if !ok {
		return TrustPubkeyLatest{}, readmodel.ErrNotFound
	}
	return row, nil
}

func (s *Trust) GetTrustPubkeysLatest(ctx context.Context, pubkeys []string) (map[string]TrustPubkeyLatest, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	normalized := normalizePubkeys(pubkeys)
	if len(normalized) == 0 {
		return map[string]TrustPubkeyLatest{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT
			pubkey,
			min_hops,
			is_seed,
			score,
			rank,
			source_run_id,
			computed_at,
			updated_at
		FROM trust_pubkeys_latest
		WHERE pubkey = ANY($1)
	`, normalized)
	if err != nil {
		return nil, fmt.Errorf("get trust_pubkeys_latest: %w", err)
	}
	defer rows.Close()

	out := make(map[string]TrustPubkeyLatest, len(normalized))
	for rows.Next() {
		var item TrustPubkeyLatest
		if err := rows.Scan(
			&item.Pubkey,
			&item.MinHops,
			&item.IsSeed,
			&item.Score,
			&item.Rank,
			&item.SourceRunID,
			&item.ComputedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan trust_pubkeys_latest: %w", err)
		}
		item.UpdatedAt = item.UpdatedAt.UTC()
		if item.ComputedAt != nil {
			ts := item.ComputedAt.UTC()
			item.ComputedAt = &ts
		}
		out[item.Pubkey] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trust_pubkeys_latest: %w", err)
	}
	return out, nil
}

// CountRankedPubkeys returns how many pubkeys currently have a global trust
// rank. Used as the percentile denominator for profile trust summaries.
func (s *Trust) CountRankedPubkeys(ctx context.Context) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	var count int64
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM trust_pubkeys_latest
		WHERE rank IS NOT NULL
	`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count ranked trust pubkeys: %w", err)
	}
	return count, nil
}
