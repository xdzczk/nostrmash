package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// PurgeExpiredNIP40Events deletes a bounded batch of events whose NIP-40
// expiration is older than expiresBefore. The candidate set is the partial
// index on event_attributes.expires_at, so an empty backlog is an index probe.
func (s *Retention) PurgeExpiredNIP40Events(ctx context.Context, expiresBefore time.Time, limit int) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}
	if expiresBefore.IsZero() {
		return 0, fmt.Errorf("expiresBefore is required")
	}
	started := time.Now()
	var rows int64
	err := s.guardedWithTx(ctx, func(tx pgx.Tx, q *retentiondb.Queries) error {
		ids, err := selectExpiredNIP40(ctx, tx, expiresBefore.UTC().Unix(), limit)
		if err != nil {
			return err
		}
		rows, err = s.reverseAndDeleteTx(ctx, tx, q, ids)
		return err
	})
	metrics.ObserveDBOperation("purge_expired_nip40_events", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge expired nip40 events: %w", err)
	}
	return rows, nil
}

func selectExpiredNIP40(ctx context.Context, tx pgx.Tx, expiresBefore int64, limit int) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT event_id
		FROM event_attributes
		WHERE expires_at IS NOT NULL
		  AND expires_at < $1
		ORDER BY expires_at ASC, event_id ASC
		LIMIT $2
	`, expiresBefore, limit)
	if err != nil {
		return nil, fmt.Errorf("select expired nip40 events: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
