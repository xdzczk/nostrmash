package derivation

import (
	"context"
	"fmt"
	"time"
)

// RefreshPaidRankings rewrites the most-zapped note windows and the
// most-supported profile windows from zap receipts. The worker runs this
// every few minutes. Readers only scan the resulting tables.
//
// Note rank is sats received in the window on a note published in that
// same window. A zap the author sent to their own note does not count,
// and replies do not count. An older note zapped today stays off the
// 24h list. Profile rank sums each sender only up to SupportSenderCapSats,
// and ignores a self-zap.
func (h *Handlers) RefreshPaidRankings(ctx context.Context) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	now := time.Now().UTC()
	cutoff24h := now.Add(-24 * time.Hour).Unix()
	cutoff7d := now.Add(-7 * 24 * time.Hour).Unix()
	cutoff30d := now.Add(-30 * 24 * time.Hour).Unix()

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin paid rankings tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '60s'`); err != nil {
		return fmt.Errorf("set paid rankings timeout: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM note_zap_window_stats`); err != nil {
		return fmt.Errorf("clear note zap windows: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO note_zap_window_stats (
			event_id, author_pubkey, kind,
			sats_24h, zap_count_24h,
			sats_7d, zap_count_7d,
			sats_30d, zap_count_30d,
			projected_at
		)
		SELECT z.event_id,
		       e.pubkey,
		       e.kind,
		       COALESCE(SUM(z.amount_sats) FILTER (WHERE z.created_at >= $1 AND e.created_at >= $1), 0)::bigint,
		       COUNT(*) FILTER (WHERE z.created_at >= $1 AND e.created_at >= $1),
		       COALESCE(SUM(z.amount_sats) FILTER (WHERE z.created_at >= $2 AND e.created_at >= $2), 0)::bigint,
		       COUNT(*) FILTER (WHERE z.created_at >= $2 AND e.created_at >= $2),
		       COALESCE(SUM(z.amount_sats), 0)::bigint,
		       COUNT(*),
		       now()
		FROM zap_receipts z
		JOIN events e ON e.id = z.event_id
		WHERE z.created_at >= $3
		  AND e.created_at >= $3
		  AND z.amount_sats > 0
		  AND z.event_id IS NOT NULL
		  AND z.event_id <> ''
		  AND z.sender_pubkey <> e.pubkey
		  AND e.kind IN (1, 30023)
		  AND NOT EXISTS (
			SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
		  )
		GROUP BY z.event_id, e.pubkey, e.kind
		HAVING COALESCE(SUM(z.amount_sats), 0) > 0
	`, cutoff24h, cutoff7d, cutoff30d); err != nil {
		return fmt.Errorf("insert note zap windows: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM profile_support_stats`); err != nil {
		return fmt.Errorf("clear profile support stats: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO profile_support_stats (
			pubkey,
			capped_sats_24h, raw_sats_24h, supporters_24h,
			capped_sats_7d, raw_sats_7d, supporters_7d,
			capped_sats_30d, raw_sats_30d, supporters_30d,
			projected_at
		)
		SELECT pubkey,
		       COALESCE(SUM(LEAST(sats_24h, $4::numeric)) FILTER (WHERE sats_24h > 0), 0)::bigint,
		       COALESCE(SUM(sats_24h) FILTER (WHERE sats_24h > 0), 0)::bigint,
		       COUNT(*) FILTER (WHERE sats_24h > 0),
		       COALESCE(SUM(LEAST(sats_7d, $4::numeric)) FILTER (WHERE sats_7d > 0), 0)::bigint,
		       COALESCE(SUM(sats_7d) FILTER (WHERE sats_7d > 0), 0)::bigint,
		       COUNT(*) FILTER (WHERE sats_7d > 0),
		       COALESCE(SUM(LEAST(sats_30d, $4::numeric)) FILTER (WHERE sats_30d > 0), 0)::bigint,
		       COALESCE(SUM(sats_30d) FILTER (WHERE sats_30d > 0), 0)::bigint,
		       COUNT(*) FILTER (WHERE sats_30d > 0),
		       now()
		FROM (
			SELECT receiver_pubkey AS pubkey,
			       sender_pubkey,
			       COALESCE(SUM(amount_sats) FILTER (WHERE created_at >= $1), 0) AS sats_24h,
			       COALESCE(SUM(amount_sats) FILTER (WHERE created_at >= $2), 0) AS sats_7d,
			       COALESCE(SUM(amount_sats), 0) AS sats_30d
			FROM zap_receipts
			WHERE created_at >= $3
			  AND amount_sats > 0
			  AND COALESCE(receiver_pubkey, '') <> ''
			  AND sender_pubkey <> ''
			  AND sender_pubkey <> receiver_pubkey
			GROUP BY receiver_pubkey, sender_pubkey
		) AS sender_totals
		GROUP BY pubkey
		HAVING COALESCE(SUM(sats_30d), 0) > 0
	`, cutoff24h, cutoff7d, cutoff30d, SupportSenderCapSats); err != nil {
		return fmt.Errorf("insert profile support stats: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit paid rankings tx: %w", err)
	}
	return nil
}
