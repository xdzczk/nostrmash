package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// BridgedPurgeResult reports one PurgeBridgedEvents batch.
type BridgedPurgeResult struct {
	// Deleted is the number of events rows removed.
	Deleted int64
	// OrphanedAuthors lists pubkeys from the batch that have no events left.
	// Their per-pubkey projections were removed in the same transaction; the
	// caller uses the list to drop their search documents.
	OrphanedAuthors []string
}

// PurgeBridgedEvents deletes a bounded batch of stored events that carry a
// NIP-48 proxy tag. The ingestor drops new bridged events, so this is the
// one-time cleanup of what was stored before that; the candidate set is the
// partial index idx_event_attributes_bridged (migration 000127), so an empty
// backlog is an index probe. Author-level projections for pubkeys left with
// no events are removed in the same transaction, so a bridged account does
// not linger in profile rankings, identity fields, or NIP-05 checks after
// its content is gone. profiles_latest cascades from the kind-0 delete.
func (s *Retention) PurgeBridgedEvents(ctx context.Context, limit int) (BridgedPurgeResult, error) {
	if s == nil || s.pool == nil {
		return BridgedPurgeResult{}, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return BridgedPurgeResult{}, fmt.Errorf("limit must be > 0")
	}
	started := time.Now()
	var result BridgedPurgeResult
	err := s.guardedWithTx(ctx, func(tx pgx.Tx, q *retentiondb.Queries) error {
		ids, err := selectBridgedEventIDs(ctx, tx, limit)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		pubkeyRows, err := tx.Query(ctx, `SELECT DISTINCT pubkey FROM events WHERE id = ANY($1)`, ids)
		if err != nil {
			return fmt.Errorf("load bridged event authors: %w", err)
		}
		pubkeys, err := pgx.CollectRows(pubkeyRows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("load bridged event authors: %w", err)
		}
		result.Deleted, err = s.reverseAndDeleteTx(ctx, tx, q, ids)
		if err != nil {
			return err
		}
		result.OrphanedAuthors, err = deleteOrphanedAuthorProjectionsTx(ctx, tx, pubkeys)
		return err
	})
	metrics.ObserveDBOperation("purge_bridged_events", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return BridgedPurgeResult{}, fmt.Errorf("purge bridged events: %w", err)
	}
	return result, nil
}

func selectBridgedEventIDs(ctx context.Context, tx pgx.Tx, limit int) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT event_id
		FROM event_attributes
		WHERE proxy_protocol <> '' OR proxy_id <> ''
		ORDER BY event_id ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("select bridged events: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// deleteOrphanedAuthorProjectionsTx removes per-pubkey projections for the
// given authors when no events remain for them, and returns those authors.
// The existence check is one index probe on events (pubkey) per author in the
// batch, so the cost is bounded by the batch, not by table size.
func deleteOrphanedAuthorProjectionsTx(ctx context.Context, tx pgx.Tx, pubkeys []string) ([]string, error) {
	if len(pubkeys) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT p
		FROM unnest($1::text[]) AS p
		WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.pubkey = p)
	`, pubkeys)
	if err != nil {
		return nil, fmt.Errorf("find orphaned authors: %w", err)
	}
	orphaned, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("find orphaned authors: %w", err)
	}
	if len(orphaned) == 0 {
		return nil, nil
	}
	for _, table := range []string{
		"profile_discovery_stats",
		"profile_public_stats",
		"profile_fields",
		"nip05_verifications",
	} {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE pubkey = ANY($1)`, table), orphaned); err != nil {
			return nil, fmt.Errorf("delete orphaned %s rows: %w", table, err)
		}
	}
	return orphaned, nil
}
