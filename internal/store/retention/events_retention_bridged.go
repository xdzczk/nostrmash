package retention

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// BridgedPurgeResult reports one PurgeBridgedEvents batch.
type BridgedPurgeResult struct {
	// Deleted is the number of events rows removed.
	Deleted int64
	// OrphanedAuthors lists pubkeys from the batch that have no events left.
	// Their per-pubkey projections were removed with the event that emptied
	// them; the caller uses the list to drop their search documents.
	OrphanedAuthors []string
}

// bridgedPurgeLockTimeout is how long one event delete waits for a row
// before it gives up and retries. Derivation statements use the pool's
// ~15s statement_timeout, which includes lock waits, so a purge that holds
// profile or event locks for longer than that dead-letters derive jobs.
const bridgedPurgeLockTimeout = 2 * time.Second

const bridgedPurgeAttempts = 3

// LockConflict reports a deadlock (40P01) or lock_timeout (55P03).
func LockConflict(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "40P01" || pgErr.Code == "55P03"
}

// PurgeBridgedEvents deletes a bounded batch of stored events that carry a
// NIP-48 proxy tag. The ingestor drops new bridged events, so this is the
// one-time cleanup of what was stored before that; the candidate set is the
// partial index idx_event_attributes_bridged (migration 000127), so an empty
// backlog is an index probe. Each event is reversed and deleted in its own
// transaction: a batch of thousands in one transaction held profile_public_stats
// and event rows long enough to time out derive jobs and deadlock the delete.
// The event row is locked before any profile row, which is the order a derive
// bundle takes when it inserts a child of events and then updates profile
// stats. Author-level projections are removed with the event that leaves a
// pubkey empty, so a bridged account does not linger in profile rankings,
// identity fields, or NIP-05 checks. profiles_latest cascades from the kind-0
// delete.
func (s *Retention) PurgeBridgedEvents(ctx context.Context, limit int) (BridgedPurgeResult, error) {
	if s == nil || s.pool == nil {
		return BridgedPurgeResult{}, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return BridgedPurgeResult{}, fmt.Errorf("limit must be > 0")
	}
	started := time.Now()
	var ids []string
	err := s.guardedWithTx(ctx, func(tx pgx.Tx, _ *retentiondb.Queries) error {
		var selectErr error
		ids, selectErr = selectBridgedEventIDs(ctx, tx, limit)
		return selectErr
	})
	if err != nil {
		metrics.ObserveDBOperation("purge_bridged_events", dbResultFromErr(err), time.Since(started))
		return BridgedPurgeResult{}, fmt.Errorf("purge bridged events: %w", err)
	}
	var result BridgedPurgeResult
	for _, id := range ids {
		outcome, purgeErr := s.purgeOneBridgedEvent(ctx, id)
		if purgeErr != nil {
			metrics.ObserveDBOperation("purge_bridged_events", dbResultFromErr(purgeErr), time.Since(started))
			return BridgedPurgeResult{}, fmt.Errorf("purge bridged events: %w", purgeErr)
		}
		result.Deleted += outcome.deleted
		if outcome.orphan != "" {
			result.OrphanedAuthors = append(result.OrphanedAuthors, outcome.orphan)
		}
	}
	metrics.ObserveDBOperation("purge_bridged_events", "ok", time.Since(started))
	return result, nil
}

type bridgedEventOutcome struct {
	deleted int64
	orphan  string
}

func (s *Retention) purgeOneBridgedEvent(ctx context.Context, id string) (bridgedEventOutcome, error) {
	var last error
	for attempt := 1; attempt <= bridgedPurgeAttempts; attempt++ {
		outcome, err := s.purgeOneBridgedEventOnce(ctx, id)
		if err == nil || !LockConflict(err) {
			return outcome, err
		}
		last = err
		if attempt == bridgedPurgeAttempts {
			break
		}
		timer := time.NewTimer(time.Duration(attempt) * 50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return bridgedEventOutcome{}, ctx.Err()
		case <-timer.C:
		}
	}
	return bridgedEventOutcome{}, last
}

func (s *Retention) purgeOneBridgedEventOnce(ctx context.Context, id string) (bridgedEventOutcome, error) {
	var outcome bridgedEventOutcome
	err := s.guardedWithTx(ctx, func(tx pgx.Tx, q *retentiondb.Queries) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = %d", bridgedPurgeLockTimeout.Milliseconds())); err != nil {
			return fmt.Errorf("set bridged purge lock_timeout: %w", err)
		}
		var pubkey string
		err := tx.QueryRow(ctx, `SELECT pubkey FROM events WHERE id = $1 FOR UPDATE`, id).Scan(&pubkey)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock bridged event: %w", err)
		}
		deleted, err := s.reverseAndDeleteTx(ctx, tx, q, []string{id})
		if err != nil {
			return err
		}
		outcome.deleted = deleted
		if deleted == 0 {
			return nil
		}
		orphans, err := deleteOrphanedAuthorProjectionsTx(ctx, tx, []string{pubkey})
		if err != nil {
			return err
		}
		if len(orphans) == 1 {
			outcome.orphan = orphans[0]
		}
		return nil
	})
	return outcome, err
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
