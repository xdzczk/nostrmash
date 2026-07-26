// Package retention owns the retention/pruning bounded context: bounded
// DELETE sweeps over event, relay, search-document, and account-state tables
// plus trusted-discovery-candidate purges. It is composed into the top-level
// store.PostgresStore via embedding so callers keep a single store handle.
package retention

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/readmodel"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// Store is the retention data-access surface backed by a shared pool.
type Retention struct {
	pool *pgxpool.Pool
}

// New builds a retention Store over the shared connection pool.
func New(pool *pgxpool.Pool) *Retention {
	return &Retention{pool: pool}
}

// Statement guards for retention purges. Every purge is a bounded batch that
// should finish in seconds; these caps exist so a pathological plan or lock
// queue can never turn one batch into a multi-day transaction that pins the
// vacuum horizon and serializes the rest of the retention pipeline (observed
// in production: purge statements running for 1-3 days while holding locks
// that blocked every other retention loop).
//
//   - retentionStatementTimeout aborts a single purge statement that runs too
//     long. The loop logs the failure and retries on its next tick.
//   - retentionLockTimeout aborts a purge that queues behind a conflicting
//     lock (e.g. an exclusive lock from a projection rebuild) instead of
//     waiting indefinitely.
const (
	retentionStatementTimeout = 15 * time.Minute
	retentionLockTimeout      = time.Minute
)

// guarded runs fn inside a transaction with SET LOCAL statement/lock timeouts
// applied, so the timeouts scope to this batch only and never leak onto other
// work sharing the pool connection.
func (s *Retention) guarded(ctx context.Context, fn func(q *retentiondb.Queries) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin retention tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = %d", retentionStatementTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("set retention statement_timeout: %w", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = %d", retentionLockTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("set retention lock_timeout: %w", err)
	}
	if err := fn(retentiondb.New(tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit retention tx: %w", err)
	}
	return nil
}

// tsz converts a domain time.Time into the pgtype.Timestamptz the generated
// queries expect. sqlc treats bind parameters as nullable, so this always
// marks the value valid — the wrappers validate non-zero times before calling.
func tsz(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}

// dbResultFromErr maps an error to the {ok,not_found,error} label used by the
// store latency/observation metrics. Mirrors the helper in the parent store
// package; kept local so the retention context has no back-dependency on it.
func dbResultFromErr(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, readmodel.ErrNotFound) {
		return "not_found"
	}
	return "error"
}
