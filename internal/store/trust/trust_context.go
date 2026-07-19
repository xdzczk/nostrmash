// Package trust owns the trust bounded context: trust-run scores, trust-state
// and qualification queries, the graph/pubkey-frontier snapshot refreshers,
// seeds, scheduling, and relay-suggestion projections. It is composed into the
// top-level store.PostgresStore via embedding so callers keep a single store
// handle.
package trust

import "github.com/jackc/pgx/v5/pgxpool"

// Trust is the trust-context data-access surface backed by a shared pool.
type Trust struct {
	pool *pgxpool.Pool
}

// New builds a trust Store over the shared connection pool.
func New(pool *pgxpool.Pool) *Trust {
	return &Trust{pool: pool}
}
