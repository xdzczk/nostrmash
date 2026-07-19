// Package retention owns the retention/pruning bounded context: bounded
// DELETE sweeps over event, relay, search-document, and account-state tables
// plus trusted-discovery-candidate purges. It is composed into the top-level
// store.PostgresStore via embedding so callers keep a single store handle.
package retention

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// Store is the retention data-access surface backed by a shared pool.
type Retention struct {
	pool *pgxpool.Pool
}

// New builds a retention Store over the shared connection pool.
func New(pool *pgxpool.Pool) *Retention {
	return &Retention{pool: pool}
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
