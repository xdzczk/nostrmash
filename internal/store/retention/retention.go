// Package retention owns the retention/pruning bounded context: bounded
// DELETE sweeps over event, relay, search-document, and account-state tables
// plus trusted-discovery-candidate purges. It is composed into the top-level
// store.PostgresStore via embedding so callers keep a single store handle.
package retention

import (
	"errors"
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

// queries binds the sqlc-generated retention statements to the shared pool.
// Kept unexported so the generated layer never leaks past the package edge:
// callers still see only the hand-written methods that own validation and
// metrics.
func (s *Retention) queries() *retentiondb.Queries {
	return retentiondb.New(s.pool)
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
