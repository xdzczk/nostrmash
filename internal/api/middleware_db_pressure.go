package api

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// dbAcquireBudget is how long a request may wait for a pooled connection
// before the API refuses it. During the 2026-11-14 traffic burst, requests
// queued on the 32-connection pool until the handler timed out and the
// process accumulated thousands of goroutines. Failing in half a second lets
// the web worker serve its last-known-good copy instead.
const dbAcquireBudget = 500 * time.Millisecond

// dbConnAcquirer checks out one pooled connection. The release func returns
// it. Tests substitute a fake so the deadline behavior does not need Postgres.
type dbConnAcquirer interface {
	Acquire(ctx context.Context) (release func(), err error)
}

type poolAcquirer struct {
	pool *pgxpool.Pool
}

func (p poolAcquirer) Acquire(ctx context.Context) (func(), error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return conn.Release, nil
}

// WithDBAcquireBudget rejects API work when every database connection is busy.
// Health, readiness, and metrics stay exempt so a saturated pool does not
// make the process look down. The check runs after the rate limiter, so a
// client that is already over its limit does not take a checkout slot.
func WithDBAcquireBudget(pool *pgxpool.Pool, next http.Handler) http.Handler {
	if pool == nil || next == nil {
		return next
	}
	return withDBAcquireBudget(poolAcquirer{pool: pool}, dbAcquireBudget, next)
}

func withDBAcquireBudget(acquirer dbConnAcquirer, timeout time.Duration, next http.Handler) http.Handler {
	if timeout <= 0 {
		timeout = dbAcquireBudget
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isRateLimitExemptPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		release, err := acquirer.Acquire(ctx)
		cancel()
		if err != nil {
			// The client already went away. Writing a 503 would only log noise.
			if r.Context().Err() != nil {
				return
			}
			w.Header().Set("Retry-After", "1")
			writeError(r.Context(), w, http.StatusServiceUnavailable, "overloaded", "database is busy, retry shortly")
			return
		}
		release()
		next.ServeHTTP(w, r)
	})
}
