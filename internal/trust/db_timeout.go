package trust

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// trustEdgeScanStatementTimeout covers full-table follower_edges scans used by
// Redis graph sync / Postgres adjacency load. Those cursors stay open while
// millions of rows stream into Redis and exceed the production 15s API
// statement_timeout guardrail.
const trustEdgeScanStatementTimeout = 60 * time.Minute

func withHeavyStatementTimeout(
	ctx context.Context,
	pool *pgxpool.Pool,
	timeout time.Duration,
	fn func(conn *pgxpool.Conn) error,
) error {
	if pool == nil {
		return fmt.Errorf("nil pool")
	}
	if timeout <= 0 {
		timeout = trustEdgeScanStatementTimeout
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire trust db conn: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		"SELECT set_config('statement_timeout', '%d', false)",
		timeout.Milliseconds(),
	)); err != nil {
		return fmt.Errorf("raise trust statement_timeout: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `RESET statement_timeout`)
	}()
	return fn(conn)
}
