package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OpenPool creates a pgx connection pool from a DATABASE_URL-style DSN.
// If maxConns > 0 it overrides whatever pool_max_conns was parsed from
// the DSN (or the pgx default of 4 when the DSN omits it).
//
// The pgx default of 4 is dangerously low for the worker process,
// which runs the bundle pool plus several background sweeper
// goroutines (author_analytics, profile_stats, meilisearch). When the
// pool is undersized, sweepers holding heavy multi-second aggregate
// queries monopolize all available connections, leaving bundle
// workers blocked at pool.Acquire() and producing the symptom of
// "bundles never run, frontend goes stale" even though no individual
// query is failing.
func OpenPool(ctx context.Context, databaseURL string, maxConns int32, applicationName string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	// pg_stat_activity.application_name is what lets a sweeper ignore its
	// own process when deciding the server is saturated, and what makes a
	// busy backend attributable to api, worker, ingestor, or trust_worker.
	if name := strings.TrimSpace(applicationName); name != "" {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["application_name"] = name
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}
