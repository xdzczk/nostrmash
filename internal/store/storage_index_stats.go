package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// IndexUsageStats is one row of pg_stat_user_indexes evidence for the
// index-ownership audit: operators must see idx_scan stay at 0 across a fresh
// observation window before an index drop is justified.
type IndexUsageStats struct {
	TableName  string
	IndexName  string
	IndexBytes int64
	IdxScan    int64
	IdxTupRead int64
}

// TableVacuumStats surfaces dead-tuple pressure per table so operators can
// distinguish live growth from reclaimable bloat (retention DELETEs leave
// dead tuples that only vacuum reuse or pg_repack actually reclaims).
type TableVacuumStats struct {
	TableName      string
	LiveTuples     int64
	DeadTuples     int64
	LastVacuum     *time.Time
	LastAutovacuum *time.Time
}

// CollectIndexStats returns per-index usage/size stats and per-table vacuum
// stats for the requested tables, both from pg_stat views (cheap; no table
// scans).
func CollectIndexStats(
	ctx context.Context,
	pool *pgxpool.Pool,
	tableNames []string,
) ([]IndexUsageStats, []TableVacuumStats, error) {
	if pool == nil {
		return nil, nil, fmt.Errorf("pool is required")
	}

	indexRows, err := pool.Query(ctx, `
		SELECT s.relname,
		       s.indexrelname,
		       COALESCE(pg_relation_size(s.indexrelid), 0)::bigint,
		       COALESCE(s.idx_scan, 0)::bigint,
		       COALESCE(s.idx_tup_read, 0)::bigint
		FROM pg_stat_user_indexes s
		WHERE s.relname = ANY($1::text[])
		ORDER BY s.relname ASC, s.indexrelname ASC
	`, tableNames)
	if err != nil {
		return nil, nil, fmt.Errorf("load index usage stats: %w", err)
	}
	defer indexRows.Close()

	indexes := make([]IndexUsageStats, 0)
	for indexRows.Next() {
		var row IndexUsageStats
		if err := indexRows.Scan(&row.TableName, &row.IndexName, &row.IndexBytes, &row.IdxScan, &row.IdxTupRead); err != nil {
			return nil, nil, fmt.Errorf("scan index usage stats: %w", err)
		}
		indexes = append(indexes, row)
	}
	if err := indexRows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read index usage stats: %w", err)
	}

	tableRows, err := pool.Query(ctx, `
		SELECT t.relname,
		       COALESCE(t.n_live_tup, 0)::bigint,
		       COALESCE(t.n_dead_tup, 0)::bigint,
		       t.last_vacuum,
		       t.last_autovacuum
		FROM pg_stat_user_tables t
		WHERE t.relname = ANY($1::text[])
		ORDER BY t.relname ASC
	`, tableNames)
	if err != nil {
		return nil, nil, fmt.Errorf("load table vacuum stats: %w", err)
	}
	defer tableRows.Close()

	tables := make([]TableVacuumStats, 0)
	for tableRows.Next() {
		var row TableVacuumStats
		if err := tableRows.Scan(&row.TableName, &row.LiveTuples, &row.DeadTuples, &row.LastVacuum, &row.LastAutovacuum); err != nil {
			return nil, nil, fmt.Errorf("scan table vacuum stats: %w", err)
		}
		tables = append(tables, row)
	}
	if err := tableRows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read table vacuum stats: %w", err)
	}
	return indexes, tables, nil
}
