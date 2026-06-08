package store

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type StorageStatsOptions struct {
	// ExactRowCountMaxBytes enables exact COUNT(*) only for tables at or below this size.
	// A value <= 0 uses estimated row counts for all tables.
	ExactRowCountMaxBytes int64
}

type StorageTableStats struct {
	TableName    string
	RowCount     int64
	StorageBytes int64  // total relation size (table + indexes + toast)
	TableBytes   int64  // table heap + toast only (pg_table_size)
	IndexBytes   int64  // indexes only (pg_indexes_size)
	RowCountMode string // "exact" or "estimated"
}

type StorageStats struct {
	DatabaseBytes int64
	Tables        []StorageTableStats
}

var storageTableIdentPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func CollectStorageStats(
	ctx context.Context,
	pool *pgxpool.Pool,
	tableNames []string,
	options StorageStatsOptions,
) (StorageStats, error) {
	if pool == nil {
		return StorageStats{}, fmt.Errorf("pool is required")
	}

	stats := StorageStats{
		Tables: make([]StorageTableStats, 0, len(tableNames)),
	}
	if err := pool.QueryRow(ctx, `SELECT pg_database_size(current_database())`).Scan(&stats.DatabaseBytes); err != nil {
		return stats, fmt.Errorf("get database size: %w", err)
	}
	if len(tableNames) == 0 {
		return stats, nil
	}

	rows, err := pool.Query(ctx, `
		WITH requested AS (
			SELECT t.table_name, t.ord
			FROM unnest($1::text[]) WITH ORDINALITY AS t(table_name, ord)
		)
		SELECT requested.table_name,
		       COALESCE(pg_total_relation_size(to_regclass(requested.table_name)), 0)::bigint AS storage_bytes,
		       COALESCE(pg_table_size(to_regclass(requested.table_name)), 0)::bigint AS table_bytes,
		       COALESCE(pg_indexes_size(to_regclass(requested.table_name)), 0)::bigint AS index_bytes,
		       COALESCE(pg_class.reltuples, 0)::bigint AS estimated_rows
		FROM requested
		LEFT JOIN pg_class ON pg_class.oid = to_regclass(requested.table_name)
		ORDER BY requested.ord
	`, tableNames)
	if err != nil {
		return stats, fmt.Errorf("load table storage stats: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		var storageBytes int64
		var tableBytes int64
		var indexBytes int64
		var estimatedRows int64
		if err := rows.Scan(&tableName, &storageBytes, &tableBytes, &indexBytes, &estimatedRows); err != nil {
			return stats, fmt.Errorf("scan storage stats: %w", err)
		}

		rowCount := estimatedRows
		rowCountMode := "estimated"
		if options.ExactRowCountMaxBytes > 0 && storageBytes <= options.ExactRowCountMaxBytes {
			exactRows, err := countExactRows(ctx, pool, tableName)
			if err != nil {
				return stats, fmt.Errorf("count exact rows for %s: %w", tableName, err)
			}
			rowCount = exactRows
			rowCountMode = "exact"
		}

		stats.Tables = append(stats.Tables, StorageTableStats{
			TableName:    tableName,
			RowCount:     rowCount,
			StorageBytes: storageBytes,
			TableBytes:   tableBytes,
			IndexBytes:   indexBytes,
			RowCountMode: rowCountMode,
		})
	}
	if err := rows.Err(); err != nil {
		return stats, fmt.Errorf("read storage stats: %w", err)
	}
	return stats, nil
}

func countExactRows(ctx context.Context, pool *pgxpool.Pool, tableName string) (int64, error) {
	if !storageTableIdentPattern.MatchString(tableName) {
		return 0, fmt.Errorf("invalid table name %q", tableName)
	}
	query := `SELECT COUNT(*) FROM ` + pgx.Identifier{tableName}.Sanitize()
	var count int64
	if err := pool.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
