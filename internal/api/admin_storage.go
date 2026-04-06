package api

import (
	"context"
	"fmt"
)

type adminStorageResponse struct {
	DatabaseBytes int64                      `json:"database_bytes"`
	Tables        []adminStorageTableDetails `json:"tables"`
}

type adminStorageTableDetails struct {
	TableName string `json:"table_name"`
	RowCount  int64  `json:"row_count"`
	StorageB  int64  `json:"storage_bytes"`
}

var trackedStorageTables = []string{
	"events",
	"event_tags",
	"event_relays",
	"invalid_events",
	"jobs",
	"derivation_active_versions",
	"projection_rebuild_runs",
	"profiles_latest",
	"author_recent_events",
	"thread_edges",
}

func (s *adminService) GetStorage(ctx context.Context) (adminStorageResponse, error) {
	resp := adminStorageResponse{
		Tables: make([]adminStorageTableDetails, 0, len(trackedStorageTables)),
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT pg_database_size(current_database())
	`).Scan(&resp.DatabaseBytes); err != nil {
		return resp, fmt.Errorf("get database size: %w", err)
	}
	for _, tableName := range trackedStorageTables {
		var rowCount int64
		if err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName)).Scan(&rowCount); err != nil {
			return resp, fmt.Errorf("count table %s: %w", tableName, err)
		}
		var tableBytes *int64
		if err := s.pool.QueryRow(ctx, `
			SELECT pg_total_relation_size(to_regclass($1))
		`, tableName).Scan(&tableBytes); err != nil {
			return resp, fmt.Errorf("size table %s: %w", tableName, err)
		}
		storageBytes := int64(0)
		if tableBytes != nil {
			storageBytes = *tableBytes
		}
		resp.Tables = append(resp.Tables, adminStorageTableDetails{
			TableName: tableName,
			RowCount:  rowCount,
			StorageB:  storageBytes,
		})
	}
	return resp, nil
}
