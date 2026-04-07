package api

import (
	"context"
	"fmt"

	"github.com/xdzczk/nostrmash/internal/store"
)

type adminStorageResponse struct {
	DatabaseBytes int64                      `json:"database_bytes"`
	Tables        []adminStorageTableDetails `json:"tables"`
}

type adminStorageTableDetails struct {
	TableName         string `json:"table_name"`
	RowCount          int64  `json:"row_count"`
	RowCountEstimated bool   `json:"row_count_estimated"`
	StorageB          int64  `json:"storage_bytes"`
}

var trackedStorageTables = []string{
	"events",
	"event_tags",
	"event_relays",
	"ingest_checkpoints",
	"invalid_events",
	"jobs",
	"event_references",
	"pubkey_references",
	"replaceable_state",
	"derivation_active_versions",
	"projection_rebuild_runs",
	"profiles_latest",
	"author_recent_events",
	"thread_edges",
	"follower_edges",
	"reply_count_contributions",
	"reaction_count_contributions",
	"repost_count_contributions",
	"reaction_events",
	"repost_events",
	"deletion_events",
	"dm_unread_counts",
	"dm_read_cursors",
	"zap_receipts",
	"trust_seeds",
	"trust_runs",
	"trust_scores_global",
}

func TrackedStorageTables() []string {
	out := make([]string, len(trackedStorageTables))
	copy(out, trackedStorageTables)
	return out
}

func (s *adminService) GetStorage(ctx context.Context) (adminStorageResponse, error) {
	resp := adminStorageResponse{
		Tables: make([]adminStorageTableDetails, 0, len(trackedStorageTables)),
	}
	snapshot, err := store.CollectStorageStats(ctx, s.pool, trackedStorageTables, store.StorageStatsOptions{
		ExactRowCountMaxBytes: 16 << 20, // keep admin endpoint reasonably accurate without heavy full scans
	})
	if err != nil {
		return resp, fmt.Errorf("collect storage stats: %w", err)
	}
	resp.DatabaseBytes = snapshot.DatabaseBytes
	for _, table := range snapshot.Tables {
		resp.Tables = append(resp.Tables, adminStorageTableDetails{
			TableName:         table.TableName,
			RowCount:          table.RowCount,
			RowCountEstimated: table.RowCountMode == "estimated",
			StorageB:          table.StorageBytes,
		})
	}
	return resp, nil
}
