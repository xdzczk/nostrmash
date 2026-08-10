package api

import (
	"context"
	"fmt"

	"github.com/xdzczk/nostrmash/internal/store"
)

type adminStorageResponse struct {
	DatabaseBytes int64                      `json:"database_bytes"`
	Tiers         map[string]int64           `json:"tier_bytes"`
	Tables        []adminStorageTableDetails `json:"tables"`
}

type adminStorageTableDetails struct {
	TableName         string `json:"table_name"`
	Tier              string `json:"tier"`
	RowCount          int64  `json:"row_count"`
	RowCountEstimated bool   `json:"row_count_estimated"`
	StorageB          int64  `json:"storage_bytes"`
	TableB            int64  `json:"table_bytes"`
	IndexB            int64  `json:"index_bytes"`
}

// Storage tiers used for byte rollups so operators can see which class of data
// is growing, not just which individual table. Classification is product
// knowledge and intentionally lives in the api layer, not the store layer.
const (
	StorageTierCanonical   = "canonical"
	StorageTierDerived     = "derived"
	StorageTierOperational = "operational"
)

// trackedStorageTableTiers maps every tracked table to its storage tier. Any
// table absent from this map defaults to "derived" (the safe assumption for a
// rebuildable projection). Keep this list in sync with trackedStorageTables.
var trackedStorageTableTiers = map[string]string{
	// Canonical roots (never auto-pruned).
	"events":             StorageTierCanonical,
	"event_tags":         StorageTierCanonical,
	"event_relays":       StorageTierCanonical,
	"ingest_checkpoints": StorageTierCanonical,
	"invalid_events":     StorageTierCanonical,
	// Operational / queue exhaust (bounded by explicit retention).
	"jobs":                       StorageTierOperational,
	"derivation_active_versions": StorageTierOperational,
	"projection_rebuild_runs":    StorageTierOperational,
	"trust_runs":                 StorageTierOperational,
	"trust_seeds":                StorageTierOperational,
	"account_state_transitions":  StorageTierOperational,
	// Derived / rebuildable projections.
	"event_references":                     StorageTierDerived,
	"replaceable_state":                    StorageTierDerived,
	"profiles_latest":                      StorageTierDerived,
	"author_recent_events":                 StorageTierDerived,
	"thread_edges":                         StorageTierDerived,
	"thread_summaries":                     StorageTierDerived,
	"follower_edges":                       StorageTierDerived,
	"reply_count_contributions":            StorageTierDerived,
	"reaction_count_contributions":         StorageTierDerived,
	"repost_count_contributions":           StorageTierDerived,
	"reaction_events":                      StorageTierDerived,
	"repost_events":                        StorageTierDerived,
	"deletion_events":                      StorageTierDerived,
	"dm_unread_counts":                     StorageTierDerived,
	"dm_read_cursors":                      StorageTierDerived,
	"zap_receipts":                         StorageTierDerived,
	"trust_scores_global":                  StorageTierDerived,
	"trust_pubkeys_latest":                 StorageTierDerived,
	"trust_graph_snapshot":                 StorageTierDerived,
	"search_documents":                     StorageTierDerived,
	"note_discovery_stats":                 StorageTierDerived,
	"profile_discovery_stats":              StorageTierDerived,
	"event_hashtags":                       StorageTierDerived,
	"event_urls":                           StorageTierDerived,
	"account_states":                       StorageTierDerived,
	"reply_counts":                         StorageTierDerived,
	"reaction_counts":                      StorageTierDerived,
	"repost_counts":                        StorageTierDerived,
	"contact_lists_latest":                 StorageTierDerived,
	"relay_lists_latest":                   StorageTierDerived,
	"unresolved_thread_references":         StorageTierDerived,
	"trusted_note_discovery_candidates":    StorageTierDerived,
	"trusted_profile_discovery_candidates": StorageTierDerived,
}

var trackedStorageTables = []string{
	"events",
	"event_tags",
	"event_relays",
	"ingest_checkpoints",
	"invalid_events",
	"jobs",
	"event_references",
	"replaceable_state",
	"derivation_active_versions",
	"projection_rebuild_runs",
	"profiles_latest",
	"author_recent_events",
	"thread_edges",
	"thread_summaries",
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
	"trust_pubkeys_latest",
	"trust_graph_snapshot",
	"search_documents",
	"note_discovery_stats",
	"profile_discovery_stats",
	"event_hashtags",
	"event_urls",
	"account_states",
	"account_state_transitions",
	"reply_counts",
	"reaction_counts",
	"repost_counts",
	"contact_lists_latest",
	"relay_lists_latest",
	"unresolved_thread_references",
	"trusted_note_discovery_candidates",
	"trusted_profile_discovery_candidates",
}

func TrackedStorageTables() []string {
	out := make([]string, len(trackedStorageTables))
	copy(out, trackedStorageTables)
	return out
}

// StorageTableTier returns the storage tier for a tracked table, defaulting to
// "derived" for any table not explicitly classified.
func StorageTableTier(table string) string {
	if tier, ok := trackedStorageTableTiers[table]; ok {
		return tier
	}
	return StorageTierDerived
}

type adminStorageIndexesResponse struct {
	Indexes []adminIndexUsageDetails  `json:"indexes"`
	Tables  []adminTableVacuumDetails `json:"tables"`
}

type adminIndexUsageDetails struct {
	TableName  string `json:"table_name"`
	IndexName  string `json:"index_name"`
	IndexB     int64  `json:"index_bytes"`
	IdxScan    int64  `json:"idx_scan"`
	IdxTupRead int64  `json:"idx_tup_read"`
}

type adminTableVacuumDetails struct {
	TableName      string  `json:"table_name"`
	LiveTuples     int64   `json:"live_tuples"`
	DeadTuples     int64   `json:"dead_tuples"`
	LastVacuum     *string `json:"last_vacuum"`
	LastAutovacuum *string `json:"last_autovacuum"`
}

// GetStorageIndexes serves the Phase 2 index-ownership audit evidence:
// pg_stat_user_indexes usage per index plus per-table dead-tuple pressure.
// Index drops stay operator-gated; this endpoint only provides the evidence
// (idx_scan must stay 0 across a fresh window before a drop is justified).
func (s *adminService) GetStorageIndexes(ctx context.Context) (adminStorageIndexesResponse, error) {
	resp := adminStorageIndexesResponse{
		Indexes: make([]adminIndexUsageDetails, 0),
		Tables:  make([]adminTableVacuumDetails, 0),
	}
	indexes, tables, err := store.CollectIndexStats(ctx, s.pool, trackedStorageTables)
	if err != nil {
		return resp, fmt.Errorf("collect index stats: %w", err)
	}
	for _, idx := range indexes {
		resp.Indexes = append(resp.Indexes, adminIndexUsageDetails{
			TableName:  idx.TableName,
			IndexName:  idx.IndexName,
			IndexB:     idx.IndexBytes,
			IdxScan:    idx.IdxScan,
			IdxTupRead: idx.IdxTupRead,
		})
	}
	for _, table := range tables {
		detail := adminTableVacuumDetails{
			TableName:  table.TableName,
			LiveTuples: table.LiveTuples,
			DeadTuples: table.DeadTuples,
		}
		if table.LastVacuum != nil {
			v := table.LastVacuum.UTC().Format("2006-01-02T15:04:05Z07:00")
			detail.LastVacuum = &v
		}
		if table.LastAutovacuum != nil {
			v := table.LastAutovacuum.UTC().Format("2006-01-02T15:04:05Z07:00")
			detail.LastAutovacuum = &v
		}
		resp.Tables = append(resp.Tables, detail)
	}
	return resp, nil
}

func (s *adminService) GetStorage(ctx context.Context) (adminStorageResponse, error) {
	resp := adminStorageResponse{
		Tiers:  make(map[string]int64, 3),
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
		tier := StorageTableTier(table.TableName)
		resp.Tiers[tier] += table.StorageBytes
		resp.Tables = append(resp.Tables, adminStorageTableDetails{
			TableName:         table.TableName,
			Tier:              tier,
			RowCount:          table.RowCount,
			RowCountEstimated: table.RowCountMode == "estimated",
			StorageB:          table.StorageBytes,
			TableB:            table.TableBytes,
			IndexB:            table.IndexBytes,
		})
	}
	return resp, nil
}
