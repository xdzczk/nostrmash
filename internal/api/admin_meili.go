package api

import (
	"context"
	"fmt"
	"time"
)

type adminMeilisearchSyncResponse struct {
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	BatchSize  int       `json:"batch_size"`
	Async      bool      `json:"async"`
	Status     string    `json:"status"`
	Stats      struct {
		Notes     int64 `json:"notes"`
		Profiles  int64 `json:"profiles"`
		Documents int64 `json:"documents"`
	} `json:"stats"`
}

func (s *adminService) TriggerMeilisearchSync(ctx context.Context, batchSize int, resetIndexes bool) (adminMeilisearchSyncResponse, error) {
	if s.meili == nil || !s.meili.Enabled() {
		return adminMeilisearchSyncResponse{}, fmt.Errorf("meilisearch is not configured")
	}
	startedAt := time.Now().UTC()
	if resetIndexes {
		// Meilisearch only reclaims disk after index deletion/recreation.
		if err := s.meili.ResetIndexes(ctx); err != nil {
			return adminMeilisearchSyncResponse{}, fmt.Errorf("reset meilisearch indexes: %w", err)
		}
	}
	stats, err := s.meili.FullSync(ctx, s.pool, batchSize)
	if err != nil {
		return adminMeilisearchSyncResponse{}, fmt.Errorf("run meilisearch full sync: %w", err)
	}
	resp := adminMeilisearchSyncResponse{
		StartedAt:  startedAt,
		FinishedAt: time.Now().UTC(),
		BatchSize:  batchSize,
		Async:      false,
		Status:     "completed",
	}
	resp.Stats.Notes = stats.Notes
	resp.Stats.Profiles = stats.Profiles
	resp.Stats.Documents = stats.Documents
	return resp, nil
}
