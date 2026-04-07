package query

import "github.com/xdzczk/nostrmash/internal/store"

func eventCursorFromStore(cursor *store.EventOrderCursor) *EventCursor {
	if cursor == nil {
		return nil
	}
	return &EventCursor{
		CreatedAt: cursor.CreatedAt,
		ID:        cursor.ID,
	}
}

func eventCursorToStore(cursor *EventCursor) *store.EventOrderCursor {
	if cursor == nil {
		return nil
	}
	return &store.EventOrderCursor{
		CreatedAt: cursor.CreatedAt,
		ID:        cursor.ID,
	}
}

func profileFromStore(row store.ProfileProjection) Profile {
	return Profile{
		Pubkey:            row.Pubkey,
		MetadataEventID:   row.MetadataEventID,
		MetadataCreatedAt: row.MetadataCreatedAt,
		ProfileJSON:       row.ProfileJSON,
	}
}

func trustScoreFromStore(row store.TrustGlobalScore) TrustScore {
	return TrustScore{
		Pubkey:         row.Pubkey,
		Score:          row.Score,
		Rank:           row.Rank,
		RunID:          row.RunID,
		DerivationName: row.DerivationName,
		TargetVersion:  row.TargetVersion,
		ComputedAt:     row.ComputedAt,
	}
}

func trustRunFromStore(row store.TrustRun) TrustRun {
	return TrustRun{
		ID:                 row.ID,
		DerivationName:     row.DerivationName,
		TargetVersion:      row.TargetVersion,
		Status:             row.Status,
		JobID:              row.JobID,
		Attempts:           row.Attempts,
		InputFollowerEdges: row.InputFollowerEdges,
		ScoreRowsPublished: row.ScoreRowsPublished,
		RedisSnapshotRef:   row.RedisSnapshotRef,
		CurrentPhase:       row.CurrentPhase,
		SyncJobID:          row.SyncJobID,
		ComputeJobID:       row.ComputeJobID,
		PromoteJobID:       row.PromoteJobID,
		PhaseStartedAt:     row.PhaseStartedAt,
		PhaseFinishedAt:    row.PhaseFinishedAt,
		PhaseLastError:     row.PhaseLastError,
		StartedAt:          row.StartedAt,
		FinishedAt:         row.FinishedAt,
		LastError:          row.LastError,
		CreatedAt:          row.CreatedAt,
		UpdatedAt:          row.UpdatedAt,
	}
}
