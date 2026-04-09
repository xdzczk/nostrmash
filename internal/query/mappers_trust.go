package query

import "github.com/xdzczk/nostrmash/internal/store"

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

func trustStateFromStore(row store.TrustState) TrustState {
	return TrustState{
		Pubkey:       row.Pubkey,
		Score:        row.Score,
		Qualified:    row.Qualified,
		Tier:         row.Tier,
		HopDistance:  row.HopDistance,
		HopBucket:    row.HopBucket,
		Rank:         row.Rank,
		ComputedAt:   row.ComputedAt,
		GenerationID: row.GenerationID,
		IsSeed:       row.IsSeed,
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

func trustQualificationFromStore(row store.TrustQualification) TrustQualification {
	return TrustQualification{
		Pubkey:       row.Pubkey,
		Trusted:      row.Trusted,
		IsSeed:       row.IsSeed,
		DistanceHops: row.DistanceHops,
		Score:        row.Score,
		Rank:         row.Rank,
		SourceRunID:  row.SourceRunID,
	}
}
