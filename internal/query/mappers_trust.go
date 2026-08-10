package query

import "github.com/xdzczk/nostrmash/internal/readmodel"

// TrustSummaryFromState maps internal trust state into the product-facing
// summary. totalRanked is the denominator for percentile (ranked pubkeys in
// trust_pubkeys_latest); pass 0 to omit percentile.
func TrustSummaryFromState(state TrustState, totalRanked int64) TrustSummary {
	summary := TrustSummary{Tier: "unranked"}
	if state.IsSeed {
		summary.Tier = "seed"
	} else if state.HopDistance != nil {
		summary.Tier = "in_network"
	}
	summary.HopDistance = state.HopDistance
	if state.Rank != nil && *state.Rank > 0 && totalRanked > 0 {
		percentile := (float64(*state.Rank) / float64(totalRanked)) * 100.0
		summary.Percentile = &percentile
	}
	return summary
}

func trustScoreFromStore(row readmodel.TrustGlobalScore) TrustScore {
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

func trustStateFromStore(row readmodel.TrustState) TrustState {
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

func trustRunFromStore(row readmodel.TrustRun) TrustRun {
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

func trustQualificationFromStore(row readmodel.TrustQualification) TrustQualification {
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
