package query

import (
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

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

func contactListFromStore(row store.ContactListProjection) ContactList {
	return ContactList{
		Pubkey:          row.Pubkey,
		EventID:         row.EventID,
		CreatedAt:       row.CreatedAt,
		DerivationVer:   row.DerivationVer,
		ContactsJSONRaw: row.ContactsJSONRaw,
	}
}

func relayListFromStore(row store.RelayListProjection) RelayList {
	return RelayList{
		Pubkey:        row.Pubkey,
		EventID:       row.EventID,
		CreatedAt:     row.CreatedAt,
		DerivationVer: row.DerivationVer,
		RelaysJSONRaw: row.RelaysJSONRaw,
	}
}

func networkStatsFromStore(row store.NetworkStats) NetworkStats {
	return NetworkStats{
		Events:   row.Events,
		Profiles: row.Profiles,
		Relays:   row.Relays,
	}
}

func curatedRecommendedReadFromStore(row store.CuratedRecommendedRead) CuratedRecommendedRead {
	return CuratedRecommendedRead{
		EventID: row.EventID,
		Title:   row.Title,
		URL:     row.URL,
		Rank:    row.Rank,
	}
}

func curatedReadsTopicFromStore(row store.CuratedReadsTopic) CuratedReadsTopic {
	return CuratedReadsTopic{
		Topic: row.Topic,
		Rank:  row.Rank,
	}
}

func curatedFeaturedAuthorFromStore(row store.CuratedFeaturedAuthor) CuratedFeaturedAuthor {
	return CuratedFeaturedAuthor{
		Pubkey: row.Pubkey,
		Rank:   row.Rank,
	}
}

func eventWithProvenanceFromStore(row store.EventWithProvenance) EventWithProvenance {
	relays := make([]model.EventRelay, 0, len(row.Relays))
	for _, relay := range row.Relays {
		relays = append(relays, model.EventRelay{
			EventID:  relay.EventID,
			RelayURL: relay.RelayURL,
			SeenAt:   relay.SeenAt.UTC(),
		})
	}
	return EventWithProvenance{
		Event:  row.Event,
		Relays: relays,
	}
}

func eventCountsFromStore(row store.EventCounts) EventCounts {
	return EventCounts{
		EventID:       row.EventID,
		ReplyCount:    row.ReplyCount,
		ReactionCount: row.ReactionCount,
		RepostCount:   row.RepostCount,
		Consistency:   row.Consistency,
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
