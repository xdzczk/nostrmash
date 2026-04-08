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

func profilePublicStatsFromStore(row store.ProfilePublicStatsProjection) ProfilePublicStats {
	return ProfilePublicStats{
		Pubkey:           row.Pubkey,
		FollowerCount:    row.FollowerCount,
		FollowingCount:   row.FollowingCount,
		NoteCount:        row.NoteCount,
		ReplyCount:       row.ReplyCount,
		RecentActivityAt: row.RecentActivityAt,
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

func publicDiscoveryNetworkStatsFromStore(row store.PublicDiscoveryNetworkStats) PublicDiscoveryNetworkStats {
	out := PublicDiscoveryNetworkStats{
		EventsIngested:    row.EventsIngested,
		ProjectedProfiles: row.ProjectedProfiles,
		Relays:            row.Relays,
		ActiveAuthors: WindowedCount{
			Last24h: row.ActiveAuthors.Last24h,
			Last7d:  row.ActiveAuthors.Last7d,
		},
		NoteVolume: WindowedCount{
			Last24h: row.NoteVolume.Last24h,
			Last7d:  row.NoteVolume.Last7d,
		},
	}
	if row.TopHashtags == nil {
		return out
	}
	topHashtags := &TrendingHashtagWindows{
		Last24h: make([]TrendingHashtag, 0, len(row.TopHashtags.Last24h)),
		Last7d:  make([]TrendingHashtag, 0, len(row.TopHashtags.Last7d)),
	}
	for _, hashtag := range row.TopHashtags.Last24h {
		topHashtags.Last24h = append(topHashtags.Last24h, trendingHashtagFromStore(hashtag))
	}
	for _, hashtag := range row.TopHashtags.Last7d {
		topHashtags.Last7d = append(topHashtags.Last7d, trendingHashtagFromStore(hashtag))
	}
	out.TopHashtags = topHashtags
	return out
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

func trendingHashtagFromStore(row store.TrendingHashtag) TrendingHashtag {
	return TrendingHashtag{
		Hashtag:       row.Hashtag,
		EventCount:    row.EventCount,
		UniqueAuthors: row.UniqueAuthors,
	}
}

func trendingNoteFromStore(row store.TrendingNote) TrendingNote {
	return TrendingNote{
		EventID:       row.EventID,
		AuthorPubkey:  row.AuthorPubkey,
		CreatedAt:     row.CreatedAt,
		Content:       row.Content,
		ReplyCount:    row.ReplyCount,
		RepostCount:   row.RepostCount,
		ReactionCount: row.ReactionCount,
		ZapCount:      row.ZapCount,
		ZapMSats:      row.ZapMSats,
		Score:         row.Score,
	}
}

func trendingProfileFromStore(row store.TrendingProfile) TrendingProfile {
	return TrendingProfile{
		Pubkey:                   row.Pubkey,
		Score:                    row.Score,
		RecentPostCount:          row.RecentPostCount,
		RecentReplyCount:         row.RecentReplyCount,
		RecentEngagementReceived: row.RecentEngagementReceived,
		RecentZapVolumeMSats:     row.RecentZapVolumeMSats,
		RecentActiveDays:         row.RecentActiveDays,
		RecentActivityAt:         row.RecentActivityAt,
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
