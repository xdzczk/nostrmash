package query

import "github.com/xdzczk/nostrmash/internal/store"

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
		RelaySummary: RelaySummaryStats{
			Total:     row.RelaySummary.Total,
			Active24h: row.RelaySummary.Active24h,
			Active7d:  row.RelaySummary.Active7d,
			EventVolume: WindowedCount{
				Last24h: row.RelaySummary.EventVolume.Last24h,
				Last7d:  row.RelaySummary.EventVolume.Last7d,
			},
			UniqueAuthors: WindowedCount{
				Last24h: row.RelaySummary.UniqueAuthors.Last24h,
				Last7d:  row.RelaySummary.UniqueAuthors.Last7d,
			},
		},
		ActiveAuthors: WindowedCount{
			Last24h: row.ActiveAuthors.Last24h,
			Last7d:  row.ActiveAuthors.Last7d,
		},
		NoteVolume: WindowedCount{
			Last24h: row.NoteVolume.Last24h,
			Last7d:  row.NoteVolume.Last7d,
		},
	}
	if len(row.TopLanguages24h) > 0 {
		out.TopLanguages24h = make([]LanguageSummary, 0, len(row.TopLanguages24h))
		for _, lang := range row.TopLanguages24h {
			out.TopLanguages24h = append(out.TopLanguages24h, languageSummaryFromStore(lang))
		}
	}
	if len(row.TopLanguages7d) > 0 {
		out.TopLanguages7d = make([]LanguageSummary, 0, len(row.TopLanguages7d))
		for _, lang := range row.TopLanguages7d {
			out.TopLanguages7d = append(out.TopLanguages7d, languageSummaryFromStore(lang))
		}
	}
	if len(row.TopRelays) > 0 {
		out.TopRelays = make([]RelayUsageSummary, 0, len(row.TopRelays))
		for _, relay := range row.TopRelays {
			out.TopRelays = append(out.TopRelays, relayUsageFromStore(relay))
		}
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

func hashtagSummaryFromStore(row store.HashtagSummary) HashtagSummary {
	return HashtagSummary{
		Hashtag:       row.Hashtag,
		LatestEventAt: row.LatestEventAt,
		Activity: HashtagActivityStats{
			Last24h: hashtagActivityFromStore(row.Activity.Last24h),
			Last7d:  hashtagActivityFromStore(row.Activity.Last7d),
			Last30d: hashtagActivityFromStore(row.Activity.Last30d),
			All:     hashtagActivityFromStore(row.Activity.All),
		},
	}
}

func hashtagActivityFromStore(row store.HashtagActivity) HashtagActivity {
	return HashtagActivity{
		EventCount:    row.EventCount,
		UniqueAuthors: row.UniqueAuthors,
	}
}

func relatedHashtagFromStore(row store.RelatedHashtag) RelatedHashtag {
	return RelatedHashtag{
		Hashtag:             row.Hashtag,
		CoOccurrenceCount:   row.CoOccurrenceCount,
		CoOccurrenceAuthors: row.CoOccurrenceAuthors,
	}
}

func eventDomainLinkFromStore(row store.EventDomainLinkProjection) EventDomainLink {
	return EventDomainLink{
		EventID: row.EventID,
		URL:     row.URL,
		Domain:  row.Domain,
	}
}

func domainStatFromStore(row store.DomainStatProjection) DomainStat {
	return DomainStat{
		Domain:        row.Domain,
		LinkCount:     row.LinkCount,
		NoteCount:     row.NoteCount,
		UniqueAuthors: row.UniqueAuthors,
	}
}

func domainActivityFromStore(row store.DomainActivityProjection) DomainActivity {
	return DomainActivity{
		LinkCount:     row.LinkCount,
		NoteCount:     row.NoteCount,
		UniqueAuthors: row.UniqueAuthors,
	}
}

func domainSummaryFromStore(row store.DomainSummaryProjection) DomainSummary {
	out := DomainSummary{
		Domain:        row.Domain,
		LatestEventAt: row.LatestEventAt,
		Activity: DomainActivityStats{
			Last24h: domainActivityFromStore(row.Activity.Last24h),
			Last7d:  domainActivityFromStore(row.Activity.Last7d),
			Last30d: domainActivityFromStore(row.Activity.Last30d),
			All:     domainActivityFromStore(row.Activity.All),
		},
		RecentNotes: make([]TrendingNote, 0, len(row.RecentNotes)),
		TopNotes:    make([]TrendingNote, 0, len(row.TopNotes)),
	}
	for _, note := range row.RecentNotes {
		out.RecentNotes = append(out.RecentNotes, trendingNoteFromStore(note))
	}
	for _, note := range row.TopNotes {
		out.TopNotes = append(out.TopNotes, trendingNoteFromStore(note))
	}
	return out
}

func trendingNoteFromStore(row store.TrendingNote) TrendingNote {
	return TrendingNote{
		EventID:       row.EventID,
		AuthorPubkey:  row.AuthorPubkey,
		CreatedAt:     row.CreatedAt,
		Content:       row.Content,
		Language:      row.Language,
		ReplyCount:    row.ReplyCount,
		RepostCount:   row.RepostCount,
		ReactionCount: row.ReactionCount,
		ZapCount:      row.ZapCount,
		ZapMSats:      row.ZapMSats,
		Score:         row.Score,
	}
}

func hotConversationFromStore(row store.HotConversation) HotConversation {
	return HotConversation{
		RootEventID:      row.RootEventID,
		AuthorPubkey:     row.AuthorPubkey,
		CreatedAt:        row.CreatedAt,
		Content:          row.Content,
		ReplyCount:       row.ReplyCount,
		ParticipantCount: row.ParticipantCount,
		LastActivityAt:   row.LastActivityAt,
		Replies24h:       row.Replies24h,
		Replies7d:        row.Replies7d,
		VelocityScore:    row.VelocityScore,
		Consistency:      row.Consistency,
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

func relatedProfileFromStore(row store.RelatedProfile) RelatedProfile {
	return RelatedProfile{
		Pubkey:               row.Pubkey,
		TopicOverlap:         row.TopicOverlap,
		ReplyAdjacency:       row.ReplyAdjacency,
		InteractionAdjacency: row.InteractionAdjacency,
		QuoteRepostAdjacency: row.QuoteRepostAdjacency,
		Reasons:              row.Reasons,
		Score:                row.Score,
	}
}
