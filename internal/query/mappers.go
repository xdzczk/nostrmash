package query

import (
	"fmt"

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

func authorAnalyticsSummaryFromStore(pubkey string, rows []store.AuthorAnalyticsSummaryProjection) AuthorAnalyticsSummary {
	out := AuthorAnalyticsSummary{
		Pubkey:  pubkey,
		Windows: make([]AuthorAnalyticsWindowSummary, 0, len(rows)),
	}
	for _, row := range rows {
		window := AuthorAnalyticsWindowSummary{
			Window:                   fmt.Sprintf("%dd", row.WindowDays),
			PostCount:                row.PostCount,
			NoteCount:                row.NoteCount,
			ReplyCount:               row.ReplyCount,
			ActiveDays:               row.ActiveDays,
			EngagementReceived:       row.EngagementReceived,
			EngagementGiven:          row.EngagementGiven,
			CadencePostsPerDay:       row.CadencePostsPerDay,
			CadencePostsPerActiveDay: row.CadencePostsPerActiveDay,
			RecentActivityAt:         row.RecentActivityAt,
			MediaMix:                 authorMediaMixFromStore(row.MediaMix),
			QuoteRepost: AuthorAnalyticsQuoteRepostWindow{
				QuotesMade:      row.QuoteRepost.QuotesMade,
				RepostsMade:     row.QuoteRepost.RepostsMade,
				QuotesReceived:  row.QuoteRepost.QuotesReceived,
				RepostsReceived: row.QuoteRepost.RepostsReceived,
			},
		}
		out.Windows = append(out.Windows, window)
	}
	return out
}

func authorRelayFootprintFromStore(row store.AuthorRelayFootprintProjection) AuthorRelayFootprintSummary {
	out := AuthorRelayFootprintSummary{
		RelayCount:       row.RelayCount,
		SeenOnEventCount: row.SeenOnEventCount,
		TopRelays:        make([]RelayUsageSummary, 0, len(row.TopRelays)),
	}
	for _, relay := range row.TopRelays {
		out.TopRelays = append(out.TopRelays, relayUsageFromStore(relay))
	}
	return out
}

func quoteRepostActivityFromStore(row store.QuoteRepostActivityProjection) QuoteRepostActivity {
	return QuoteRepostActivity{
		EventID:     row.EventID,
		ActorPubkey: row.ActorPubkey,
		CreatedAt:   row.CreatedAt,
		Action:      row.Action,
		Quote:       row.Quote,
		LinkedNote: QuoteRepostLinkedNoteSummary{
			EventID:      row.LinkedNote.EventID,
			AuthorPubkey: row.LinkedNote.AuthorPubkey,
			CreatedAt:    row.LinkedNote.CreatedAt,
			Content:      row.LinkedNote.Content,
		},
	}
}

func noteQuoteRepostLinkageFromStore(row store.NoteQuoteRepostLinkageProjection) NoteQuoteRepostLinkageSummary {
	out := NoteQuoteRepostLinkageSummary{
		EventID:        row.EventID,
		QuoteCount:     row.QuoteCount,
		RepostCount:    row.RepostCount,
		RecentActivity: make([]QuoteRepostActivity, 0, len(row.RecentActivity)),
	}
	for _, activity := range row.RecentActivity {
		out.RecentActivity = append(out.RecentActivity, quoteRepostActivityFromStore(activity))
	}
	return out
}

func authorTopicStatFromStore(row store.AuthorTopicStatsProjection) AuthorTopicStat {
	return AuthorTopicStat{
		Hashtag:    row.Hashtag,
		UsageCount: row.UsageCount,
		ActiveDays: row.ActiveDays,
	}
}

func authorMediaMixFromStore(row store.AuthorMediaMixStatsProjection) AuthorAnalyticsMediaMix {
	return AuthorAnalyticsMediaMix{
		TotalPosts:           row.TotalPosts,
		WithImageCount:       row.WithImageCount,
		WithVideoCount:       row.WithVideoCount,
		WithLinkCount:        row.WithLinkCount,
		WithArticleCount:     row.WithArticleCount,
		TextOnlyCount:        row.TextOnlyCount,
		TotalAttachmentCount: row.TotalAttachmentCount,
	}
}

func authorActivityWindowsFromStore(
	pubkey string,
	windowDays int,
	rows []store.AuthorActivityWindowBucketProjection,
) AuthorActivityWindows {
	byHour := make([]AuthorHourlyEngagementWindow, 24)
	for hour := 0; hour < 24; hour++ {
		byHour[hour] = AuthorHourlyEngagementWindow{HourOfDay: hour}
	}
	byDay := make([]AuthorDailyEngagementWindow, 7)
	for day := 0; day < 7; day++ {
		byDay[day] = AuthorDailyEngagementWindow{DayOfWeek: day}
	}
	heatmap := make([]AuthorEngagementHeatmapBucket, 0, len(rows))
	for _, row := range rows {
		if row.HourOfDay >= 0 && row.HourOfDay < 24 {
			byHour[row.HourOfDay].EngagementReceived += row.EngagementReceived
			byHour[row.HourOfDay].ReplyReceived += row.ReplyReceived
			byHour[row.HourOfDay].ReactionReceived += row.ReactionReceived
			byHour[row.HourOfDay].RepostReceived += row.RepostReceived
			byHour[row.HourOfDay].ZapReceived += row.ZapReceived
		}
		if row.DayOfWeek >= 0 && row.DayOfWeek < 7 {
			byDay[row.DayOfWeek].EngagementReceived += row.EngagementReceived
			byDay[row.DayOfWeek].ReplyReceived += row.ReplyReceived
			byDay[row.DayOfWeek].ReactionReceived += row.ReactionReceived
			byDay[row.DayOfWeek].RepostReceived += row.RepostReceived
			byDay[row.DayOfWeek].ZapReceived += row.ZapReceived
		}
		heatmap = append(heatmap, AuthorEngagementHeatmapBucket{
			DayOfWeek:          row.DayOfWeek,
			HourOfDay:          row.HourOfDay,
			EngagementReceived: row.EngagementReceived,
			ReplyReceived:      row.ReplyReceived,
			ReactionReceived:   row.ReactionReceived,
			RepostReceived:     row.RepostReceived,
			ZapReceived:        row.ZapReceived,
		})
	}
	return AuthorActivityWindows{
		Pubkey:   pubkey,
		Window:   fmt.Sprintf("%dd", windowDays),
		Timezone: "UTC",
		ByHour:   byHour,
		ByDay:    byDay,
		Heatmap:  heatmap,
	}
}

func authorPostingPatternsFromStore(
	pubkey string,
	windowDays int,
	rows []store.AuthorPostingPatternBucketProjection,
) AuthorPostingPatterns {
	byHour := make([]AuthorHourlyPostingPattern, 24)
	for hour := 0; hour < 24; hour++ {
		byHour[hour] = AuthorHourlyPostingPattern{HourOfDay: hour}
	}
	byDay := make([]AuthorDailyPostingPattern, 7)
	for day := 0; day < 7; day++ {
		byDay[day] = AuthorDailyPostingPattern{DayOfWeek: day}
	}
	heatmap := make([]AuthorPostingHeatmapBucket, 0, len(rows))
	for _, row := range rows {
		if row.HourOfDay >= 0 && row.HourOfDay < 24 {
			byHour[row.HourOfDay].PostCount += row.PostCount
			byHour[row.HourOfDay].NoteCount += row.NoteCount
			byHour[row.HourOfDay].ReplyCount += row.ReplyCount
		}
		if row.DayOfWeek >= 0 && row.DayOfWeek < 7 {
			byDay[row.DayOfWeek].PostCount += row.PostCount
			byDay[row.DayOfWeek].NoteCount += row.NoteCount
			byDay[row.DayOfWeek].ReplyCount += row.ReplyCount
		}
		heatmap = append(heatmap, AuthorPostingHeatmapBucket{
			DayOfWeek:  row.DayOfWeek,
			HourOfDay:  row.HourOfDay,
			PostCount:  row.PostCount,
			NoteCount:  row.NoteCount,
			ReplyCount: row.ReplyCount,
		})
	}
	return AuthorPostingPatterns{
		Pubkey:   pubkey,
		Window:   fmt.Sprintf("%dd", windowDays),
		Timezone: "UTC",
		ByHour:   byHour,
		ByDay:    byDay,
		Heatmap:  heatmap,
	}
}

func authorTopNoteFromStore(row store.AuthorTopNoteProjection) AuthorTopNote {
	out := AuthorTopNote{
		EventID:            row.EventID,
		CreatedAt:          row.CreatedAt,
		Content:            row.Content,
		ReplyCount:         row.ReplyCount,
		ReactionCount:      row.ReactionCount,
		RepostCount:        row.RepostCount,
		ZapCount:           row.ZapCount,
		ZapMSats:           row.ZapMSats,
		WeightedEngagement: row.WeightedEngagement,
		MediaSegment:       row.MediaSegment,
	}
	if row.PrimaryTopicHashtag != nil {
		out.PrimaryTopic = *row.PrimaryTopicHashtag
	}
	return out
}

func authorRecycleCandidateFromStore(row store.AuthorRecycleCandidateProjection) AuthorRecycleCandidate {
	out := AuthorRecycleCandidate{
		EventID:               row.EventID,
		CreatedAt:             row.CreatedAt,
		Content:               row.Content,
		ReplyCount:            row.ReplyCount,
		ReactionCount:         row.ReactionCount,
		RepostCount:           row.RepostCount,
		ZapCount:              row.ZapCount,
		ZapMSats:              row.ZapMSats,
		WeightedEngagement:    row.WeightedEngagement,
		PerformancePercentile: row.PerformancePercentile,
		IsReply:               row.IsReply,
		HasRecentRepostMarker: row.HasRecentRepostMarker,
		MediaSegment:          row.MediaSegment,
	}
	if row.PrimaryTopicHashtag != nil {
		out.PrimaryTopic = *row.PrimaryTopicHashtag
	}
	return out
}

func authorPerformanceSummaryFromStore(
	pubkey string,
	windowDays int,
	current store.AuthorPerformanceAggregateProjection,
	previous store.AuthorPerformanceAggregateProjection,
	mediaMix store.AuthorMediaMixStatsProjection,
	topics []store.AuthorTopicStatsProjection,
) AuthorPerformanceSummary {
	out := AuthorPerformanceSummary{
		Pubkey:                  pubkey,
		Window:                  fmt.Sprintf("%dd", windowDays),
		NoteCount:               current.NoteCount,
		TotalWeightedEngagement: current.TotalWeightedEngagement,
		WeightedEngagement: AuthorPerformanceStats{
			Average: current.AverageWeightedEngagement,
			Median:  current.MedianWeightedEngagement,
		},
		ReplyCount: AuthorPerformanceStats{
			Average: current.AverageReplyCount,
			Median:  current.MedianReplyCount,
		},
		ReactionCount: AuthorPerformanceStats{
			Average: current.AverageReactionCount,
			Median:  current.MedianReactionCount,
		},
		RepostCount: AuthorPerformanceStats{
			Average: current.AverageRepostCount,
			Median:  current.MedianRepostCount,
		},
		ZapCount: AuthorPerformanceStats{
			Average: current.AverageZapCount,
			Median:  current.MedianZapCount,
		},
		Totals: AuthorPerformanceTotals{
			ReplyCount:    current.TotalReplyCount,
			ReactionCount: current.TotalReactionCount,
			RepostCount:   current.TotalRepostCount,
			ZapCount:      current.TotalZapCount,
			ZapMSats:      current.TotalZapMSats,
		},
		MediaMix:  authorMediaMixFromStore(mediaMix),
		TopTopics: make([]AuthorTopicStat, 0, len(topics)),
		Comparison: AuthorPerformanceComparison{
			Window:                         fmt.Sprintf("previous_%dd", windowDays),
			NoteCountDelta:                 current.NoteCount - previous.NoteCount,
			TotalWeightedEngagementDelta:   current.TotalWeightedEngagement - previous.TotalWeightedEngagement,
			AverageWeightedEngagementDelta: current.AverageWeightedEngagement - previous.AverageWeightedEngagement,
			MedianWeightedEngagementDelta:  current.MedianWeightedEngagement - previous.MedianWeightedEngagement,
		},
	}
	for _, topic := range topics {
		out.TopTopics = append(out.TopTopics, authorTopicStatFromStore(topic))
	}
	return out
}

func groupedNoteAnalyticsFromStore(row store.GroupedNoteAnalyticsProjection) GroupedNoteAnalyticsSummary {
	out := GroupedNoteAnalyticsSummary{
		Pubkey:      row.Pubkey,
		Window:      fmt.Sprintf("%dd", row.WindowDays),
		GroupKind:   row.GroupKind,
		GroupKey:    row.GroupKey,
		MetadataTag: row.MetadataTag,
		NoteCount:   row.NoteCount,
		Engagement: GroupedEngagementTotals{
			ReplyCount:    row.Engagement.ReplyCount,
			ReactionCount: row.Engagement.ReactionCount,
			RepostCount:   row.Engagement.RepostCount,
			ZapCount:      row.Engagement.ZapCount,
			ZapMSats:      row.Engagement.ZapMSats,
		},
		Media: GroupedMediaSummary{
			TotalPosts:           row.Media.TotalPosts,
			WithImageCount:       row.Media.WithImageCount,
			WithVideoCount:       row.Media.WithVideoCount,
			WithLinkCount:        row.Media.WithLinkCount,
			WithArticleCount:     row.Media.WithArticleCount,
			TextOnlyCount:        row.Media.TextOnlyCount,
			TotalAttachmentCount: row.Media.TotalAttachmentCount,
		},
		TopNotes:  make([]GroupedTopNote, 0, len(row.TopNotes)),
		TopTopics: make([]GroupedTopicSummary, 0, len(row.TopTopics)),
	}
	for _, note := range row.TopNotes {
		topNote := GroupedTopNote{
			EventID:            note.EventID,
			CreatedAt:          note.CreatedAt,
			Content:            note.Content,
			ReplyCount:         note.ReplyCount,
			ReactionCount:      note.ReactionCount,
			RepostCount:        note.RepostCount,
			ZapCount:           note.ZapCount,
			ZapMSats:           note.ZapMSats,
			WeightedEngagement: note.WeightedEngagement,
			MediaSegment:       note.MediaSegment,
		}
		if note.PrimaryTopicHashtag != nil {
			topNote.PrimaryTopic = *note.PrimaryTopicHashtag
		}
		out.TopNotes = append(out.TopNotes, topNote)
	}
	for _, topic := range row.TopTopics {
		out.TopTopics = append(out.TopTopics, GroupedTopicSummary{
			Hashtag:    topic.Hashtag,
			UsageCount: topic.UsageCount,
			ActiveDays: topic.ActiveDays,
		})
	}
	return out
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

func languageSummaryFromStore(row store.LanguageSummary) LanguageSummary {
	return LanguageSummary{
		Language: row.Language,
		Count:    row.Count,
	}
}

func relayUsageFromStore(row store.RelayUsageSummary) RelayUsageSummary {
	return RelayUsageSummary{
		RelayURL:      row.RelayURL,
		EventCount:    row.EventCount,
		UniqueAuthors: row.UniqueAuthors,
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

func threadSummaryFromStore(row store.ThreadSummaryProjection) ThreadSummary {
	return ThreadSummary{
		RootEventID:      row.RootEventID,
		ReplyCount:       row.ReplyCount,
		ParticipantCount: row.ParticipantCount,
		MaxDepth:         row.MaxDepth,
		LastActivityAt:   row.LastActivityAt,
		Velocity: ThreadVelocityHints{
			Replies24h: row.Replies24h,
			Replies7d:  row.Replies7d,
		},
		Consistency: row.Consistency,
	}
}

func noteStatsFromStore(row store.NoteStats) (NoteEngagementCounts, NoteMediaFlags) {
	return NoteEngagementCounts{
			ReplyCount:    row.ReplyCount,
			ReactionCount: row.ReactionCount,
			RepostCount:   row.RepostCount,
			ZapCount:      row.ZapCount,
			ZapMSats:      row.ZapMSats,
		}, NoteMediaFlags{
			HasImage:        row.HasImage,
			HasVideo:        row.HasVideo,
			HasLink:         row.HasLink,
			HasArticle:      row.HasArticle,
			AttachmentCount: row.AttachmentCount,
		}
}

func noteConversationActivityFromStore(row store.NoteConversationVelocity) NoteConversationActivity {
	return NoteConversationActivity{
		Replies24h: row.Replies24h,
		Replies7d:  row.Replies7d,
	}
}

func relatedNoteFromStore(row store.RelatedNote) RelatedNote {
	return RelatedNote{
		EventID:      row.EventID,
		AuthorPubkey: row.AuthorPubkey,
		CreatedAt:    row.CreatedAt,
		Content:      row.Content,
		Event:        row.Event,
		Counts: NoteEngagementCounts{
			ReplyCount:    row.ReplyCount,
			ReactionCount: row.ReactionCount,
			RepostCount:   row.RepostCount,
			ZapCount:      row.ZapCount,
			ZapMSats:      row.ZapMSats,
		},
		Reasons: row.Reasons,
		Score:   row.RankScore,
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
