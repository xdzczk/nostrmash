package query

import (
	"fmt"

	"github.com/xdzczk/nostrmash/internal/store"
)

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
