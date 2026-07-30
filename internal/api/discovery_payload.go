package api

import (
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

const discoveryRankingVersion = "discovery-v1"

func discoveryConfidence(sample int64) string {
	switch {
	case sample >= 100:
		return "high"
	case sample >= 10:
		return "medium"
	default:
		return "low"
	}
}

func addDiscoveryListMeta(
	payload map[string]any,
	window string,
	computedAt *time.Time,
	itemCount int,
) {
	payload["computed_at"] = computedAt
	payload["ranking_version"] = discoveryRankingVersion
	payload["meta"] = query.DiscoveryListMeta{
		Window:         window,
		ComputedAt:     computedAt,
		RankingVersion: discoveryRankingVersion,
		Confidence:     discoveryConfidence(int64(itemCount)),
	}
}

func discoveryReason(code, metric string, value float64, unit string) query.DiscoveryReason {
	return query.DiscoveryReason{
		Code: code,
		Evidence: []query.DiscoveryEvidence{{
			Metric: metric,
			Value:  value,
			Unit:   unit,
		}},
	}
}

func buildNoteRanking(note query.TrendingNote, rank int) query.DiscoveryItemRanking {
	reasons := make([]query.DiscoveryReason, 0, 4)
	if note.ReplyCount > 0 {
		reasons = append(reasons, discoveryReason("reply_velocity", "reply_count", float64(note.ReplyCount), "replies"))
	}
	if note.RepostCount > 0 {
		reasons = append(reasons, discoveryReason("repost_lift", "repost_count", float64(note.RepostCount), "reposts"))
	}
	if note.ReactionCount > 0 {
		reasons = append(reasons, discoveryReason("recent_engagement", "reaction_count", float64(note.ReactionCount), "reactions"))
	}
	if note.ZapMSats > 0 {
		reasons = append(reasons, discoveryReason("zap_support", "zap_msats", float64(note.ZapMSats), "msats"))
	}
	sample := note.ReplyCount + note.RepostCount + note.ReactionCount + note.ZapCount
	return query.DiscoveryItemRanking{
		Rank:       rank,
		Score:      note.Score,
		Reasons:    reasons,
		Confidence: discoveryConfidence(sample),
	}
}

func buildProfileRanking(profile query.TrendingProfile, rank int) query.DiscoveryItemRanking {
	reasons := make([]query.DiscoveryReason, 0, 3)
	if profile.RecentNewFollowers > 0 {
		reasons = append(reasons, discoveryReason("follower_growth", "recent_new_followers", float64(profile.RecentNewFollowers), "followers"))
	}
	if profile.RecentPostCount > 0 {
		reasons = append(reasons, discoveryReason("publishing_momentum", "recent_post_count", float64(profile.RecentPostCount), "notes"))
	}
	if profile.RecentEngagementReceived > 0 {
		reasons = append(reasons, discoveryReason("engagement_received", "recent_engagement_received", float64(profile.RecentEngagementReceived), "interactions"))
	}
	sample := profile.RecentNewFollowers + profile.RecentPostCount + profile.RecentEngagementReceived
	return query.DiscoveryItemRanking{
		Rank:       rank,
		Score:      profile.Score,
		Reasons:    reasons,
		Confidence: discoveryConfidence(sample),
	}
}

func buildHashtagRanking(topic query.TrendingHashtag, rank int) query.DiscoveryItemRanking {
	sourceBreadth := topic.UniqueAuthors
	reasons := []query.DiscoveryReason{
		discoveryReason("mention_volume", "event_count", float64(topic.EventCount), "events"),
		discoveryReason("author_breadth", "unique_authors", float64(topic.UniqueAuthors), "authors"),
	}
	return query.DiscoveryItemRanking{
		Rank:          rank,
		Score:         float64(topic.EventCount),
		Reasons:       reasons,
		SourceBreadth: &sourceBreadth,
		Confidence:    discoveryConfidence(topic.UniqueAuthors),
	}
}

func buildDomainRanking(row query.DomainSummary, rank int, window string) query.DiscoveryItemRanking {
	activity := row.Activity.Last24h
	if window == "7d" {
		activity = row.Activity.Last7d
	}
	sourceBreadth := activity.UniqueAuthors
	reasons := []query.DiscoveryReason{
		discoveryReason("link_circulation", "link_count", float64(activity.LinkCount), "links"),
		discoveryReason("author_breadth", "unique_authors", float64(activity.UniqueAuthors), "authors"),
	}
	return query.DiscoveryItemRanking{
		Rank:          rank,
		Score:         float64(activity.LinkCount),
		Reasons:       reasons,
		SourceBreadth: &sourceBreadth,
		Confidence:    discoveryConfidence(activity.UniqueAuthors),
	}
}

func buildDiscoveryHashtagItems(rows []query.TrendingHashtag) []map[string]any {
	items := make([]map[string]any, 0, len(rows))
	for index, topic := range rows {
		items = append(items, map[string]any{
			"hashtag":        topic.Hashtag,
			"event_count":    topic.EventCount,
			"unique_authors": topic.UniqueAuthors,
			"ranking":        buildHashtagRanking(topic, index+1),
		})
	}
	return items
}

func buildDiscoveryDomainItems(rows []query.DomainSummary, window string) []map[string]any {
	items := make([]map[string]any, 0, len(rows))
	for index, row := range rows {
		items = append(items, map[string]any{
			"domain":          row.Domain,
			"latest_event_at": row.LatestEventAt,
			"link_count":      row.Activity.Last7d.LinkCount,
			"note_count":      row.Activity.Last7d.NoteCount,
			"unique_authors":  row.Activity.Last7d.UniqueAuthors,
			"trend_windows": map[string]any{
				"24h": map[string]any{
					"link_count":     row.Activity.Last24h.LinkCount,
					"note_count":     row.Activity.Last24h.NoteCount,
					"unique_authors": row.Activity.Last24h.UniqueAuthors,
				},
				"7d": map[string]any{
					"link_count":     row.Activity.Last7d.LinkCount,
					"note_count":     row.Activity.Last7d.NoteCount,
					"unique_authors": row.Activity.Last7d.UniqueAuthors,
				},
			},
			"ranking": buildDomainRanking(row, index+1, window),
		})
	}
	return items
}

func buildConversationRanking(conversation query.HotConversation, rank int) query.DiscoveryItemRanking {
	sourceBreadth := int64(conversation.ParticipantCount)
	reasons := []query.DiscoveryReason{
		discoveryReason("conversation_velocity", "velocity_score", conversation.VelocityScore, "score"),
		discoveryReason("reply_velocity", "replies_24h", float64(conversation.Replies24h), "replies"),
		discoveryReason("participant_breadth", "participant_count", float64(conversation.ParticipantCount), "participants"),
	}
	return query.DiscoveryItemRanking{
		Rank:          rank,
		Score:         conversation.VelocityScore,
		Reasons:       reasons,
		SourceBreadth: &sourceBreadth,
		Confidence:    discoveryConfidence(sourceBreadth),
	}
}

func buildRelayStatsPayload(stats query.PublicDiscoveryNetworkStats) map[string]any {
	relays := map[string]any{
		"total":      stats.Relays,
		"active_24h": stats.RelaySummary.Active24h,
		"active_7d":  stats.RelaySummary.Active7d,
		"event_volume": map[string]any{
			"24h": stats.RelaySummary.EventVolume.Last24h,
			"7d":  stats.RelaySummary.EventVolume.Last7d,
		},
		"unique_authors": map[string]any{
			"24h": stats.RelaySummary.UniqueAuthors.Last24h,
			"7d":  stats.RelaySummary.UniqueAuthors.Last7d,
		},
	}
	if len(stats.TopRelays) > 0 {
		topRelays := make([]map[string]any, 0, len(stats.TopRelays))
		for _, relay := range stats.TopRelays {
			topRelays = append(topRelays, map[string]any{
				"relay_url":      relay.RelayURL,
				"event_count":    relay.EventCount,
				"unique_authors": relay.UniqueAuthors,
			})
		}
		relays["top"] = topRelays
	}
	return relays
}
