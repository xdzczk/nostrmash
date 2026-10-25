package api

import (
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

const discoveryRankingVersion = "discovery-v1"

// Profile discovery surfaces. "trending" backs the engagement-per-post
// ranking ("Profiles in motion" on the frontend); "rising" backs the
// small-account-momentum ranking ("Up and coming"). Confidence sampling in
// assignProfileRankings differs per surface because the two underlying scores
// (computeProfileTrendingScore / computeProfileRisingScore in
// internal/derivation/discovery_scores.go) weigh different signals. The
// printed reason is chosen later, across the whole page, so it never cites a
// number that rises down the list.
const (
	discoverySurfaceTrending = "trending"
	discoverySurfaceRising   = "rising"
)

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

// risingEngagementReasonFloor mirrors computeProfileRisingScore's
// risingEngagementNoiseFloor: a single (weighted) interaction contributes
// nothing to the rising score, so it must not be advertised as an
// engagement-based reason either.
const risingEngagementReasonFloor = 1

func profileRankingEngagement(profile query.TrendingProfile) float64 {
	if profile.ScoredEngagementReceived != nil {
		return *profile.ScoredEngagementReceived
	}
	return float64(profile.RecentEngagementReceived)
}

func profileRankingNewFollowers(profile query.TrendingProfile) float64 {
	if profile.ScoredNewFollowers != nil {
		return *profile.ScoredNewFollowers
	}
	return float64(profile.RecentNewFollowers)
}

func buildProfileRanking(profile query.TrendingProfile, rank int, surface string) query.DiscoveryItemRanking {
	ranking := assignProfileRankings([]query.TrendingProfile{profile}, surface, "24h")[0]
	ranking.Rank = rank
	return ranking
}

func buildHashtagRanking(topic query.TrendingHashtag, rank int) query.DiscoveryItemRanking {
	sourceBreadth := topic.UniqueAuthors
	reasons := []query.DiscoveryReason{
		discoveryReason("author_breadth", "unique_authors", float64(topic.UniqueAuthors), "authors"),
		discoveryReason("mention_volume", "event_count", float64(topic.EventCount), "events"),
	}
	return query.DiscoveryItemRanking{
		Rank:          rank,
		Score:         float64(topic.UniqueAuthors),
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
		discoveryReason("author_breadth", "unique_authors", float64(activity.UniqueAuthors), "authors"),
		discoveryReason("link_circulation", "link_count", float64(activity.LinkCount), "links"),
	}
	return query.DiscoveryItemRanking{
		Rank:          rank,
		Score:         float64(activity.UniqueAuthors),
		Reasons:       reasons,
		SourceBreadth: &sourceBreadth,
		Confidence:    discoveryConfidence(activity.UniqueAuthors),
	}
}

// pickHashtagWindow selects the 24h or 7d list out of a snapshot's
// TrendingHashtagWindows for the given window label ("24h" or "7d"),
// returning nil if windows is nil (capability unavailable / snapshot empty).
func pickHashtagWindow(windows *query.TrendingHashtagWindows, window string) []query.TrendingHashtag {
	if windows == nil {
		return nil
	}
	if window == "7d" {
		return windows.Last7d
	}
	return windows.Last24h
}

// sliceHashtags bounds rows to at most limit entries. Snapshots are
// materialized at the API max (50) so callers can request any smaller limit
// from the same precomputed list.
func sliceHashtags(rows []query.TrendingHashtag, limit int) []query.TrendingHashtag {
	if limit <= 0 || len(rows) <= limit {
		return rows
	}
	return rows[:limit]
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

func buildRelayStatsPayload(stats query.PublicDiscoveryNetworkStats, window string) map[string]any {
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
	if selected := relayTopForWindow(stats, window); len(selected) > 0 {
		relays["top"] = relayActivityJSON(selected)
	}
	if len(stats.TopRelays24h) > 0 {
		relays["top_24h"] = relayActivityJSON(stats.TopRelays24h)
	}
	if top7d := relayTopForWindow(stats, "7d"); len(top7d) > 0 {
		relays["top_7d"] = relayActivityJSON(top7d)
	}
	return relays
}

func relayTopForWindow(stats query.PublicDiscoveryNetworkStats, window string) []query.RelayUsageSummary {
	if window == "24h" {
		return stats.TopRelays24h
	}
	if len(stats.TopRelays7d) > 0 {
		return stats.TopRelays7d
	}
	return stats.TopRelays
}

func relayActivityJSON(rows []query.RelayUsageSummary) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, relay := range rows {
		out = append(out, map[string]any{
			"relay_url":      relay.RelayURL,
			"event_count":    relay.EventCount,
			"unique_authors": relay.UniqueAuthors,
		})
	}
	return out
}
