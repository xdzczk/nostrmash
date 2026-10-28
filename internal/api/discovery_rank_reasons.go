package api

import (
	"math"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/query"
)

// rankReasonEpsilon is the gap that counts as "this row's number is smaller
// than a row below it". Ties are allowed: the printed figure must not rise
// down the page, and equal values do not.
const rankReasonEpsilon = 1e-3

// rankedSignal is one candidate explanation. A row keeps the metric even when
// offer is false, so a later row can disqualify an earlier claim of the same
// code. comparable signals are checked against every lower row. bare signals
// are ordered by value but print no number. A non-comparable signal (the
// combined-engagement fallback) is eligible whenever the walk reaches it.
type rankedSignal struct {
	code       string
	metric     string
	value      float64
	unit       string
	offer      bool
	comparable bool
	bare       bool
}

func chooseRankReasons(rows [][]rankedSignal) [][]query.DiscoveryReason {
	out := make([][]query.DiscoveryReason, len(rows))
	for i, signals := range rows {
		out[i] = []query.DiscoveryReason{}
		for idx := range signals {
			candidate := signals[idx]
			if !candidate.offer {
				continue
			}
			if candidate.comparable && !signalMonotonic(rows, i, candidate.code, candidate.value) {
				continue
			}
			out[i] = []query.DiscoveryReason{reasonFromSignal(candidate)}
			break
		}
	}
	return out
}

func signalMonotonic(rows [][]rankedSignal, index int, code string, value float64) bool {
	for j := index + 1; j < len(rows); j++ {
		later, ok := signalByCode(rows[j], code)
		if !ok || !later.comparable {
			continue
		}
		if later.value > value+rankReasonEpsilon {
			return false
		}
	}
	return true
}

func signalByCode(signals []rankedSignal, code string) (rankedSignal, bool) {
	for _, signal := range signals {
		if signal.code == code {
			return signal, true
		}
	}
	return rankedSignal{}, false
}

func reasonFromSignal(signal rankedSignal) query.DiscoveryReason {
	if signal.bare || signal.metric == "" {
		return query.DiscoveryReason{Code: signal.code}
	}
	return discoveryReason(signal.code, signal.metric, signal.value, signal.unit)
}

func noteRankSignals(note query.TrendingNote) []rankedSignal {
	return []rankedSignal{
		countSignal("reply_velocity", "reply_count", note.ReplyCount, "replies"),
		countSignal("zap_support", "zap_count", note.ZapCount, "zaps"),
		countSignal("recent_engagement", "reaction_count", note.ReactionCount, "reactions"),
		countSignal("repost_lift", "repost_count", note.RepostCount, "reposts"),
		{
			code:       "recency",
			value:      float64(note.CreatedAt),
			offer:      note.CreatedAt > 0,
			comparable: true,
			bare:       true,
		},
		{code: "combined_engagement", offer: true},
	}
}

func countSignal(code, metric string, value int64, unit string) rankedSignal {
	return rankedSignal{
		code:       code,
		metric:     metric,
		value:      float64(value),
		unit:       unit,
		offer:      value > 0,
		comparable: true,
	}
}

func buildNoteRankings(notes []query.TrendingNote) []query.DiscoveryItemRanking {
	signals := make([][]rankedSignal, len(notes))
	for i, note := range notes {
		signals[i] = noteRankSignals(note)
	}
	reasons := chooseRankReasons(signals)
	out := make([]query.DiscoveryItemRanking, len(notes))
	for i, note := range notes {
		sample := note.ReplyCount + note.RepostCount + note.ReactionCount + note.ZapCount
		out[i] = query.DiscoveryItemRanking{
			Rank:       i + 1,
			Score:      note.Score,
			Reasons:    reasons[i],
			Confidence: discoveryConfidence(sample),
		}
	}
	return out
}

func profileTrendingSignals(profile query.TrendingProfile, window string) []rankedSignal {
	engagement := profileRankingEngagement(profile)
	// Root notes only. Replies are not the posts the trending score divides by,
	// and recent_post_count is already that root-note counter. It is the 24h
	// window only, so a 7d rate would divide 7d engagement by 24h posts and
	// the 7d list never cites it.
	totalPosts := profile.RecentPostCount
	signals := make([]rankedSignal, 0, 4)
	if window != "7d" && window != "30d" {
		rate := 0.0
		if totalPosts > 0 {
			rate = engagement / (1 + float64(totalPosts))
		}
		signals = append(signals, rankedSignal{
			code:       "engagement_quality",
			metric:     "engagement_per_post",
			value:      rate,
			unit:       "interactions per note",
			offer:      totalPosts > 0 && engagement > 0 && rate >= 1,
			comparable: true,
		})
	}
	signals = append(signals, rankedSignal{
		code:       "engagement_received",
		metric:     "recent_engagement_received",
		value:      engagement,
		unit:       "interactions",
		offer:      engagement >= 1,
		comparable: true,
	})
	if profile.RecentActivityAt != nil && *profile.RecentActivityAt > 0 {
		signals = append(signals, rankedSignal{
			code:       "recency",
			value:      float64(*profile.RecentActivityAt),
			offer:      true,
			comparable: true,
			bare:       true,
		})
	}
	if profile.Score > 0 || engagement > 0 {
		signals = append(signals, rankedSignal{code: "combined_engagement", offer: true})
	}
	return signals
}

func profileRisingSignals(profile query.TrendingProfile) []rankedSignal {
	newFollowers := profileRankingNewFollowers(profile)
	engagement := profileRankingEngagement(profile)
	_, _, momentum := derivation.FollowerMomentum(profile.FollowerCount, newFollowers)
	// The stored rising score is follower momentum plus the engagement side.
	// Momentum dominates when it is at least that remainder.
	residual := profile.Score - momentum
	followerDominates := momentum > 0 && momentum+rankReasonEpsilon >= residual
	followers := profile.FollowerCount
	if followers < 0 {
		followers = 0
	}
	perFollower := engagement / (1 + float64(followers))
	signals := []rankedSignal{
		{
			code:       "follower_growth",
			metric:     "follower_momentum",
			value:      momentum,
			unit:       "followers",
			offer:      followerDominates && momentum >= 2,
			comparable: true,
		},
		{
			code:       "relative_engagement_growth",
			metric:     "engagement_per_follower",
			value:      perFollower,
			unit:       "interactions per follower",
			offer:      engagement > float64(risingEngagementReasonFloor) && perFollower >= 1,
			comparable: true,
		},
		{
			code:       "engagement_received",
			metric:     "recent_engagement_received",
			value:      engagement,
			unit:       "interactions",
			offer:      engagement > float64(risingEngagementReasonFloor),
			comparable: true,
		},
	}
	if profile.RecentActivityAt != nil && *profile.RecentActivityAt > 0 {
		signals = append(signals, rankedSignal{
			code:       "recency",
			value:      float64(*profile.RecentActivityAt),
			offer:      true,
			comparable: true,
			bare:       true,
		})
	}
	if profile.Score > 0 || engagement > 0 || momentum > 0 {
		signals = append(signals, rankedSignal{code: "combined_engagement", offer: true})
	}
	return signals
}

func assignProfileRankings(rows []query.TrendingProfile, surface, window string) []query.DiscoveryItemRanking {
	signals := make([][]rankedSignal, len(rows))
	for i, profile := range rows {
		if surface == discoverySurfaceTrending {
			signals[i] = profileTrendingSignals(profile, window)
		} else {
			signals[i] = profileRisingSignals(profile)
		}
	}
	reasons := chooseRankReasons(signals)
	out := make([]query.DiscoveryItemRanking, len(rows))
	for i, profile := range rows {
		engagement := profileRankingEngagement(profile)
		var sample float64
		if surface == discoverySurfaceTrending {
			sample = float64(profile.RecentPostCount) + engagement
		} else {
			sample = profileRankingNewFollowers(profile) + engagement
		}
		out[i] = query.DiscoveryItemRanking{
			Rank:       i + 1,
			Score:      profile.Score,
			Reasons:    reasons[i],
			Confidence: discoveryConfidence(int64(math.Round(sample))),
		}
	}
	return out
}
