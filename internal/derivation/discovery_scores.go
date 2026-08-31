package derivation

import (
	"math"
	"time"
)

// computeTrendingScore blends windowed engagement into a decayed trending
// score. Engagement inputs are float64 "weights", not raw event counts: the
// projection counts each engager pubkey once per signal (excluding the note
// author), and optionally scales each engager by trust-graph proximity, so a
// single account emitting many events cannot inflate the score.
func computeTrendingScore(
	window time.Duration,
	nowUnix int64,
	noteCreatedAt int64,
	replyWeight float64,
	repostWeight float64,
	reactionWeight float64,
	zapWeight float64,
	zapMSats float64,
) float64 {
	windowSeconds := int64(window / time.Second)
	if windowSeconds <= 0 {
		return 0
	}
	ageSeconds := nowUnix - noteCreatedAt
	if ageSeconds < 0 {
		ageSeconds = 0
	}
	if ageSeconds > windowSeconds {
		return 0
	}
	base := replyWeight*3.0 +
		repostWeight*2.0 +
		reactionWeight*1.0 +
		zapWeight*2.0 +
		(zapMSats / 100000.0)
	decay := 1.0 / (1.0 + (float64(ageSeconds) / float64(windowSeconds)))
	score := base * decay
	if score <= 0 {
		return 0
	}
	return math.Round(score*1000.0) / 1000.0
}

// computeProfileTrendingScore takes float64 engagement/zap inputs so callers
// can pass either raw counters or deduplicated trust-weighted votes.
func computeProfileTrendingScore(
	window time.Duration,
	nowUnix int64,
	recentActivityAt *int64,
	postCount int64,
	replyCount int64,
	engagementReceived float64,
	zapVolumeMSats float64,
	activeDays int,
) float64 {
	windowSeconds := int64(window / time.Second)
	if windowSeconds <= 0 || recentActivityAt == nil {
		return 0
	}
	ageSeconds := nowUnix - *recentActivityAt
	if ageSeconds < 0 {
		ageSeconds = 0
	}
	if ageSeconds > windowSeconds {
		return 0
	}
	safePosts := maxInt64(0, postCount)
	safeReplies := maxInt64(0, replyCount)
	safeEngagement := math.Max(0, engagementReceived)
	safeZapVolume := math.Max(0, zapVolumeMSats)
	safeActiveDays := maxInt64(0, int64(activeDays))
	totalPosts := safePosts + safeReplies
	// "Profiles in motion" is an engagement-per-post ranking. Posting,
	// consistency, and zap volume are secondary signals and must not let a
	// profile with zero (weighted) engagement occupy a slot.
	if safeEngagement <= 0 {
		return 0
	}

	// Posting volume and consistency are deliberately kept as minor,
	// secondary signals: this score is meant to rank profiles by how much
	// engagement their posts earn, not by how often they post. Volume
	// mostly matters through engagementPerPost/qualityBoost below.
	engagementSignal := 3.0 * math.Log1p(safeEngagement)
	postingSignal := 0.35 * math.Log1p(float64(totalPosts))
	zapSignal := math.Log1p(safeZapVolume / 100000.0)
	consistencySignal := 0.5 * math.Log1p(float64(safeActiveDays))

	// qualityBoost rewards engagement-per-post with diminishing (sqrt)
	// returns instead of a hard cap, so accounts with exceptional per-post
	// engagement keep pulling ahead of merely-good ones rather than
	// flattening out at the same boost.
	engagementPerPost := safeEngagement / (1.0 + float64(totalPosts))
	qualityBoost := 1.0 + math.Min(6.0, math.Sqrt(engagementPerPost))
	postingPressure := float64(totalPosts) / (1.0 + safeEngagement + float64(safeActiveDays))
	volumePenalty := 1.0 / (1.0 + math.Max(0.0, postingPressure-1.0))

	base := (engagementSignal + postingSignal + zapSignal + consistencySignal) * qualityBoost * volumePenalty
	decay := 1.0 / (1.0 + (float64(ageSeconds) / float64(windowSeconds)))
	score := base * decay
	if score <= 0 {
		return 0
	}
	return math.Round(score*1000.0) / 1000.0
}

// computeProfileRisingScore takes float64 newFollowers/engagement inputs so
// callers can pass either raw counters or deduplicated trust-weighted votes.
func computeProfileRisingScore(
	trendingScore float64,
	followerCount int64,
	newFollowers float64,
	engagementReceived float64,
	postCount int64,
	replyCount int64,
	activeDays int,
) float64 {
	safeFollowerCount := maxInt64(0, followerCount)
	safeNewFollowers := math.Max(0, newFollowers)
	safeEngagement := math.Max(0, engagementReceived)
	safePosts := maxInt64(0, postCount)
	safeReplies := maxInt64(0, replyCount)
	safeActiveDays := maxInt64(0, int64(activeDays))
	totalPosts := safePosts + safeReplies
	// A handful of new followers (1-2) is common noise for any brand-new
	// account and isn't a meaningful momentum signal on its own -- discount
	// it so accounts with substantial absolute growth reliably outrank
	// barely-started ones, even though both may sit well under the
	// audience-penalty's "small account" ceiling above.
	const risingFollowerNoiseFloor = 2.0
	creditedNewFollowers := math.Max(0, safeNewFollowers-risingFollowerNoiseFloor)
	// Rising is independently reachable via credited follower growth or
	// relative engagement. It must not require a positive trending score:
	// the trending floor now rejects zero-engagement posters, and a small
	// account gaining 60 followers with no measured engagement is still a
	// valid "Up and coming" candidate.
	if trendingScore <= 0 && creditedNewFollowers <= 0 && safeEngagement <= 0 {
		return 0
	}
	// Gentle log through the "small account" range, then a multiplicative
	// hinge past risingAudienceSoftCap so mid/large accounts fall off
	// quickly. A 400-follower account stays in the intended band; a
	// 5,000- or 50,000-follower account needs far more momentum to compete.
	// No hard cutoff: anyone can still appear if the growth is enormous.
	const risingAudienceSoftCap = 500.0
	audiencePenalty := 1.0 + 1.3*math.Log10(1.0+float64(safeFollowerCount))
	if audiencePenalty <= 0 {
		audiencePenalty = 1.0
	}
	if float64(safeFollowerCount) > risingAudienceSoftCap {
		audiencePenalty *= float64(safeFollowerCount) / risingAudienceSoftCap
	}
	followerMomentum := 4.0 * math.Log1p(creditedNewFollowers)

	// relativeEngagementMomentum gives a small account a real path into the
	// ranking purely from getting outsized engagement relative to its own
	// (tiny) audience, independent of whether it gained any new followers.
	// The *100 scale-up keeps typical fractional engagement/follower
	// ratios (e.g. 0.5 engagement per follower) in a range where log1p
	// produces a meaningful signal instead of ~0.
	engagementPerFollower := safeEngagement / (1.0 + float64(safeFollowerCount))
	relativeEngagementMomentum := 3.0 * math.Log1p(engagementPerFollower*100.0)
	engagementMomentum := 0.4*math.Log1p(safeEngagement) + relativeEngagementMomentum
	qualityFactor := 1.0 + math.Min(1.0, safeEngagement/(1.0+float64(totalPosts)))
	postingPressure := float64(totalPosts) / (1.0 + safeEngagement + float64(safeActiveDays))
	volumePenalty := 1.0 / (1.0 + math.Max(0.0, postingPressure-1.0))
	engagementMomentum = engagementMomentum * qualityFactor * volumePenalty
	// Dampen by days of sustained activity only for the engagement
	// component: it counters low-quality volume spread across many days.
	// Follower momentum is a simple lagging count and must NOT shrink just
	// because the account has been around/active longer -- otherwise a
	// week-old account with real growth loses to a same-day account with a
	// trivial handful of new followers.
	if safeActiveDays > 0 {
		engagementMomentum = engagementMomentum / math.Sqrt(float64(safeActiveDays))
	}
	momentum := followerMomentum + engagementMomentum
	score := (0.2*math.Max(0, trendingScore) + momentum) / audiencePenalty
	if score <= 0 {
		return 0
	}
	return math.Round(score*1000.0) / 1000.0
}

func maxInt64(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
