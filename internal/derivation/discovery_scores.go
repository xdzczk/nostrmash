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

func computeProfileTrendingScore(
	window time.Duration,
	nowUnix int64,
	recentActivityAt *int64,
	postCount int64,
	replyCount int64,
	engagementReceived int64,
	zapVolumeMSats int64,
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
	safeEngagement := maxInt64(0, engagementReceived)
	safeZapVolume := maxInt64(0, zapVolumeMSats)
	safeActiveDays := maxInt64(0, int64(activeDays))
	totalPosts := safePosts + safeReplies

	engagementSignal := 3.0 * math.Log1p(float64(safeEngagement))
	postingSignal := 1.0 * math.Log1p(float64(totalPosts))
	zapSignal := math.Log1p(float64(safeZapVolume) / 100000.0)
	consistencySignal := 0.9 * math.Log1p(float64(safeActiveDays))

	engagementPerPost := float64(safeEngagement) / (1.0 + float64(totalPosts))
	qualityBoost := 1.0 + math.Min(1.5, engagementPerPost)
	postingPressure := float64(totalPosts) / (1.0 + float64(safeEngagement) + float64(safeActiveDays))
	volumePenalty := 1.0 / (1.0 + math.Max(0.0, postingPressure-1.0))

	base := (engagementSignal + postingSignal + zapSignal + consistencySignal) * qualityBoost * volumePenalty
	decay := 1.0 / (1.0 + (float64(ageSeconds) / float64(windowSeconds)))
	score := base * decay
	if score <= 0 {
		return 0
	}
	return math.Round(score*1000.0) / 1000.0
}

func computeProfileRisingScore(
	trendingScore float64,
	followerCount int64,
	newFollowers int64,
	engagementReceived int64,
	postCount int64,
	replyCount int64,
	activeDays int,
) float64 {
	if trendingScore <= 0 {
		return 0
	}
	safeFollowerCount := maxInt64(0, followerCount)
	safeNewFollowers := maxInt64(0, newFollowers)
	safeEngagement := maxInt64(0, engagementReceived)
	safePosts := maxInt64(0, postCount)
	safeReplies := maxInt64(0, replyCount)
	safeActiveDays := maxInt64(0, int64(activeDays))
	totalPosts := safePosts + safeReplies
	audiencePenalty := 1.0 + math.Log10(1.0+float64(safeFollowerCount))
	if audiencePenalty <= 0 {
		audiencePenalty = 1.0
	}
	followerMomentum := 4.0 * math.Log1p(float64(safeNewFollowers))
	momentum := followerMomentum + 0.4*math.Log1p(float64(safeEngagement))
	qualityFactor := 1.0 + math.Min(1.0, float64(safeEngagement)/(1.0+float64(totalPosts)))
	postingPressure := float64(totalPosts) / (1.0 + float64(safeEngagement) + float64(safeActiveDays))
	volumePenalty := 1.0 / (1.0 + math.Max(0.0, postingPressure-1.0))
	momentum = momentum * qualityFactor * volumePenalty
	if safeActiveDays > 0 {
		momentum = momentum / math.Sqrt(float64(safeActiveDays))
	}
	score := (0.2*trendingScore + momentum) / audiencePenalty
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
