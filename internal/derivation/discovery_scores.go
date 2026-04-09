package derivation

import (
	"math"
	"time"
)

func computeTrendingScore(
	window time.Duration,
	nowUnix int64,
	noteCreatedAt int64,
	replyCount int64,
	repostCount int64,
	reactionCount int64,
	zapCount int64,
	zapMSats int64,
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
	base := float64(replyCount)*3.0 +
		float64(repostCount)*2.0 +
		float64(reactionCount)*1.0 +
		float64(zapCount)*2.0 +
		(float64(zapMSats) / 100000.0)
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
	base := float64(postCount)*2.0 +
		float64(replyCount)*1.5 +
		float64(engagementReceived)*2.5 +
		(float64(zapVolumeMSats) / 100000.0) +
		float64(activeDays)*0.75
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
	engagementReceived int64,
	postCount int64,
	replyCount int64,
	activeDays int,
) float64 {
	if trendingScore <= 0 {
		return 0
	}
	safeFollowerCount := maxInt64(0, followerCount)
	audiencePenalty := 1.0 + math.Log10(1.0+float64(safeFollowerCount))
	if audiencePenalty <= 0 {
		audiencePenalty = 1.0
	}
	momentum := float64(engagementReceived) + float64(postCount) + float64(replyCount)
	if activeDays > 0 {
		momentum = momentum / float64(activeDays)
	}
	score := (trendingScore + momentum) / audiencePenalty
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
