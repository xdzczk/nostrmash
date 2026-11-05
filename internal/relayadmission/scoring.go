package relayadmission

import (
	"encoding/json"
	"math"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

// maxPopularityScore is only a safety ceiling against pathological ref counts.
// Normal multi-thousand relays stay well below it under popularityScore().
const maxPopularityScore = 150

// largeRelayUserThreshold is the distinct user-list ref count at which a relay
// is treated as "large" for admission scoring.
const largeRelayUserThreshold = 1000

// ScoreComponents holds the transparent breakdown of a relay's computed score.
type ScoreComponents struct {
	PopularityScore   float64 `json:"popularity_score"`
	ProbeHealthScore  float64 `json:"probe_health_score"`
	LatencyScore      float64 `json:"latency_score"`
	YieldScore        float64 `json:"yield_score"`
	DuplicatePenalty  float64 `json:"duplicate_penalty"`
	FailurePenalty    float64 `json:"failure_penalty"`
	FirstSeenShare    float64 `json:"first_seen_share"`
	ExclusiveShare    float64 `json:"exclusive_share"`
	PropagationScore  float64 `json:"propagation_score"`
	BlockedRefPenalty float64 `json:"blocked_ref_penalty"`
	TotalScore        float64 `json:"total_score"`
}

// ComputeScore calculates a transparent, additive score for a relay record.
func ComputeScore(rec relayregistry.RelayRecord) ScoreComponents {
	var sc ScoreComponents

	sc.PopularityScore = popularityScore(rec.DistinctUserRefCount)

	// Probe health: successful probes contribute positively.
	if rec.LastProbeStatus != nil && *rec.LastProbeStatus == relayregistry.ProbeStatusOK {
		sc.ProbeHealthScore = 20
	} else if rec.LastConnectOK != nil && *rec.LastConnectOK {
		sc.ProbeHealthScore = 5
	}

	// Latency: lower is better, capped contribution.
	if rec.AvgConnectLatency != nil {
		latMs := *rec.AvgConnectLatency
		if latMs < 500 {
			sc.LatencyScore = 10
		} else if latMs < 2000 {
			sc.LatencyScore = 5
		}
	}

	// Yield: probe sample usefulness.
	sc.YieldScore = clamp(rec.YieldScore*10, 0, 10)

	// Duplicate penalty: high duplicate ratio reduces score.
	if rec.DuplicateRatio > 0.5 {
		sc.DuplicatePenalty = -clamp((rec.DuplicateRatio-0.5)*20, 0, 10)
	}

	// Failure penalty: persistent failures heavily reduce score.
	if rec.ProbeFailRate > 0.3 {
		sc.FailurePenalty = -clamp(rec.ProbeFailRate*40, 0, 30)
	}

	sc.FirstSeenShare = rec.FirstSeenShare
	sc.ExclusiveShare = rec.ExclusiveShare
	sc.PropagationScore = clamp(rec.FirstSeenShare*8+rec.ExclusiveShare*4, 0, 12)
	if rec.BlockedRefCount > 0 {
		sc.BlockedRefPenalty = -math.Min(float64(rec.BlockedRefCount), 8)
	}

	sc.TotalScore = sc.PopularityScore + sc.ProbeHealthScore + sc.LatencyScore +
		sc.YieldScore + sc.DuplicatePenalty + sc.FailurePenalty + sc.PropagationScore + sc.BlockedRefPenalty

	if sc.TotalScore < 0 {
		sc.TotalScore = 0
	}
	return sc
}

// popularityScore maps distinct kind-10002 user refs onto an admission weight.
// Log growth keeps multi-thousand relays ahead of sub-1000 niches without the
// old linear cap that treated 40 users the same as 40_000. A boost at
// largeRelayUserThreshold makes "thousands of users" clearly beat smaller relays.
func popularityScore(distinctUsers int) float64 {
	if distinctUsers <= 0 {
		return 0
	}
	score := 22 * math.Log10(1+float64(distinctUsers))
	if distinctUsers >= largeRelayUserThreshold {
		score += 20
	}
	return clamp(score, 0, maxPopularityScore)
}

// MarshalComponents serializes score components for storage.
func MarshalComponents(sc ScoreComponents) json.RawMessage {
	data, _ := json.Marshal(sc)
	return data
}

// ApplyBlockedRefs copies kind-10006 mention counts onto rec.
func ApplyBlockedRefs(rec relayregistry.RelayRecord, counts map[string]int) relayregistry.RelayRecord {
	if counts == nil {
		return rec
	}
	if count, ok := counts[rec.URLKey]; ok {
		rec.BlockedRefCount = count
		return rec
	}
	if count, ok := counts[rec.NormalizedURL]; ok {
		rec.BlockedRefCount = count
	}
	return rec
}

// ApplyPropagationShare copies the latest daily propagation mix onto rec
// so ComputeScore can include it. Lookup is by normalized URL, then URL key.
func ApplyPropagationShare(rec relayregistry.RelayRecord, shares map[string]relayregistry.PropagationShare) relayregistry.RelayRecord {
	if len(shares) == 0 {
		return rec
	}
	share, ok := shares[rec.NormalizedURL]
	if !ok {
		share, ok = shares[rec.URLKey]
	}
	if !ok {
		return rec
	}
	rec.FirstSeenShare = share.FirstSeen
	rec.ExclusiveShare = share.Exclusive
	return rec
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
