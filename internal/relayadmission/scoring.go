package relayadmission

import (
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

// ScoreComponents holds the transparent breakdown of a relay's computed score.
type ScoreComponents struct {
	PopularityScore  float64 `json:"popularity_score"`
	ProbeHealthScore float64 `json:"probe_health_score"`
	LatencyScore     float64 `json:"latency_score"`
	YieldScore       float64 `json:"yield_score"`
	DuplicatePenalty float64 `json:"duplicate_penalty"`
	FailurePenalty   float64 `json:"failure_penalty"`
	TotalScore       float64 `json:"total_score"`
}

// ComputeScore calculates a transparent, additive score for a relay record.
func ComputeScore(rec relayregistry.RelayRecord) ScoreComponents {
	var sc ScoreComponents

	// Popularity: log-scaled distinct user references (diminishing returns).
	if rec.DistinctUserRefCount > 0 {
		sc.PopularityScore = clamp(float64(rec.DistinctUserRefCount)*2.0, 0, 40)
	}

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

	sc.TotalScore = sc.PopularityScore + sc.ProbeHealthScore + sc.LatencyScore +
		sc.YieldScore + sc.DuplicatePenalty + sc.FailurePenalty

	if sc.TotalScore < 0 {
		sc.TotalScore = 0
	}
	return sc
}

// MarshalComponents serializes score components for storage.
func MarshalComponents(sc ScoreComponents) json.RawMessage {
	data, _ := json.Marshal(sc)
	return data
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
