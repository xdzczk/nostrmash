package store

import (
	"strings"
	"time"
)

type TrustQualificationPolicy struct {
	MaxHops      int
	MinimumScore float64
}

type TrustQualification struct {
	Pubkey       string
	Trusted      bool
	IsSeed       bool
	DistanceHops *int
	Score        *float64
	Rank         *int64
	SourceRunID  *int64
}

type TrustState struct {
	Pubkey       string
	Score        *float64
	Qualified    bool
	Tier         string
	HopDistance  *int
	HopBucket    string
	Rank         *int64
	ComputedAt   *time.Time
	GenerationID *int64
	IsSeed       bool
}

func normalizePubkeys(pubkeys []string) []string {
	out := make([]string, 0, len(pubkeys))
	seen := make(map[string]struct{}, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		out = append(out, pubkey)
	}
	return out
}

func trustQualificationFromState(state TrustState, policy TrustQualificationPolicy) TrustQualification {
	policy = normalizeTrustPolicy(policy)
	return TrustQualification{
		Pubkey:       state.Pubkey,
		Trusted:      trustStateTrusted(state, policy),
		IsSeed:       state.IsSeed,
		DistanceHops: state.HopDistance,
		Score:        state.Score,
		Rank:         state.Rank,
		SourceRunID:  state.GenerationID,
	}
}

func normalizeTrustPolicy(policy TrustQualificationPolicy) TrustQualificationPolicy {
	if policy.MaxHops < 0 {
		policy.MaxHops = 0
	}
	if policy.MinimumScore < 0 {
		policy.MinimumScore = 0
	}
	return policy
}

func trustStateTrusted(state TrustState, policy TrustQualificationPolicy) bool {
	if state.IsSeed {
		return true
	}
	if !trustStateQualified(state) {
		return false
	}
	if state.HopDistance != nil && policy.MaxHops > 0 && *state.HopDistance > policy.MaxHops {
		return false
	}
	if policy.MinimumScore > 0 {
		if state.Score == nil || *state.Score < policy.MinimumScore {
			return false
		}
	}
	return true
}

func trustStateQualified(state TrustState) bool {
	return state.IsSeed || state.HopDistance != nil || state.Score != nil
}

func trustStateKnown(state TrustState) bool {
	return trustStateQualified(state) || state.GenerationID != nil || state.ComputedAt != nil || state.Rank != nil
}

func trustHopBucket(hops *int) string {
	if hops == nil {
		return "unknown"
	}
	switch {
	case *hops <= 0:
		return "0"
	case *hops == 1:
		return "1"
	case *hops == 2:
		return "2"
	case *hops == 3:
		return "3"
	default:
		return "4_plus"
	}
}

func trustTierFromState(state TrustState) string {
	if state.IsSeed {
		return "seed"
	}
	if state.HopDistance == nil {
		return "unknown"
	}
	switch {
	case *state.HopDistance <= 1:
		return "core"
	case *state.HopDistance <= 3:
		return "near"
	default:
		return "outer"
	}
}
