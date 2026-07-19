package query

import (
	"context"
	"fmt"
	"strings"
)

func (s Service) GetTrustState(ctx context.Context, pubkey string) (TrustState, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return TrustState{}, fmt.Errorf("pubkey is required")
	}
	reader := s.capabilities.trust.state
	if reader == nil {
		return TrustState{}, unsupportedCapabilityError("trust state")
	}
	row, err := reader.GetTrustState(ctx, pubkey)
	if err != nil {
		return TrustState{}, err
	}
	return trustStateFromStore(row), nil
}

func (s Service) GetTrustStates(ctx context.Context, pubkeys []string) (map[string]TrustState, error) {
	normalized := normalizeUniqueStrings(pubkeys)
	if len(normalized) == 0 {
		return map[string]TrustState{}, nil
	}
	reader := s.capabilities.trust.state
	if reader == nil {
		return nil, unsupportedCapabilityError("trust state")
	}
	rows, err := reader.GetTrustStates(ctx, normalized)
	if err != nil {
		return nil, err
	}
	out := make(map[string]TrustState, len(normalized))
	for _, pubkey := range normalized {
		row, ok := rows[pubkey]
		if !ok {
			out[pubkey] = TrustState{
				Pubkey:    pubkey,
				Tier:      "unknown",
				HopBucket: "unknown",
			}
			continue
		}
		out[pubkey] = trustStateFromStore(row)
	}
	return out, nil
}

func (s Service) GetTrustScore(ctx context.Context, pubkey string) (TrustScore, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return TrustScore{}, fmt.Errorf("pubkey is required")
	}
	reader := s.capabilities.trust.score
	if reader == nil {
		return TrustScore{}, unsupportedCapabilityError("trust score")
	}
	score, err := reader.GetTrustScore(ctx, pubkey)
	if err != nil {
		return TrustScore{}, err
	}
	return trustScoreFromStore(score), nil
}

func (s Service) ListTopTrustedPubkeys(ctx context.Context, limit int) ([]TrustScore, error) {
	if limit <= 0 {
		limit = 50
	}
	reader := s.capabilities.trust.topPubkeys
	if reader == nil {
		return nil, unsupportedCapabilityError("top trusted pubkeys")
	}
	rows, err := reader.ListTopTrustedPubkeys(ctx, limit)
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, trustScoreFromStore), nil
}

func (s Service) GetTrustRun(ctx context.Context, runID int64) (TrustRun, error) {
	if runID <= 0 {
		return TrustRun{}, fmt.Errorf("run id must be > 0")
	}
	reader := s.capabilities.trust.run
	if reader == nil {
		return TrustRun{}, unsupportedCapabilityError("trust run")
	}
	row, err := reader.GetTrustRun(ctx, runID)
	if err != nil {
		return TrustRun{}, err
	}
	return trustRunFromStore(row), nil
}

func (s Service) ListTrustRuns(ctx context.Context, limit int) ([]TrustRun, error) {
	if limit <= 0 {
		limit = 50
	}
	reader := s.capabilities.trust.runs
	if reader == nil {
		return nil, unsupportedCapabilityError("trust runs")
	}
	rows, err := reader.ListTrustRuns(ctx, limit)
	if err != nil {
		return nil, err
	}
	return mapSlice(rows, trustRunFromStore), nil
}

func (s Service) IsTrustedAuthor(ctx context.Context, pubkey string, policy TrustQualificationPolicy) (bool, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return false, fmt.Errorf("pubkey is required")
	}
	reader := s.capabilities.trust.qualification
	if reader == nil {
		return false, unsupportedCapabilityError("trust qualification")
	}
	return reader.IsTrustedAuthor(ctx, pubkey, trustQualificationPolicyToStore(policy))
}

func (s Service) GetTrustQualification(
	ctx context.Context,
	pubkeys []string,
	policy TrustQualificationPolicy,
) (map[string]TrustQualification, error) {
	normalized := normalizeUniqueStrings(pubkeys)
	if len(normalized) == 0 {
		return map[string]TrustQualification{}, nil
	}
	if reader := s.capabilities.trust.state; reader != nil {
		states, err := reader.GetTrustStates(ctx, normalized)
		if err == nil {
			out := make(map[string]TrustQualification, len(normalized))
			for _, pubkey := range normalized {
				state, ok := states[pubkey]
				if !ok {
					out[pubkey] = trustQualificationFromState(TrustState{
						Pubkey:    pubkey,
						Tier:      "unknown",
						HopBucket: "unknown",
					}, policy)
					continue
				}
				out[pubkey] = trustQualificationFromState(trustStateFromStore(state), policy)
			}
			return out, nil
		}
	}
	reader := s.capabilities.trust.qualification
	if reader == nil {
		return nil, unsupportedCapabilityError("trust qualification")
	}
	rows, err := reader.GetTrustQualifications(ctx, normalized, trustQualificationPolicyToStore(policy))
	if err != nil {
		return nil, err
	}
	out := make(map[string]TrustQualification, len(normalized))
	for _, pubkey := range normalized {
		if row, ok := rows[pubkey]; ok {
			out[pubkey] = trustQualificationFromStore(row)
			continue
		}
		out[pubkey] = TrustQualification{Pubkey: pubkey}
	}
	return out, nil
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
	if !state.Qualified {
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
