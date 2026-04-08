package query

import (
	"context"
	"fmt"
	"strings"
)

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
	return score, nil
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
	return rows, nil
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
	return row, nil
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
	return rows, nil
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
	return reader.IsTrustedAuthor(ctx, pubkey, policy)
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
	reader := s.capabilities.trust.qualification
	if reader == nil {
		return nil, unsupportedCapabilityError("trust qualification")
	}
	rows, err := reader.GetTrustQualifications(ctx, normalized, policy)
	if err != nil {
		return nil, err
	}
	out := make(map[string]TrustQualification, len(normalized))
	for _, pubkey := range normalized {
		if row, ok := rows[pubkey]; ok {
			out[pubkey] = row
			continue
		}
		out[pubkey] = TrustQualification{Pubkey: pubkey}
	}
	return out, nil
}
