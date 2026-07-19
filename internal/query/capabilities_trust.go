package query

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type trustStateCapability interface {
	GetTrustState(ctx context.Context, pubkey string) (TrustState, error)
	GetTrustStates(ctx context.Context, pubkeys []string) (map[string]TrustState, error)
}

type trustScoreCapability interface {
	GetTrustScore(ctx context.Context, pubkey string) (TrustScore, error)
}

type topTrustedPubkeysCapability interface {
	ListTopTrustedPubkeys(ctx context.Context, limit int) ([]TrustScore, error)
}

type trustRunCapability interface {
	GetTrustRun(ctx context.Context, runID int64) (TrustRun, error)
}

type trustRunsCapability interface {
	ListTrustRuns(ctx context.Context, limit int) ([]TrustRun, error)
}

type trustQualificationCapability interface {
	GetTrustQualifications(ctx context.Context, pubkeys []string, policy TrustQualificationPolicy) (map[string]TrustQualification, error)
	IsTrustedAuthor(ctx context.Context, pubkey string, policy TrustQualificationPolicy) (bool, error)
}

func adaptTrustCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(trustStateCapability); ok {
		caps.trust.state = r
	}
	if r, ok := reader.(trustScoreCapability); ok {
		caps.trust.score = r
	}
	if r, ok := reader.(topTrustedPubkeysCapability); ok {
		caps.trust.topPubkeys = r
	}
	if r, ok := reader.(trustRunCapability); ok {
		caps.trust.run = r
	}
	if r, ok := reader.(trustRunsCapability); ok {
		caps.trust.runs = r
	}
	if r, ok := reader.(trustQualificationCapability); ok {
		caps.trust.qualification = r
	}
	if legacy, ok := reader.(legacyTrustCapability); ok {
		adapted := legacyTrustAdapter{legacy: legacy}
		if caps.trust.state == nil {
			caps.trust.state = adapted
		}
		if caps.trust.score == nil {
			caps.trust.score = adapted
		}
		if caps.trust.topPubkeys == nil {
			caps.trust.topPubkeys = adapted
		}
		if caps.trust.run == nil {
			caps.trust.run = adapted
		}
		if caps.trust.runs == nil {
			caps.trust.runs = adapted
		}
	}
	if legacy, ok := reader.(legacyTrustQualificationCapability); ok {
		adapted := legacyTrustQualificationAdapter{legacy: legacy}
		if caps.trust.qualification == nil {
			caps.trust.qualification = adapted
		}
	}
}

type legacyTrustCapability interface {
	GetTrustState(ctx context.Context, pubkey string) (readmodel.TrustState, error)
	GetTrustStates(ctx context.Context, pubkeys []string) (map[string]readmodel.TrustState, error)
	GetTrustScore(ctx context.Context, pubkey string) (readmodel.TrustGlobalScore, error)
	ListTopTrustedPubkeys(ctx context.Context, limit int) ([]readmodel.TrustGlobalScore, error)
	GetTrustRun(ctx context.Context, runID int64) (readmodel.TrustRun, error)
	ListTrustRuns(ctx context.Context, limit int) ([]readmodel.TrustRun, error)
}

type legacyTrustQualificationCapability interface {
	GetTrustQualifications(ctx context.Context, pubkeys []string, policy readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error)
	IsTrustedAuthor(ctx context.Context, pubkey string, policy readmodel.TrustQualificationPolicy) (bool, error)
}

type legacyTrustAdapter struct {
	legacy legacyTrustCapability
}

type legacyTrustQualificationAdapter struct {
	legacy legacyTrustQualificationCapability
}

func (a legacyTrustAdapter) GetTrustScore(ctx context.Context, pubkey string) (TrustScore, error) {
	row, err := a.legacy.GetTrustScore(ctx, pubkey)
	if err != nil {
		return TrustScore{}, err
	}
	return trustScoreFromStore(row), nil
}

func (a legacyTrustAdapter) GetTrustState(ctx context.Context, pubkey string) (TrustState, error) {
	row, err := a.legacy.GetTrustState(ctx, pubkey)
	if err != nil {
		return TrustState{}, err
	}
	return trustStateFromStore(row), nil
}

func (a legacyTrustAdapter) GetTrustStates(ctx context.Context, pubkeys []string) (map[string]TrustState, error) {
	rows, err := a.legacy.GetTrustStates(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]TrustState, len(rows))
	for pubkey, row := range rows {
		out[pubkey] = trustStateFromStore(row)
	}
	return out, nil
}

func (a legacyTrustAdapter) ListTopTrustedPubkeys(ctx context.Context, limit int) ([]TrustScore, error) {
	rows, err := a.legacy.ListTopTrustedPubkeys(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]TrustScore, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustScoreFromStore(row))
	}
	return out, nil
}

func (a legacyTrustAdapter) GetTrustRun(ctx context.Context, runID int64) (TrustRun, error) {
	row, err := a.legacy.GetTrustRun(ctx, runID)
	if err != nil {
		return TrustRun{}, err
	}
	return trustRunFromStore(row), nil
}

func (a legacyTrustAdapter) ListTrustRuns(ctx context.Context, limit int) ([]TrustRun, error) {
	rows, err := a.legacy.ListTrustRuns(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]TrustRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustRunFromStore(row))
	}
	return out, nil
}

func (a legacyTrustQualificationAdapter) GetTrustQualifications(
	ctx context.Context,
	pubkeys []string,
	policy TrustQualificationPolicy,
) (map[string]TrustQualification, error) {
	rows, err := a.legacy.GetTrustQualifications(ctx, pubkeys, readmodel.TrustQualificationPolicy{
		MaxHops:      policy.MaxHops,
		MinimumScore: policy.MinimumScore,
	})
	if err != nil {
		return nil, err
	}
	out := make(map[string]TrustQualification, len(rows))
	for pubkey, row := range rows {
		out[pubkey] = trustQualificationFromStore(row)
	}
	return out, nil
}

func (a legacyTrustQualificationAdapter) IsTrustedAuthor(
	ctx context.Context,
	pubkey string,
	policy TrustQualificationPolicy,
) (bool, error) {
	return a.legacy.IsTrustedAuthor(ctx, pubkey, readmodel.TrustQualificationPolicy{
		MaxHops:      policy.MaxHops,
		MinimumScore: policy.MinimumScore,
	})
}
