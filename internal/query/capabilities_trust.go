package query

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/store"
)

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

func adaptTrustCapabilities(reader any, caps *serviceCapabilities) {
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
	if legacy, ok := reader.(legacyTrustCapability); ok {
		adapted := legacyTrustAdapter{legacy: legacy}
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
}

type legacyTrustCapability interface {
	GetTrustScore(ctx context.Context, pubkey string) (store.TrustGlobalScore, error)
	ListTopTrustedPubkeys(ctx context.Context, limit int) ([]store.TrustGlobalScore, error)
	GetTrustRun(ctx context.Context, runID int64) (store.TrustRun, error)
	ListTrustRuns(ctx context.Context, limit int) ([]store.TrustRun, error)
}

type legacyTrustAdapter struct {
	legacy legacyTrustCapability
}

func (a legacyTrustAdapter) GetTrustScore(ctx context.Context, pubkey string) (TrustScore, error) {
	row, err := a.legacy.GetTrustScore(ctx, pubkey)
	if err != nil {
		return TrustScore{}, err
	}
	return trustScoreFromStore(row), nil
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
