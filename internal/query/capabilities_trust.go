package query

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// Trust capability interfaces are readmodel-shaped; the Service maps to query
// DTOs at the response edge via mappers_trust.go.

type trustStateCapability interface {
	GetTrustState(ctx context.Context, pubkey string) (readmodel.TrustState, error)
	GetTrustStates(ctx context.Context, pubkeys []string) (map[string]readmodel.TrustState, error)
}

type trustScoreCapability interface {
	GetTrustScore(ctx context.Context, pubkey string) (readmodel.TrustGlobalScore, error)
}

type topTrustedPubkeysCapability interface {
	ListTopTrustedPubkeys(ctx context.Context, limit int) ([]readmodel.TrustGlobalScore, error)
}

type trustRunCapability interface {
	GetTrustRun(ctx context.Context, runID int64) (readmodel.TrustRun, error)
}

type trustRunsCapability interface {
	ListTrustRuns(ctx context.Context, limit int) ([]readmodel.TrustRun, error)
}

type trustQualificationCapability interface {
	GetTrustQualifications(ctx context.Context, pubkeys []string, policy readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error)
	IsTrustedAuthor(ctx context.Context, pubkey string, policy readmodel.TrustQualificationPolicy) (bool, error)
}

type rankedPubkeyCountCapability interface {
	CountRankedPubkeys(ctx context.Context) (int64, error)
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
	if r, ok := reader.(rankedPubkeyCountCapability); ok {
		caps.trust.rankedCount = r
	}
}
