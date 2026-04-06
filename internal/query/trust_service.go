package query

import (
	"context"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/store"
)

type trustReader interface {
	GetTrustScore(ctx context.Context, pubkey string) (store.TrustGlobalScore, error)
	ListTopTrustedPubkeys(ctx context.Context, limit int) ([]store.TrustGlobalScore, error)
	GetTrustRun(ctx context.Context, runID int64) (store.TrustRun, error)
	ListTrustRuns(ctx context.Context, limit int) ([]store.TrustRun, error)
}

func (s Service) GetTrustScore(ctx context.Context, pubkey string) (store.TrustGlobalScore, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return store.TrustGlobalScore{}, fmt.Errorf("pubkey is required")
	}
	reader, ok := s.reader.(trustReader)
	if !ok {
		return store.TrustGlobalScore{}, fmt.Errorf("trust reads are not configured")
	}
	return reader.GetTrustScore(ctx, pubkey)
}

func (s Service) ListTopTrustedPubkeys(ctx context.Context, limit int) ([]store.TrustGlobalScore, error) {
	if limit <= 0 {
		limit = 50
	}
	reader, ok := s.reader.(trustReader)
	if !ok {
		return nil, fmt.Errorf("trust reads are not configured")
	}
	return reader.ListTopTrustedPubkeys(ctx, limit)
}

func (s Service) GetTrustRun(ctx context.Context, runID int64) (store.TrustRun, error) {
	if runID <= 0 {
		return store.TrustRun{}, fmt.Errorf("run id must be > 0")
	}
	reader, ok := s.reader.(trustReader)
	if !ok {
		return store.TrustRun{}, fmt.Errorf("trust reads are not configured")
	}
	return reader.GetTrustRun(ctx, runID)
}

func (s Service) ListTrustRuns(ctx context.Context, limit int) ([]store.TrustRun, error) {
	if limit <= 0 {
		limit = 50
	}
	reader, ok := s.reader.(trustReader)
	if !ok {
		return nil, fmt.Errorf("trust reads are not configured")
	}
	return reader.ListTrustRuns(ctx, limit)
}
