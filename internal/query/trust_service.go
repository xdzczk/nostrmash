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

func (s Service) GetTrustScore(ctx context.Context, pubkey string) (TrustScore, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return TrustScore{}, fmt.Errorf("pubkey is required")
	}
	reader, ok := s.rawReader.(trustReader)
	if !ok {
		return TrustScore{}, fmt.Errorf("trust reads are not configured")
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
	reader, ok := s.rawReader.(trustReader)
	if !ok {
		return nil, fmt.Errorf("trust reads are not configured")
	}
	rows, err := reader.ListTopTrustedPubkeys(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]TrustScore, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustScoreFromStore(row))
	}
	return out, nil
}

func (s Service) GetTrustRun(ctx context.Context, runID int64) (TrustRun, error) {
	if runID <= 0 {
		return TrustRun{}, fmt.Errorf("run id must be > 0")
	}
	reader, ok := s.rawReader.(trustReader)
	if !ok {
		return TrustRun{}, fmt.Errorf("trust reads are not configured")
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
	reader, ok := s.rawReader.(trustReader)
	if !ok {
		return nil, fmt.Errorf("trust reads are not configured")
	}
	rows, err := reader.ListTrustRuns(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]TrustRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustRunFromStore(row))
	}
	return out, nil
}
