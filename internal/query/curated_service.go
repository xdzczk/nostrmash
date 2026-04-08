package query

import (
	"context"
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/store"
)

func (s Service) GetNetworkStats(ctx context.Context) (NetworkStats, error) {
	type queryStatsReader interface {
		GetNetworkStats(ctx context.Context) (NetworkStats, error)
	}
	type legacyStatsReader interface {
		GetNetworkStats(ctx context.Context) (store.NetworkStats, error)
	}
	if r, ok := s.rawReader.(queryStatsReader); ok {
		return r.GetNetworkStats(ctx)
	}
	if r, ok := s.rawReader.(legacyStatsReader); ok {
		row, err := r.GetNetworkStats(ctx)
		if err != nil {
			return NetworkStats{}, err
		}
		return networkStatsFromStore(row), nil
	}
	return NetworkStats{}, nil
}

func (s Service) GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error) {
	type curatedReader interface {
		GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error)
	}
	if r, ok := s.rawReader.(curatedReader); ok {
		return r.GetCuratedValues(ctx, tableName, valueColumn, limit)
	}
	return []string{}, nil
}

func (s Service) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error) {
	type queryCuratedReader interface {
		GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error)
	}
	type legacyCuratedReader interface {
		GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error)
	}
	if r, ok := s.rawReader.(queryCuratedReader); ok {
		return r.GetCuratedRecommendedReads(ctx, limit)
	}
	if r, ok := s.rawReader.(legacyCuratedReader); ok {
		rows, err := r.GetCuratedRecommendedReads(ctx, limit)
		if err != nil {
			return nil, err
		}
		out := make([]CuratedRecommendedRead, 0, len(rows))
		for _, row := range rows {
			out = append(out, curatedRecommendedReadFromStore(row))
		}
		return out, nil
	}
	return []CuratedRecommendedRead{}, nil
}

func (s Service) GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error) {
	type queryCuratedReader interface {
		GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error)
	}
	type legacyCuratedReader interface {
		GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error)
	}
	if r, ok := s.rawReader.(queryCuratedReader); ok {
		return r.GetCuratedReadsTopics(ctx, limit)
	}
	if r, ok := s.rawReader.(legacyCuratedReader); ok {
		rows, err := r.GetCuratedReadsTopics(ctx, limit)
		if err != nil {
			return nil, err
		}
		out := make([]CuratedReadsTopic, 0, len(rows))
		for _, row := range rows {
			out = append(out, curatedReadsTopicFromStore(row))
		}
		return out, nil
	}
	return []CuratedReadsTopic{}, nil
}

func (s Service) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error) {
	type queryCuratedReader interface {
		GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error)
	}
	type legacyCuratedReader interface {
		GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error)
	}
	if r, ok := s.rawReader.(queryCuratedReader); ok {
		return r.GetCuratedFeaturedAuthors(ctx, limit)
	}
	if r, ok := s.rawReader.(legacyCuratedReader); ok {
		rows, err := r.GetCuratedFeaturedAuthors(ctx, limit)
		if err != nil {
			return nil, err
		}
		out := make([]CuratedFeaturedAuthor, 0, len(rows))
		for _, row := range rows {
			out = append(out, curatedFeaturedAuthorFromStore(row))
		}
		return out, nil
	}
	return []CuratedFeaturedAuthor{}, nil
}

func (s Service) GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error) {
	type curatedReader interface {
		GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error)
	}
	if r, ok := s.rawReader.(curatedReader); ok {
		return r.GetCreatorPaidTiers(ctx, pubkey)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error) {
	type curatedReader interface {
		GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error)
	}
	if r, ok := s.rawReader.(curatedReader); ok {
		return r.GetPubkeyByLNAddress(ctx, lnAddress)
	}
	return "", store.ErrNotFound
}
