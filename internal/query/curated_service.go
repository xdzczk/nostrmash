package query

import (
	"context"
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/store"
)

func (s Service) GetNetworkStats(ctx context.Context) (store.NetworkStats, error) {
	type statsReader interface {
		GetNetworkStats(ctx context.Context) (store.NetworkStats, error)
	}
	if r, ok := s.reader.(statsReader); ok {
		return r.GetNetworkStats(ctx)
	}
	return store.NetworkStats{}, nil
}

func (s Service) GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error) {
	type curatedReader interface {
		GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetCuratedValues(ctx, tableName, valueColumn, limit)
	}
	return []string{}, nil
}

func (s Service) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error) {
	type curatedReader interface {
		GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetCuratedRecommendedReads(ctx, limit)
	}
	return []store.CuratedRecommendedRead{}, nil
}

func (s Service) GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error) {
	type curatedReader interface {
		GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetCuratedReadsTopics(ctx, limit)
	}
	return []store.CuratedReadsTopic{}, nil
}

func (s Service) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error) {
	type curatedReader interface {
		GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetCuratedFeaturedAuthors(ctx, limit)
	}
	return []store.CuratedFeaturedAuthor{}, nil
}

func (s Service) GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error) {
	type curatedReader interface {
		GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetCreatorPaidTiers(ctx, pubkey)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error) {
	type curatedReader interface {
		GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetPubkeyByLNAddress(ctx, lnAddress)
	}
	return "", store.ErrNotFound
}
