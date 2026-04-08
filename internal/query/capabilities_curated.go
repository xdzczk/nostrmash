package query

import (
	"context"
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/store"
)

type networkStatsCapability interface {
	GetNetworkStats(ctx context.Context) (NetworkStats, error)
}

type curatedValuesCapability interface {
	GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error)
}

type curatedRecommendedReadsCapability interface {
	GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error)
}

type curatedReadsTopicsCapability interface {
	GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error)
}

type curatedFeaturedAuthorsCapability interface {
	GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error)
}

type creatorPaidTiersCapability interface {
	GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error)
}

type pubkeyByLNAddressCapability interface {
	GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error)
}

func adaptCuratedCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(networkStatsCapability); ok {
		caps.curated.networkStats = r
	} else if legacy, ok := reader.(legacyNetworkStatsCapability); ok {
		caps.curated.networkStats = legacyNetworkStatsAdapter{legacy: legacy}
	}
	if r, ok := reader.(curatedValuesCapability); ok {
		caps.curated.values = r
	}
	if r, ok := reader.(curatedRecommendedReadsCapability); ok {
		caps.curated.recommendedReads = r
	} else if legacy, ok := reader.(legacyCuratedRecommendedReadsCapability); ok {
		caps.curated.recommendedReads = legacyCuratedRecommendedReadsAdapter{legacy: legacy}
	}
	if r, ok := reader.(curatedReadsTopicsCapability); ok {
		caps.curated.readsTopics = r
	} else if legacy, ok := reader.(legacyCuratedReadsTopicsCapability); ok {
		caps.curated.readsTopics = legacyCuratedReadsTopicsAdapter{legacy: legacy}
	}
	if r, ok := reader.(curatedFeaturedAuthorsCapability); ok {
		caps.curated.featuredAuthors = r
	} else if legacy, ok := reader.(legacyCuratedFeaturedAuthorsCapability); ok {
		caps.curated.featuredAuthors = legacyCuratedFeaturedAuthorsAdapter{legacy: legacy}
	}
	if r, ok := reader.(creatorPaidTiersCapability); ok {
		caps.curated.creatorPaidTiers = r
	}
	if r, ok := reader.(pubkeyByLNAddressCapability); ok {
		caps.curated.pubkeyByLNAddress = r
	}
}

type legacyNetworkStatsCapability interface {
	GetNetworkStats(ctx context.Context) (store.NetworkStats, error)
}

type legacyNetworkStatsAdapter struct {
	legacy legacyNetworkStatsCapability
}

func (a legacyNetworkStatsAdapter) GetNetworkStats(ctx context.Context) (NetworkStats, error) {
	row, err := a.legacy.GetNetworkStats(ctx)
	if err != nil {
		return NetworkStats{}, err
	}
	return networkStatsFromStore(row), nil
}

type legacyCuratedRecommendedReadsCapability interface {
	GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error)
}

type legacyCuratedRecommendedReadsAdapter struct {
	legacy legacyCuratedRecommendedReadsCapability
}

func (a legacyCuratedRecommendedReadsAdapter) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error) {
	rows, err := a.legacy.GetCuratedRecommendedReads(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CuratedRecommendedRead, 0, len(rows))
	for _, row := range rows {
		out = append(out, curatedRecommendedReadFromStore(row))
	}
	return out, nil
}

type legacyCuratedReadsTopicsCapability interface {
	GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error)
}

type legacyCuratedReadsTopicsAdapter struct {
	legacy legacyCuratedReadsTopicsCapability
}

func (a legacyCuratedReadsTopicsAdapter) GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error) {
	rows, err := a.legacy.GetCuratedReadsTopics(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CuratedReadsTopic, 0, len(rows))
	for _, row := range rows {
		out = append(out, curatedReadsTopicFromStore(row))
	}
	return out, nil
}

type legacyCuratedFeaturedAuthorsCapability interface {
	GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error)
}

type legacyCuratedFeaturedAuthorsAdapter struct {
	legacy legacyCuratedFeaturedAuthorsCapability
}

func (a legacyCuratedFeaturedAuthorsAdapter) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error) {
	rows, err := a.legacy.GetCuratedFeaturedAuthors(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CuratedFeaturedAuthor, 0, len(rows))
	for _, row := range rows {
		out = append(out, curatedFeaturedAuthorFromStore(row))
	}
	return out, nil
}
