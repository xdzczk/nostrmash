package query

import (
	"context"
	"encoding/json"
	"time"
)

func (s Service) GetNetworkStats(ctx context.Context) (NetworkStats, error) {
	if r := s.capabilities.curated.networkStats; r != nil {
		return r.GetNetworkStats(ctx)
	}
	return NetworkStats{}, unsupportedCapabilityError("network stats")
}

func (s Service) GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (PublicDiscoveryNetworkStats, error) {
	if r := s.capabilities.curated.publicNetworkStats; r != nil {
		return r.GetPublicDiscoveryNetworkStats(ctx, hashtagLimit)
	}
	return PublicDiscoveryNetworkStats{}, unsupportedCapabilityError("public discovery network stats")
}

func (s Service) GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error) {
	if r := s.capabilities.curated.values; r != nil {
		return r.GetCuratedValues(ctx, tableName, valueColumn, limit)
	}
	return nil, unsupportedCapabilityError("curated values")
}

func (s Service) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error) {
	if r := s.capabilities.curated.recommendedReads; r != nil {
		return r.GetCuratedRecommendedReads(ctx, limit)
	}
	return nil, unsupportedCapabilityError("curated recommended reads")
}

func (s Service) GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error) {
	if r := s.capabilities.curated.readsTopics; r != nil {
		return r.GetCuratedReadsTopics(ctx, limit)
	}
	return nil, unsupportedCapabilityError("curated reads topics")
}

func (s Service) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error) {
	if r := s.capabilities.curated.featuredAuthors; r != nil {
		return r.GetCuratedFeaturedAuthors(ctx, limit)
	}
	return nil, unsupportedCapabilityError("curated featured authors")
}

func (s Service) GetTrendingHashtags(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingHashtag, error) {
	if r := s.capabilities.curated.trendingHashtags; r != nil {
		if s.discoveryTrustMode == trustModeOpen {
			return r.GetTrendingHashtags(ctx, window, limit, offset)
		}
		return s.getTrendingHashtagsTrustAware(ctx, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("trending hashtags")
}

func (s Service) GetTrendingNotes(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingNote, error) {
	if r := s.capabilities.curated.trendingNotes; r != nil {
		if s.discoveryTrustMode == trustModeOpen {
			return r.GetTrendingNotes(ctx, window, limit, offset)
		}
		return s.getTrendingNotesTrustAware(ctx, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("trending notes")
}

func (s Service) GetTrendingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error) {
	if r := s.capabilities.curated.trendingProfiles; r != nil {
		if s.discoveryTrustMode == trustModeOpen {
			return r.GetTrendingProfiles(ctx, window, limit, offset)
		}
		return s.getTrendingProfilesTrustAware(ctx, r.GetTrendingProfiles, false, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("trending profiles")
}

func (s Service) GetRisingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error) {
	if r := s.capabilities.curated.risingProfiles; r != nil {
		if s.discoveryTrustMode == trustModeOpen {
			return r.GetRisingProfiles(ctx, window, limit, offset)
		}
		return s.getTrendingProfilesTrustAware(ctx, r.GetRisingProfiles, true, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("rising profiles")
}

func (s Service) GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error) {
	if r := s.capabilities.curated.creatorPaidTiers; r != nil {
		return r.GetCreatorPaidTiers(ctx, pubkey)
	}
	return nil, unsupportedCapabilityError("creator paid tiers")
}

func (s Service) GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error) {
	if r := s.capabilities.curated.pubkeyByLNAddress; r != nil {
		return r.GetPubkeyByLNAddress(ctx, lnAddress)
	}
	return "", unsupportedCapabilityError("pubkey lookup by lightning address")
}
