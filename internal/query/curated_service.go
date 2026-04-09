package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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

func (s Service) GetHashtagSummary(ctx context.Context, hashtag string) (HashtagSummary, error) {
	normalized, err := normalizeHashtagToken(hashtag)
	if err != nil {
		return HashtagSummary{}, err
	}
	if r := s.capabilities.curated.hashtagSummary; r != nil {
		return r.GetHashtagSummary(ctx, normalized)
	}
	return HashtagSummary{}, unsupportedCapabilityError("hashtag summary")
}

func (s Service) GetHashtagNotes(
	ctx context.Context,
	hashtag string,
	sort string,
	window string,
	limit int,
	offset int,
) ([]TrendingNote, error) {
	normalized, err := normalizeHashtagToken(hashtag)
	if err != nil {
		return nil, err
	}
	if r := s.capabilities.curated.hashtagNotes; r != nil {
		return r.GetHashtagNotes(ctx, normalized, sort, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("hashtag notes")
}

func (s Service) GetRelatedHashtags(ctx context.Context, hashtag string, limit int) ([]RelatedHashtag, error) {
	normalized, err := normalizeHashtagToken(hashtag)
	if err != nil {
		return nil, err
	}
	if r := s.capabilities.curated.relatedHashtags; r != nil {
		return r.GetRelatedHashtags(ctx, normalized, limit)
	}
	return nil, unsupportedCapabilityError("related hashtags")
}

func normalizeHashtagToken(value string) (string, error) {
	normalized := normalizeHashtagForLookup(value)
	if normalized == "" {
		return "", fmt.Errorf("hashtag is invalid: %w", ErrInvalidHashtag)
	}
	return normalized, nil
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

func (s Service) GetHotConversations(ctx context.Context, window time.Duration, limit int, offset int) ([]HotConversation, error) {
	if r := s.capabilities.curated.hotConversations; r != nil {
		return r.GetHotConversations(ctx, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("hot conversations")
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

func (s Service) GetRelatedProfiles(ctx context.Context, pubkey string, limit int) ([]RelatedProfile, error) {
	normalized := strings.TrimSpace(pubkey)
	if normalized == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	if r := s.capabilities.curated.relatedProfiles; r != nil {
		return r.GetRelatedProfiles(ctx, normalized, limit)
	}
	return nil, unsupportedCapabilityError("related profiles")
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
