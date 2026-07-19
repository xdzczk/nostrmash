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
		row, err := r.GetNetworkStats(ctx)
		if err != nil {
			return NetworkStats{}, err
		}
		return networkStatsFromStore(row), nil
	}
	return NetworkStats{}, unsupportedCapabilityError("network stats")
}

func (s Service) GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (PublicDiscoveryNetworkStats, error) {
	if r := s.capabilities.curated.publicNetworkStats; r != nil {
		row, err := r.GetPublicDiscoveryNetworkStats(ctx, hashtagLimit)
		if err != nil {
			return PublicDiscoveryNetworkStats{}, err
		}
		return publicDiscoveryNetworkStatsFromStore(row), nil
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
		rows, err := r.GetCuratedRecommendedReads(ctx, limit)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, curatedRecommendedReadFromStore), nil
	}
	return nil, unsupportedCapabilityError("curated recommended reads")
}

func (s Service) GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error) {
	if r := s.capabilities.curated.readsTopics; r != nil {
		rows, err := r.GetCuratedReadsTopics(ctx, limit)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, curatedReadsTopicFromStore), nil
	}
	return nil, unsupportedCapabilityError("curated reads topics")
}

func (s Service) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error) {
	if r := s.capabilities.curated.featuredAuthors; r != nil {
		rows, err := r.GetCuratedFeaturedAuthors(ctx, limit)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, curatedFeaturedAuthorFromStore), nil
	}
	return nil, unsupportedCapabilityError("curated featured authors")
}

func (s Service) GetTrendingHashtags(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingHashtag, error) {
	if r := s.capabilities.curated.trendingHashtags; r != nil {
		if s.discoveryTrustMode == trustModeOpen {
			rows, err := r.GetTrendingHashtags(ctx, window, limit, offset)
			if err != nil {
				return nil, err
			}
			return mapSlice(rows, trendingHashtagFromStore), nil
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
		row, err := r.GetHashtagSummary(ctx, normalized)
		if err != nil {
			return HashtagSummary{}, err
		}
		return hashtagSummaryFromStore(row), nil
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
		rows, err := r.GetHashtagNotes(ctx, normalized, sort, window, limit, offset)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, trendingNoteFromStore), nil
	}
	return nil, unsupportedCapabilityError("hashtag notes")
}

func (s Service) GetRelatedHashtags(ctx context.Context, hashtag string, limit int) ([]RelatedHashtag, error) {
	normalized, err := normalizeHashtagToken(hashtag)
	if err != nil {
		return nil, err
	}
	if r := s.capabilities.curated.relatedHashtags; r != nil {
		rows, err := r.GetRelatedHashtags(ctx, normalized, limit)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, relatedHashtagFromStore), nil
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
			rows, err := r.GetTrendingNotes(ctx, window, limit, offset)
			if err != nil {
				return nil, err
			}
			return mapSlice(rows, trendingNoteFromStore), nil
		}
		return s.getTrendingNotesTrustAware(ctx, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("trending notes")
}

func (s Service) GetTrendingLongForm(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingNote, error) {
	if r := s.capabilities.curated.trendingLongForm; r != nil {
		if s.discoveryTrustMode == trustModeOpen {
			rows, err := r.GetTrendingLongForm(ctx, window, limit, offset)
			if err != nil {
				return nil, err
			}
			return mapSlice(rows, trendingNoteFromStore), nil
		}
		return s.getTrendingLongFormTrustAware(ctx, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("trending long-form")
}

func (s Service) GetHotConversations(ctx context.Context, window time.Duration, limit int, offset int) ([]HotConversation, error) {
	if r := s.capabilities.curated.hotConversations; r != nil {
		rows, err := r.GetHotConversations(ctx, window, limit, offset)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, hotConversationFromStore), nil
	}
	return nil, unsupportedCapabilityError("hot conversations")
}

func (s Service) GetTrendingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error) {
	if r := s.capabilities.curated.trendingProfiles; r != nil {
		fetch := queryTrendingProfilesFetch(r.GetTrendingProfiles)
		if s.discoveryTrustMode == trustModeOpen {
			return fetch(ctx, window, limit, offset)
		}
		return s.getTrendingProfilesTrustAware(ctx, fetch, false, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("trending profiles")
}

func (s Service) GetRisingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error) {
	if r := s.capabilities.curated.risingProfiles; r != nil {
		fetch := queryTrendingProfilesFetch(r.GetRisingProfiles)
		if s.discoveryTrustMode == trustModeOpen {
			return fetch(ctx, window, limit, offset)
		}
		return s.getTrendingProfilesTrustAware(ctx, fetch, true, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("rising profiles")
}

func (s Service) GetRelatedProfiles(ctx context.Context, pubkey string, limit int) ([]RelatedProfile, error) {
	normalized := CanonicalizePubkey(pubkey)
	if normalized == "" {
		normalized = strings.TrimSpace(pubkey)
	}
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
		rows, err := r.GetRelatedProfiles(ctx, normalized, limit)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, relatedProfileFromStore), nil
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
