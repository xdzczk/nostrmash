package query

import (
	"context"
	"encoding/json"
	"time"

	"github.com/xdzczk/nostrmash/internal/store"
)

type networkStatsCapability interface {
	GetNetworkStats(ctx context.Context) (NetworkStats, error)
}

type publicDiscoveryNetworkStatsCapability interface {
	GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (PublicDiscoveryNetworkStats, error)
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

type trendingHashtagsCapability interface {
	GetTrendingHashtags(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingHashtag, error)
}

type trendingNotesCapability interface {
	GetTrendingNotes(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingNote, error)
}

type trustQualifiedTrendingNotesCapability interface {
	GetTrustQualifiedTrendingNotes(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		mode string,
		policy TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]trustedNoteCandidate, bool, error)
}

type trendingProfilesCapability interface {
	GetTrendingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error)
}

type trustQualifiedTrendingProfilesCapability interface {
	GetTrustQualifiedTrendingProfiles(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		rising bool,
		mode string,
		policy TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]trustedProfileCandidate, bool, error)
}

type risingProfilesCapability interface {
	GetRisingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error)
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
	if r, ok := reader.(publicDiscoveryNetworkStatsCapability); ok {
		caps.curated.publicNetworkStats = r
	} else if legacy, ok := reader.(legacyPublicDiscoveryNetworkStatsCapability); ok {
		caps.curated.publicNetworkStats = legacyPublicDiscoveryNetworkStatsAdapter{legacy: legacy}
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
	if r, ok := reader.(trendingHashtagsCapability); ok {
		caps.curated.trendingHashtags = r
	} else if legacy, ok := reader.(legacyTrendingHashtagsCapability); ok {
		caps.curated.trendingHashtags = legacyTrendingHashtagsAdapter{legacy: legacy}
	}
	if r, ok := reader.(trendingNotesCapability); ok {
		caps.curated.trendingNotes = r
	} else if legacy, ok := reader.(legacyTrendingNotesCapability); ok {
		caps.curated.trendingNotes = legacyTrendingNotesAdapter{legacy: legacy}
	}
	if r, ok := reader.(trustQualifiedTrendingNotesCapability); ok {
		caps.curated.trustQualifiedNotes = r
	} else if legacy, ok := reader.(legacyTrustQualifiedTrendingNotesCapability); ok {
		caps.curated.trustQualifiedNotes = legacyTrustQualifiedTrendingNotesAdapter{legacy: legacy}
	}
	if r, ok := reader.(trendingProfilesCapability); ok {
		caps.curated.trendingProfiles = r
	} else if legacy, ok := reader.(legacyTrendingProfilesCapability); ok {
		caps.curated.trendingProfiles = legacyTrendingProfilesAdapter{legacy: legacy}
	}
	if r, ok := reader.(trustQualifiedTrendingProfilesCapability); ok {
		caps.curated.trustQualifiedProfiles = r
	} else if legacy, ok := reader.(legacyTrustQualifiedTrendingProfilesCapability); ok {
		caps.curated.trustQualifiedProfiles = legacyTrustQualifiedTrendingProfilesAdapter{legacy: legacy}
	}
	if r, ok := reader.(risingProfilesCapability); ok {
		caps.curated.risingProfiles = r
	} else if legacy, ok := reader.(legacyRisingProfilesCapability); ok {
		caps.curated.risingProfiles = legacyRisingProfilesAdapter{legacy: legacy}
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

type legacyPublicDiscoveryNetworkStatsCapability interface {
	GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (store.PublicDiscoveryNetworkStats, error)
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

type legacyPublicDiscoveryNetworkStatsAdapter struct {
	legacy legacyPublicDiscoveryNetworkStatsCapability
}

func (a legacyPublicDiscoveryNetworkStatsAdapter) GetPublicDiscoveryNetworkStats(
	ctx context.Context,
	hashtagLimit int,
) (PublicDiscoveryNetworkStats, error) {
	row, err := a.legacy.GetPublicDiscoveryNetworkStats(ctx, hashtagLimit)
	if err != nil {
		return PublicDiscoveryNetworkStats{}, err
	}
	return publicDiscoveryNetworkStatsFromStore(row), nil
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

type legacyTrendingHashtagsCapability interface {
	GetTrendingHashtags(ctx context.Context, window time.Duration, limit int, offset int) ([]store.TrendingHashtag, error)
}

type legacyTrendingHashtagsAdapter struct {
	legacy legacyTrendingHashtagsCapability
}

func (a legacyTrendingHashtagsAdapter) GetTrendingHashtags(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]TrendingHashtag, error) {
	rows, err := a.legacy.GetTrendingHashtags(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]TrendingHashtag, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingHashtagFromStore(row))
	}
	return out, nil
}

type legacyTrendingNotesCapability interface {
	GetTrendingNotes(ctx context.Context, window time.Duration, limit int, offset int) ([]store.TrendingNote, error)
}

type legacyTrustQualifiedTrendingNotesCapability interface {
	GetTrustQualifiedTrendingNotes(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		mode string,
		policy store.TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]store.TrustQualifiedTrendingNote, bool, error)
}

type legacyTrendingNotesAdapter struct {
	legacy legacyTrendingNotesCapability
}

func (a legacyTrendingNotesAdapter) GetTrendingNotes(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]TrendingNote, error) {
	rows, err := a.legacy.GetTrendingNotes(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]TrendingNote, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingNoteFromStore(row))
	}
	return out, nil
}

type legacyTrustQualifiedTrendingNotesAdapter struct {
	legacy legacyTrustQualifiedTrendingNotesCapability
}

func (a legacyTrustQualifiedTrendingNotesAdapter) GetTrustQualifiedTrendingNotes(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	mode string,
	policy TrustQualificationPolicy,
	maxStaleness time.Duration,
) ([]trustedNoteCandidate, bool, error) {
	rows, ready, err := a.legacy.GetTrustQualifiedTrendingNotes(ctx, window, limit, offset, mode, store.TrustQualificationPolicy{
		MaxHops:      policy.MaxHops,
		MinimumScore: policy.MinimumScore,
	}, maxStaleness)
	if err != nil {
		return nil, false, err
	}
	out := make([]trustedNoteCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustedNoteCandidate{
			note:    trendingNoteFromStore(row.Note),
			trusted: row.Trusted,
		})
	}
	return out, ready, nil
}

type legacyTrendingProfilesCapability interface {
	GetTrendingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]store.TrendingProfile, error)
}

type legacyTrustQualifiedTrendingProfilesCapability interface {
	GetTrustQualifiedTrendingProfiles(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		rising bool,
		mode string,
		policy store.TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]store.TrustQualifiedTrendingProfile, bool, error)
}

type legacyTrendingProfilesAdapter struct {
	legacy legacyTrendingProfilesCapability
}

func (a legacyTrendingProfilesAdapter) GetTrendingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]TrendingProfile, error) {
	rows, err := a.legacy.GetTrendingProfiles(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]TrendingProfile, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingProfileFromStore(row))
	}
	return out, nil
}

type legacyTrustQualifiedTrendingProfilesAdapter struct {
	legacy legacyTrustQualifiedTrendingProfilesCapability
}

func (a legacyTrustQualifiedTrendingProfilesAdapter) GetTrustQualifiedTrendingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	rising bool,
	mode string,
	policy TrustQualificationPolicy,
	maxStaleness time.Duration,
) ([]trustedProfileCandidate, bool, error) {
	rows, ready, err := a.legacy.GetTrustQualifiedTrendingProfiles(ctx, window, limit, offset, rising, mode, store.TrustQualificationPolicy{
		MaxHops:      policy.MaxHops,
		MinimumScore: policy.MinimumScore,
	}, maxStaleness)
	if err != nil {
		return nil, false, err
	}
	out := make([]trustedProfileCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustedProfileCandidate{
			profile: trendingProfileFromStore(row.Profile),
			trusted: row.Trusted,
		})
	}
	return out, ready, nil
}

type legacyRisingProfilesCapability interface {
	GetRisingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]store.TrendingProfile, error)
}

type legacyRisingProfilesAdapter struct {
	legacy legacyRisingProfilesCapability
}

func (a legacyRisingProfilesAdapter) GetRisingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]TrendingProfile, error) {
	rows, err := a.legacy.GetRisingProfiles(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]TrendingProfile, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingProfileFromStore(row))
	}
	return out, nil
}
