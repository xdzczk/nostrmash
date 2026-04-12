package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

type Reader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventWithProvenance(ctx context.Context, id string) (EventWithProvenance, error)
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error)
	GetProfileByPubkey(ctx context.Context, pubkey string) (Profile, error)
	GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error)
	GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (ProfilePublicStats, error)
	GetAuthorAnalyticsSummary(ctx context.Context, pubkey string) (AuthorAnalyticsSummary, error)
	GetAuthorQuoteRepostRecentActivity(ctx context.Context, pubkey string, limit int) ([]QuoteRepostActivity, error)
	GetAuthorTopicStats(ctx context.Context, pubkey string, windowDays int, limit int) ([]AuthorTopicStat, error)
	GetAuthorTopLanguages(ctx context.Context, pubkey string, windowDays int, limit int) ([]LanguageSummary, error)
	GetAuthorMediaMixStats(ctx context.Context, pubkey string, windowDays int) (AuthorAnalyticsMediaMix, error)
	GetAuthorActivityWindows(ctx context.Context, pubkey string, windowDays int) (AuthorActivityWindows, error)
	GetAuthorPostingPatterns(ctx context.Context, pubkey string, windowDays int) (AuthorPostingPatterns, error)
	GetAuthorTopNotes(ctx context.Context, pubkey string, windowDays int, limit int) ([]AuthorTopNote, error)
	GetAuthorRecycleCandidates(
		ctx context.Context,
		pubkey string,
		windowDays int,
		minAgeDays int,
		minPerformancePercentile float64,
		includeReplies bool,
		excludeRecentlyReposted bool,
		recentRepostWindowDays int,
		limit int,
	) ([]AuthorRecycleCandidate, error)
	GetAuthorPerformanceSummary(
		ctx context.Context,
		pubkey string,
		windowDays int,
	) (AuthorPerformanceSummary, error)
	GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventCounts(ctx context.Context, eventID string) (EventCounts, error)
	GetEventReplies(ctx context.Context, eventID string, limit int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error)
	GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
	ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error)
	GetContactListByPubkey(ctx context.Context, pubkey string) (ContactList, error)
	GetRelayListByPubkey(ctx context.Context, pubkey string) (RelayList, error)
	SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error)
	SearchProfiles(ctx context.Context, query string, limit int) ([]Profile, error)
	GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
	GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
}

// ThreadReader is the minimal dependency needed for thread assembly orchestration.
type ThreadReader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventReplies(ctx context.Context, eventID string, limit int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error)
	GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
}

// EventReader is the minimal dependency needed for event lookup/count orchestration.
type EventReader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventCounts(ctx context.Context, eventID string) (EventCounts, error)
}

// ProfileReader is the minimal dependency needed for profile/user-info orchestration.
type ProfileReader interface {
	GetProfileByPubkey(ctx context.Context, pubkey string) (Profile, error)
	GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error)
	GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (ProfilePublicStats, error)
}

type MeilisearchSearcher interface {
	SearchNotes(
		ctx context.Context,
		query string,
		sort string,
		window *time.Duration,
		language string,
		limit int,
		offset int,
	) ([]json.RawMessage, error)
	SearchProfiles(ctx context.Context, query string, sort string, limit int, offset int) ([]Profile, error)
	SuggestProfiles(ctx context.Context, query string, limit int) ([]Profile, error)
	SuggestHashtags(ctx context.Context, query string, limit int) ([]HashtagSuggestion, error)
	SearchDocuments(ctx context.Context, query string, limit int) ([]SearchDocument, error)
}

type Service struct {
	reader                          Reader
	capabilities                    serviceCapabilities
	fallback                        FallbackReader
	fallbackPersister               FallbackProfilePersister
	fallbackEventPersister          FallbackEventPersister
	fallbackFetchMode               string
	fallbackFetchPolicy             TrustQualificationPolicy
	fallbackMaxAttempts             int
	fallbackMaxTimeBudget           time.Duration
	fallbackDirectLookups           bool
	discoveryTrustMode              string
	discoveryTrustPolicy            TrustQualificationPolicy
	discoveryTrustScanSize          int
	discoveryProjectionMaxStaleness time.Duration
	searchTrustMode                 string
	searchTrustPolicy               TrustQualificationPolicy
	searchTrustScanSize             int
	retentionHooks                  TrustRetentionHooks
	meilisearch                     MeilisearchSearcher
}

// FallbackReader fetches entity-shaped data from configured relays on local miss.
type FallbackReader interface {
	FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error)
}

// FallbackProfilePersister saves profiles obtained from relay fallback
// into the local store so subsequent lookups avoid relay round-trips.
type FallbackProfilePersister interface {
	PersistFallbackProfile(ctx context.Context, profile Profile) error
}

// FallbackEventPersister saves raw events obtained from relay fallback
// into the local store so subsequent lookups avoid relay round-trips.
type FallbackEventPersister interface {
	PersistFallbackEvent(ctx context.Context, eventID string, raw json.RawMessage) error
}

type ServiceOptions struct {
	FallbackReader                  any
	FallbackProfilePersister        FallbackProfilePersister
	FallbackEventPersister          FallbackEventPersister
	FallbackFetchTrustMode          string
	FallbackFetchMinimumScore       float64
	FallbackFetchMaxHops            int
	FallbackFetchMaxAttempts        int
	FallbackFetchMaxTimeBudget      time.Duration
	FallbackFetchAllowDirectLookup  *bool
	DiscoveryCandidateTrustMode     string
	SearchRankingTrustMode          string
	DiscoveryCandidateMinimumScore  float64
	DiscoveryCandidateMaxHops       int
	DiscoveryProjectionMaxStaleness time.Duration
	TrustRetentionHooks             TrustRetentionHooks
	MeilisearchSearcher             MeilisearchSearcher
}

func NewService(reader any) (Service, error) {
	return NewServiceWithOptions(reader, ServiceOptions{})
}

func NewServiceWithOptions(reader any, options ServiceOptions) (Service, error) {
	adaptedReader, err := adaptReader(reader)
	if err != nil {
		return Service{}, err
	}
	adaptedFallback, err := adaptFallbackReader(options.FallbackReader)
	if err != nil {
		return Service{}, err
	}
	mode := strings.ToLower(strings.TrimSpace(options.DiscoveryCandidateTrustMode))
	if mode == "" {
		mode = trustModeOpen
	}
	if mode != trustModeOpen && mode != trustModePreferTrusted && mode != trustModeTrustedOnly {
		return Service{}, fmt.Errorf("invalid discovery trust mode %q", mode)
	}
	searchMode := strings.ToLower(strings.TrimSpace(options.SearchRankingTrustMode))
	if searchMode == "" {
		searchMode = trustModePreferTrusted
	}
	if searchMode != trustModeOpen && searchMode != trustModePreferTrusted && searchMode != trustModeTrustedOnly {
		return Service{}, fmt.Errorf("invalid search ranking trust mode %q", searchMode)
	}
	fallbackMode := strings.ToLower(strings.TrimSpace(options.FallbackFetchTrustMode))
	if fallbackMode == "" {
		fallbackMode = trustModeOpen
	}
	if fallbackMode != trustModeOpen && fallbackMode != trustModePreferTrusted && fallbackMode != trustModeTrustedOnly {
		return Service{}, fmt.Errorf("invalid fallback fetch trust mode %q", fallbackMode)
	}
	maxHops := options.DiscoveryCandidateMaxHops
	if maxHops <= 0 {
		maxHops = 3
	}
	minScore := options.DiscoveryCandidateMinimumScore
	if minScore < 0 {
		minScore = 0
	}
	fallbackMaxHops := options.FallbackFetchMaxHops
	if fallbackMaxHops <= 0 {
		fallbackMaxHops = maxHops
	}
	fallbackMinScore := options.FallbackFetchMinimumScore
	if fallbackMinScore < 0 {
		fallbackMinScore = minScore
	}
	fallbackMaxAttempts := options.FallbackFetchMaxAttempts
	if fallbackMaxAttempts <= 0 {
		fallbackMaxAttempts = 1
	}
	fallbackMaxTimeBudget := options.FallbackFetchMaxTimeBudget
	if fallbackMaxTimeBudget <= 0 {
		fallbackMaxTimeBudget = 2 * time.Second
	}
	fallbackDirectLookups := true
	if options.FallbackFetchAllowDirectLookup != nil {
		fallbackDirectLookups = *options.FallbackFetchAllowDirectLookup
	}
	discoveryProjectionMaxStaleness := options.DiscoveryProjectionMaxStaleness
	if discoveryProjectionMaxStaleness <= 0 {
		discoveryProjectionMaxStaleness = 10 * time.Minute
	}
	retentionHooks := options.TrustRetentionHooks
	if retentionHooks.isZero() {
		retentionHooks = DefaultTrustRetentionHooks(trustModeOpen)
	}
	if strings.TrimSpace(retentionHooks.Mode) == "" {
		retentionHooks.Mode = trustModeOpen
	}
	if err := retentionHooks.Validate(); err != nil {
		return Service{}, err
	}
	return Service{
		reader:                 adaptedReader,
		capabilities:           adaptServiceCapabilities(reader),
		fallback:               adaptedFallback,
		fallbackPersister:      options.FallbackProfilePersister,
		fallbackEventPersister: options.FallbackEventPersister,
		fallbackFetchMode:      fallbackMode,
		fallbackFetchPolicy:    TrustQualificationPolicy{MaxHops: fallbackMaxHops, MinimumScore: fallbackMinScore},
		fallbackMaxAttempts:    fallbackMaxAttempts,
		fallbackMaxTimeBudget:  fallbackMaxTimeBudget,
		fallbackDirectLookups:  fallbackDirectLookups,
		discoveryTrustMode:     mode,
		discoveryTrustPolicy:   TrustQualificationPolicy{MaxHops: maxHops, MinimumScore: minScore},
		// Keep trust-aware candidate scans bounded and predictable.
		discoveryTrustScanSize:          400,
		discoveryProjectionMaxStaleness: discoveryProjectionMaxStaleness,
		searchTrustMode:                 searchMode,
		searchTrustPolicy:               TrustQualificationPolicy{MaxHops: maxHops, MinimumScore: minScore},
		searchTrustScanSize:             400,
		retentionHooks:                  retentionHooks,
		meilisearch:                     options.MeilisearchSearcher,
	}, nil
}

var (
	_ ThreadService     = Service{}
	_ EventService      = Service{}
	_ ProfileService    = Service{}
	_ TrustService      = Service{}
	_ ReadOrchestration = Service{}

	// ErrThreadEventNotFound indicates the focal/root event for a thread was not found.
	// This is intentionally narrower than store.ErrNotFound so transports can preserve
	// historical status code behavior for non-root thread fetch failures.
	ErrThreadEventNotFound = errors.New("thread event not found")

	// ErrUnsupportedCapability marks optional reader capabilities that are absent.
	// Query methods should wrap this sentinel with stable context.
	ErrUnsupportedCapability = errors.New("unsupported capability")
	ErrInvalidHashtag        = errors.New("invalid hashtag")
	ErrInvalidDomain         = errors.New("invalid domain")
)

func normalizeUniqueStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func IsNotFound(err error) bool {
	return errors.Is(err, store.ErrNotFound)
}

func unsupportedCapabilityError(feature string) error {
	feature = strings.TrimSpace(feature)
	if feature == "" {
		return ErrUnsupportedCapability
	}
	return fmt.Errorf("query: %s unsupported: %w", feature, ErrUnsupportedCapability)
}

func IsUnsupportedCapability(err error) bool {
	return errors.Is(err, ErrUnsupportedCapability)
}

func (s Service) SearchEngineName() string {
	if s.meilisearch != nil {
		return "meilisearch"
	}
	return "postgres"
}
