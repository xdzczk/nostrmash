package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

const rankedPubkeyCountCacheTTL = 5 * time.Minute

type rankedPubkeyCountCache struct {
	mu        sync.Mutex
	count     int64
	expiresAt time.Time
}

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
	rankedPubkeyCountCache          *rankedPubkeyCountCache
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
	FallbackReader                  FallbackReader
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

	// Optional typed capability groups. When a group is non-nil it is wired
	// whole (the typed production path). When nil, the corresponding
	// capabilities are discovered by asserting the capability interfaces on the
	// reader (the partial-fake path used by tests). NewServiceFromStore fills
	// every group from the single store value.
	Curated     CuratedReads
	Trust       TrustReads
	DM          DMReads
	Moderation  ModerationReads
	Replaceable ReplaceableReads
	Social      SocialReads
	Event       EventReads
	Thread      ThreadReads
	NotePage    NotePageReads
}

// NewServiceFromStore builds a Service from a single complete typed store
// value. Every capability group is wired whole from the store (no runtime
// capability probing), and the composition root proves completeness at compile
// time (var _ query.FullStoreReader = (*store.PostgresStore)(nil)). This is the
// production entry point.
func NewServiceFromStore(store FullStoreReader, options ServiceOptions) (Service, error) {
	if store == nil {
		return Service{}, fmt.Errorf("query: store reader is required")
	}
	options.Curated = store
	options.Trust = store
	options.DM = store
	options.Moderation = store
	options.Replaceable = store
	options.Social = store
	options.Event = store
	options.Thread = store
	options.NotePage = store
	// probeSource is unused because every group is explicit, but pass the store
	// anyway so the secondary author-analytics/advanced-search adapters resolve.
	return buildService(readModelReaderAdapter{readModel: store}, store, options)
}

// NewServiceFromStoreReader builds a Service from a readmodel-shaped core store
// reader. When the reader also satisfies the complete FullStoreReader surface it
// is wired whole (the production path); otherwise optional capabilities are
// discovered by asserting the capability interfaces on the reader (the
// partial-reader path used by tests and reduced deployments).
func NewServiceFromStoreReader(reader StoreReader, options ServiceOptions) (Service, error) {
	if reader == nil {
		return Service{}, fmt.Errorf("query: store reader is required")
	}
	if full, ok := reader.(FullStoreReader); ok {
		return NewServiceFromStore(full, options)
	}
	return buildService(readModelReaderAdapter{readModel: reader}, reader, options)
}

func NewService(reader Reader) (Service, error) {
	return NewServiceWithOptions(reader, ServiceOptions{})
}

func NewServiceWithOptions(reader Reader, options ServiceOptions) (Service, error) {
	if reader == nil {
		return Service{}, fmt.Errorf("query: reader is required")
	}
	return buildService(reader, reader, options)
}

// buildService is the shared constructor body. nativeReader is the query-shaped
// core reader the Service consumes; probeSource is the value whose capability
// interfaces are asserted when a capability group is not supplied explicitly.
func buildService(nativeReader Reader, probeSource any, options ServiceOptions) (Service, error) {
	adaptedReader := nativeReader
	adaptedFallback := options.FallbackReader
	discoveryMode, err := ParseTrustMode(options.DiscoveryCandidateTrustMode, TrustModeOpen)
	if err != nil {
		return Service{}, fmt.Errorf("discovery trust mode: %w", err)
	}
	mode := discoveryMode.String()
	searchTrustMode, err := ParseTrustMode(options.SearchRankingTrustMode, TrustModePreferTrusted)
	if err != nil {
		return Service{}, fmt.Errorf("search ranking trust mode: %w", err)
	}
	searchMode := searchTrustMode.String()
	fallbackTrustMode, err := ParseTrustMode(options.FallbackFetchTrustMode, TrustModeOpen)
	if err != nil {
		return Service{}, fmt.Errorf("fallback fetch trust mode: %w", err)
	}
	fallbackMode := fallbackTrustMode.String()
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
		capabilities:           adaptServiceCapabilities(probeSource, options),
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
		rankedPubkeyCountCache:          &rankedPubkeyCountCache{},
	}, nil
}

var (
	_ ThreadService     = Service{}
	_ EventService      = Service{}
	_ ProfileService    = Service{}
	_ TrustService      = Service{}
	_ ReadOrchestration = Service{}

	// ErrThreadEventNotFound indicates the focal/root event for a thread was not found.
	// This is intentionally narrower than readmodel.ErrNotFound so transports can preserve
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
	return errors.Is(err, readmodel.ErrNotFound)
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
	if s.meilisearch == nil {
		return "postgres"
	}
	if available, ok := s.meilisearch.(interface{ Available() bool }); ok && !available.Available() {
		return "degraded"
	}
	return "meilisearch"
}
