package store

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/model"
)

// PostgresStore persists Layer 1 ingest records into Postgres.
type PostgresStore struct {
	pool         *pgxpool.Pool
	jobPublisher jobs.CanonicalEventPublisher
}

// CanonicalInsertResult exposes idempotent upsert outcomes for metrics.
type CanonicalInsertResult struct {
	EventInserted bool
}

var ErrNotFound = errors.New("not found")

// ProfileProjection is the latest projected profile metadata for one pubkey.
type ProfileProjection struct {
	Pubkey            string
	MetadataEventID   string
	MetadataCreatedAt int64
	ProfileJSON       json.RawMessage
}

// ProfilePublicStatsProjection captures denormalized public-facing profile counters.
type ProfilePublicStatsProjection struct {
	Pubkey           string
	FollowerCount    int64
	FollowingCount   int64
	NoteCount        int64
	ReplyCount       int64
	RecentActivityAt *int64
}

type EventCounts struct {
	EventID       string
	ReplyCount    int64
	ReactionCount int64
	RepostCount   int64
	Consistency   string
}

type ThreadSummaryProjection struct {
	RootEventID      string
	ReplyCount       int64
	ParticipantCount int
	MaxDepth         int
	LastActivityAt   int64
	Replies24h       int64
	Replies7d        int64
	Consistency      string
}

type HotConversation struct {
	RootEventID      string
	AuthorPubkey     string
	CreatedAt        int64
	Content          string
	ReplyCount       int64
	ParticipantCount int
	LastActivityAt   int64
	Replies24h       int64
	Replies7d        int64
	VelocityScore    float64
	Consistency      string
}

type EventWithProvenance struct {
	Event  json.RawMessage
	Relays []model.EventRelay
}

type EventOrderCursor struct {
	CreatedAt int64
	ID        string
}

type AuthorAnalyticsSummaryProjection struct {
	Pubkey                   string
	WindowDays               int
	PostCount                int64
	NoteCount                int64
	ReplyCount               int64
	ActiveDays               int
	EngagementReceived       int64
	EngagementGiven          int64
	CadencePostsPerDay       float64
	CadencePostsPerActiveDay float64
	RecentActivityAt         *int64
	MediaMix                 AuthorMediaMixStatsProjection
	QuoteRepost              AuthorQuoteRepostWindowProjection
}

type AuthorRelayFootprintProjection struct {
	Pubkey           string
	RelayCount       int64
	SeenOnEventCount int64
	TopRelays        []RelayUsageSummary
}

type AuthorQuoteRepostWindowProjection struct {
	QuotesMade      int64
	RepostsMade     int64
	QuotesReceived  int64
	RepostsReceived int64
}

type QuoteRepostLinkedNoteProjection struct {
	EventID      string
	AuthorPubkey string
	CreatedAt    int64
	Content      string
}

type QuoteRepostActivityProjection struct {
	EventID     string
	ActorPubkey string
	CreatedAt   int64
	Action      string
	Quote       string
	LinkedNote  QuoteRepostLinkedNoteProjection
}

type NoteQuoteRepostLinkageProjection struct {
	EventID        string
	QuoteCount     int64
	RepostCount    int64
	RecentActivity []QuoteRepostActivityProjection
}

type AuthorTopicStatsProjection struct {
	Pubkey     string
	WindowDays int
	Hashtag    string
	UsageCount int64
	ActiveDays int
}

type EventDomainLinkProjection struct {
	EventID string
	URL     string
	Domain  string
}

type DomainStatProjection struct {
	Domain        string
	LinkCount     int64
	NoteCount     int64
	UniqueAuthors int64
}

type DomainActivityProjection struct {
	LinkCount     int64
	NoteCount     int64
	UniqueAuthors int64
}

type DomainActivityStatsProjection struct {
	Last24h DomainActivityProjection
	Last7d  DomainActivityProjection
	Last30d DomainActivityProjection
	All     DomainActivityProjection
}

type DomainSummaryProjection struct {
	Domain        string
	LatestEventAt *int64
	Activity      DomainActivityStatsProjection
	RecentNotes   []TrendingNote
	TopNotes      []TrendingNote
}

type AuthorMediaMixStatsProjection struct {
	Pubkey               string
	WindowDays           int
	TotalPosts           int64
	WithImageCount       int64
	WithVideoCount       int64
	WithLinkCount        int64
	WithArticleCount     int64
	TextOnlyCount        int64
	TotalAttachmentCount int64
}

type AuthorActivityWindowBucketProjection struct {
	Pubkey             string
	WindowDays         int
	DayOfWeek          int
	HourOfDay          int
	EngagementReceived int64
	ReplyReceived      int64
	ReactionReceived   int64
	RepostReceived     int64
	ZapReceived        int64
}

type AuthorPostingPatternBucketProjection struct {
	Pubkey     string
	WindowDays int
	DayOfWeek  int
	HourOfDay  int
	PostCount  int64
	NoteCount  int64
	ReplyCount int64
}

type AuthorTopNoteProjection struct {
	EventID             string
	CreatedAt           int64
	Content             string
	ReplyCount          int64
	ReactionCount       int64
	RepostCount         int64
	ZapCount            int64
	ZapMSats            int64
	WeightedEngagement  float64
	MediaSegment        string
	PrimaryTopicHashtag *string
}

type AuthorRecycleCandidateProjection struct {
	EventID               string
	CreatedAt             int64
	Content               string
	ReplyCount            int64
	ReactionCount         int64
	RepostCount           int64
	ZapCount              int64
	ZapMSats              int64
	WeightedEngagement    float64
	PerformancePercentile float64
	HasRecentRepostMarker bool
	IsReply               bool
	MediaSegment          string
	PrimaryTopicHashtag   *string
}

type AuthorPerformanceAggregateProjection struct {
	NoteCount                 int64
	TotalWeightedEngagement   float64
	AverageWeightedEngagement float64
	MedianWeightedEngagement  float64
	TotalReplyCount           int64
	TotalReactionCount        int64
	TotalRepostCount          int64
	TotalZapCount             int64
	TotalZapMSats             int64
	AverageReplyCount         float64
	AverageReactionCount      float64
	AverageRepostCount        float64
	AverageZapCount           float64
	MedianReplyCount          float64
	MedianReactionCount       float64
	MedianRepostCount         float64
	MedianZapCount            float64
}

type GroupedNoteAnalyticsQuery struct {
	Pubkey        string
	WindowDays    int
	GroupKind     string
	GroupKey      string
	MetadataTag   string
	TopNotesLimit int
	TopicsLimit   int
}

type GroupedEngagementTotalsProjection struct {
	ReplyCount    int64
	ReactionCount int64
	RepostCount   int64
	ZapCount      int64
	ZapMSats      int64
}

type GroupedMediaSummaryProjection struct {
	TotalPosts           int64
	WithImageCount       int64
	WithVideoCount       int64
	WithLinkCount        int64
	WithArticleCount     int64
	TextOnlyCount        int64
	TotalAttachmentCount int64
}

type GroupedTopNoteProjection struct {
	EventID             string
	CreatedAt           int64
	Content             string
	ReplyCount          int64
	ReactionCount       int64
	RepostCount         int64
	ZapCount            int64
	ZapMSats            int64
	WeightedEngagement  float64
	MediaSegment        string
	PrimaryTopicHashtag *string
}

type GroupedTopicSummaryProjection struct {
	Hashtag    string
	UsageCount int64
	ActiveDays int
}

type GroupedNoteAnalyticsProjection struct {
	Pubkey      string
	WindowDays  int
	GroupKind   string
	GroupKey    string
	MetadataTag string
	NoteCount   int64
	Engagement  GroupedEngagementTotalsProjection
	Media       GroupedMediaSummaryProjection
	TopNotes    []GroupedTopNoteProjection
	TopTopics   []GroupedTopicSummaryProjection
}

type SearchDocumentProjection struct {
	EntityType     string
	EntityID       string
	Title          string
	Body           string
	Aliases        []string
	IdentityTokens []string
	Freshness      time.Time
	Popularity     float64
	TrustScore     *float64
	Score          float64
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{
		pool:         pool,
		jobPublisher: jobs.NewQueuePublisher(5),
	}
}

// SetCanonicalEventJobPublisher overrides canonical-event downstream publication.
func (s *PostgresStore) SetCanonicalEventJobPublisher(publisher jobs.CanonicalEventPublisher) {
	if s == nil {
		return
	}
	s.jobPublisher = publisher
}

func dbResultFromErr(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, ErrNotFound) {
		return "not_found"
	}
	return "error"
}
