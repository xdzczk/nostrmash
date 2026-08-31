package readmodel

import (
	"encoding/json"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

// The types below are storage-neutral read-model projections moved out of
// internal/store so higher layers (query) can consume them without importing
// the concrete store package. store re-exports each via a type alias.

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

type AuthorPostingPatternBucketProjection struct {
	Pubkey     string
	WindowDays int
	DayOfWeek  int
	HourOfDay  int
	PostCount  int64
	NoteCount  int64
	ReplyCount int64
}

type AuthorQuoteRepostWindowProjection struct {
	QuotesMade      int64
	RepostsMade     int64
	QuotesReceived  int64
	RepostsReceived int64
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

type AuthorRelayFootprintProjection struct {
	Pubkey           string
	RelayCount       int64
	SeenOnEventCount int64
	TopRelays        []RelayUsageSummary
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

type AuthorTopicStatsProjection struct {
	Pubkey     string
	WindowDays int
	Hashtag    string
	UsageCount int64
	ActiveDays int
}

type ContactListProjection struct {
	Pubkey          string
	EventID         string
	CreatedAt       int64
	DerivationVer   int
	ContactsJSONRaw json.RawMessage
}

type CuratedFeaturedAuthor struct {
	Pubkey string `json:"pubkey"`
	Rank   int    `json:"rank"`
}

type CuratedReadsTopic struct {
	Topic string `json:"topic"`
	Rank  int    `json:"rank"`
}

type CuratedRecommendedRead struct {
	EventID string `json:"event_id"`
	Title   string `json:"title"`
	URL     string `json:"url"`
	Rank    int    `json:"rank"`
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

type DomainStatProjection struct {
	Domain        string
	LinkCount     int64
	NoteCount     int64
	UniqueAuthors int64
}

type DomainSummaryProjection struct {
	Domain        string
	LatestEventAt *int64
	Activity      DomainActivityStatsProjection
	RecentNotes   []TrendingNote
	TopNotes      []TrendingNote
}

type EventCounts struct {
	EventID       string
	ReplyCount    int64
	ReactionCount int64
	RepostCount   int64
	ZapCount      int64
	ZapMSats      int64
	Consistency   string
}

type EventDomainLinkProjection struct {
	EventID string
	URL     string
	Domain  string
}

type EventOrderCursor struct {
	CreatedAt int64
	ID        string
}

type EventWithProvenance struct {
	Event  json.RawMessage
	Relays []model.EventRelay
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

type GroupedNoteAnalyticsQuery struct {
	Pubkey        string
	WindowDays    int
	GroupKind     string
	GroupKey      string
	MetadataTag   string
	TopNotesLimit int
	TopicsLimit   int
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

type HashtagActivity struct {
	EventCount    int64 `json:"event_count"`
	UniqueAuthors int64 `json:"unique_authors"`
}

type HashtagActivityStats struct {
	Last24h HashtagActivity `json:"24h"`
	Last7d  HashtagActivity `json:"7d"`
	Last30d HashtagActivity `json:"30d"`
	All     HashtagActivity `json:"all"`
}

type HashtagSummary struct {
	Hashtag       string               `json:"hashtag"`
	LatestEventAt *int64               `json:"latest_event_at,omitempty"`
	Activity      HashtagActivityStats `json:"activity"`
}

type HotConversation struct {
	RootEventID      string
	AuthorPubkey     string
	CreatedAt        int64
	Content          string
	ReplyCount       int64
	RepostCount      int64
	ReactionCount    int64
	ZapCount         int64
	ZapMSats         int64
	ParticipantCount int
	LastActivityAt   int64
	Replies24h       int64
	Replies7d        int64
	VelocityScore    float64
	Consistency      string
}

type LanguageSummary struct {
	Language string `json:"language"`
	Count    int64  `json:"count"`
}

type NetworkStats struct {
	Events   int64 `json:"events"`
	Profiles int64 `json:"profiles"`
	Relays   int64 `json:"relays"`
}

type NoteConversationVelocity struct {
	Replies24h int64
	Replies7d  int64
}

type NoteQuoteRepostLinkageProjection struct {
	EventID        string
	QuoteCount     int64
	RepostCount    int64
	RecentActivity []QuoteRepostActivityProjection
}

type NoteStats struct {
	EventID         string
	ReplyCount      int64
	ReactionCount   int64
	RepostCount     int64
	ZapCount        int64
	ZapMSats        int64
	HasImage        bool
	HasVideo        bool
	HasLink         bool
	HasArticle      bool
	AttachmentCount int
}

type ProfileProjection struct {
	Pubkey            string
	MetadataEventID   string
	MetadataCreatedAt int64
	ProfileJSON       json.RawMessage
}

type ProfilePublicStatsProjection struct {
	Pubkey           string
	FollowerCount    int64
	FollowingCount   int64
	NoteCount        int64
	ReplyCount       int64
	RecentActivityAt *int64
}

type PublicDiscoveryNetworkStats struct {
	EventsIngested    int64                   `json:"events_ingested"`
	ProjectedProfiles int64                   `json:"projected_profiles"`
	ComputedAt        *time.Time              `json:"computed_at,omitempty"`
	Relays            int64                   `json:"relays"`
	RelaySummary      RelaySummaryStats       `json:"relay_summary"`
	TopRelays         []RelayUsageSummary     `json:"top_relays,omitempty"`
	ActiveAuthors     WindowedCount           `json:"active_authors"`
	NoteVolume        WindowedCount           `json:"note_volume"`
	TopHashtags       *TrendingHashtagWindows `json:"top_hashtags,omitempty"`
	TopLanguages24h   []LanguageSummary       `json:"top_languages_24h,omitempty"`
	TopLanguages7d    []LanguageSummary       `json:"top_languages_7d,omitempty"`
}

type DiscoveryStatsSeriesPoint struct {
	T time.Time `json:"-"`
	V int64     `json:"-"`
}

type DiscoveryStatsSeries struct {
	Metric     string
	Window     string
	ComputedAt *time.Time
	Points     []DiscoveryStatsSeriesPoint
}

type QuoteRepostActivityProjection struct {
	EventID     string
	ActorPubkey string
	CreatedAt   int64
	Action      string
	Quote       string
	LinkedNote  QuoteRepostLinkedNoteProjection
}

type QuoteRepostLinkedNoteProjection struct {
	EventID      string
	AuthorPubkey string
	CreatedAt    int64
	Content      string
}

type RelatedHashtag struct {
	Hashtag             string `json:"hashtag"`
	CoOccurrenceCount   int64  `json:"co_occurrence_count"`
	CoOccurrenceAuthors int64  `json:"co_occurrence_authors"`
}

type RelatedNote struct {
	EventID       string
	AuthorPubkey  string
	CreatedAt     int64
	Content       string
	Event         json.RawMessage
	ReplyCount    int64
	ReactionCount int64
	RepostCount   int64
	ZapCount      int64
	ZapMSats      int64
	Reasons       []string
	RankScore     int64
}

type RelatedProfile struct {
	Pubkey               string   `json:"pubkey"`
	TopicOverlap         int64    `json:"topic_overlap"`
	ReplyAdjacency       int64    `json:"reply_adjacency"`
	InteractionAdjacency int64    `json:"interaction_adjacency"`
	QuoteRepostAdjacency int64    `json:"quote_repost_adjacency"`
	Reasons              []string `json:"reasons"`
	Score                int64    `json:"score"`
}

type RelayListProjection struct {
	Pubkey        string
	EventID       string
	CreatedAt     int64
	DerivationVer int
	RelaysJSONRaw json.RawMessage
}

type RelaySummaryStats struct {
	Total         int64         `json:"total"`
	Active24h     int64         `json:"active_24h"`
	Active7d      int64         `json:"active_7d"`
	EventVolume   WindowedCount `json:"event_volume"`
	UniqueAuthors WindowedCount `json:"unique_authors"`
}

type RelayUsageSummary struct {
	RelayURL      string `json:"relay_url"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
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

type TrendingHashtag struct {
	Hashtag       string `json:"hashtag"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

type TrendingHashtagWindows struct {
	Last24h []TrendingHashtag `json:"24h"`
	Last7d  []TrendingHashtag `json:"7d"`
}

type TrendingNote struct {
	EventID       string `json:"event_id"`
	AuthorPubkey  string `json:"author_pubkey"`
	CreatedAt     int64  `json:"created_at"`
	Content       string `json:"content"`
	Language      string `json:"language,omitempty"`
	ReplyCount    int64  `json:"reply_count"`
	RepostCount   int64  `json:"repost_count"`
	ReactionCount int64  `json:"reaction_count"`
	ZapCount      int64  `json:"zap_count"`
	ZapMSats      int64  `json:"zap_msats"`
	Score         float64
}

type TrendingProfile struct {
	Pubkey                   string   `json:"pubkey"`
	Score                    float64  `json:"score"`
	RecentPostCount          int64    `json:"recent_post_count"`
	RecentReplyCount         int64    `json:"recent_reply_count"`
	RecentEngagementReceived int64    `json:"recent_engagement_received"`
	RecentNewFollowers       int64    `json:"recent_new_followers"`
	RecentZapVolumeMSats     int64    `json:"recent_zap_volume_msats"`
	RecentActiveDays         int      `json:"recent_active_days"`
	RecentActivityAt         *int64   `json:"recent_activity_at,omitempty"`
	FollowerCount            int64    `json:"follower_count"`
	ScoredEngagementReceived *float64 `json:"-"`
	ScoredNewFollowers       *float64 `json:"-"`
}

type TrustGlobalScore struct {
	Pubkey         string
	Score          float64
	Rank           int64
	RunID          int64
	DerivationName string
	TargetVersion  int
	ComputedAt     time.Time
}

// TrustPubkeyLatest is the denormalized hop+score row for one pubkey.
type TrustPubkeyLatest struct {
	Pubkey      string
	MinHops     *int
	IsSeed      bool
	Score       *float64
	Rank        *int64
	SourceRunID *int64
	ComputedAt  *time.Time
	UpdatedAt   time.Time
}

type TrustQualification struct {
	Pubkey       string
	Trusted      bool
	IsSeed       bool
	DistanceHops *int
	Score        *float64
	Rank         *int64
	SourceRunID  *int64
}

type TrustQualificationPolicy struct {
	MaxHops      int
	MinimumScore float64
}

type TrustQualifiedTrendingNote struct {
	Note    TrendingNote
	Trusted bool
}

type TrustQualifiedTrendingProfile struct {
	Profile TrendingProfile
	Trusted bool
}

type TrustRun struct {
	ID                 int64
	DerivationName     string
	TargetVersion      int
	Status             string
	JobID              *int64
	Attempts           int
	InputFollowerEdges int64
	ScoreRowsPublished int64
	RedisSnapshotRef   *string
	CurrentPhase       *string
	SyncJobID          *int64
	ComputeJobID       *int64
	PromoteJobID       *int64
	PhaseStartedAt     *time.Time
	PhaseFinishedAt    *time.Time
	PhaseLastError     *string
	StartedAt          *time.Time
	FinishedAt         *time.Time
	LastError          *string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type TrustState struct {
	Pubkey       string
	Score        *float64
	Qualified    bool
	Tier         string
	HopDistance  *int
	HopBucket    string
	Rank         *int64
	ComputedAt   *time.Time
	GenerationID *int64
	IsSeed       bool
}

type WindowedCount struct {
	Last24h int64 `json:"24h"`
	Last7d  int64 `json:"7d"`
}
