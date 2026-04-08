package query

import (
	"encoding/json"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

// ThreadRequest captures transport-agnostic inputs for assembling one thread view.
type ThreadRequest struct {
	EventID  string
	Limit    int
	MaxDepth int
	Cursor   *EventCursor
}

// ThreadWindowRequest captures transport-agnostic inputs for descending-window thread lookups.
type ThreadWindowRequest struct {
	EventID  string
	Limit    int
	MaxDepth int
	Cursor   *EventCursor
	Offset   int
}

type EventCursor struct {
	CreatedAt int64
	ID        string
}

type ThreadView struct {
	Event              json.RawMessage
	Ancestors          []json.RawMessage
	MissingAncestorIDs []string
	Replies            []json.RawMessage
	NextCursor         *EventCursor
	Consistency        string
}

type ThreadVelocityHints struct {
	Replies24h int64
	Replies7d  int64
}

type ThreadSummary struct {
	RootEventID      string
	ReplyCount       int64
	ParticipantCount int
	MaxDepth         int
	LastActivityAt   int64
	Velocity         ThreadVelocityHints
	Consistency      string
}

type ActionCounts struct {
	EventID       string `json:"event_id"`
	ReplyCount    int64  `json:"reply_count"`
	ReactionCount int64  `json:"reaction_count"`
	RepostCount   int64  `json:"repost_count"`
	Consistency   string `json:"consistency"`
}

type UserInfosResult struct {
	Profiles       []Profile
	MissingPubkeys []string
}

type SearchResult struct {
	Events   []json.RawMessage `json:"events"`
	Profiles []Profile         `json:"profiles"`
}

type SearchSuggestionsResult struct {
	Profiles []Profile           `json:"profiles"`
	Hashtags []HashtagSuggestion `json:"hashtags"`
}

type HashtagSuggestion struct {
	Hashtag       string `json:"hashtag"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

type NotesSearchParams struct {
	Query    string
	Limit    int
	Offset   int
	Sort     string
	Language string
	Window   *time.Duration
}

type ProfileSearchParams struct {
	Query  string
	Limit  int
	Offset int
	Sort   string
}

type TrustModeMetadata struct {
	TrustMode    string `json:"trust_mode"`
	TrustApplied bool   `json:"trust_applied"`
	ResultScope  string `json:"result_scope"`
}

type EventRepliesResult struct {
	EventID     string
	Replies     []json.RawMessage
	NextCursor  *EventCursor
	Consistency string
}

type EventAncestorsResult struct {
	EventID            string
	Ancestors          []json.RawMessage
	MissingAncestorIDs []string
	Consistency        string
}

type EventWithProvenanceResult struct {
	Event       json.RawMessage
	Relays      []model.EventRelay
	Consistency string
}

type EventWithProvenance struct {
	Event  json.RawMessage
	Relays []model.EventRelay
}

type EventCounts struct {
	EventID       string
	ReplyCount    int64
	ReactionCount int64
	RepostCount   int64
	Consistency   string
}

type EventSeenOnResult struct {
	EventID string
	SeenOn  []model.EventRelay
}

type Profile struct {
	Pubkey            string
	MetadataEventID   string
	MetadataCreatedAt int64
	ProfileJSON       json.RawMessage
}

type ProfilePublicStats struct {
	Pubkey           string
	FollowerCount    int64
	FollowingCount   int64
	NoteCount        int64
	ReplyCount       int64
	RecentActivityAt *int64
}

type ProfilePublicSummary struct {
	Profile Profile
	Stats   ProfilePublicStats
}

type AuthorAnalyticsMediaMix struct {
	TotalPosts           int64 `json:"total_posts"`
	WithImageCount       int64 `json:"with_image_count"`
	WithVideoCount       int64 `json:"with_video_count"`
	WithLinkCount        int64 `json:"with_link_count"`
	WithArticleCount     int64 `json:"with_article_count"`
	TextOnlyCount        int64 `json:"text_only_count"`
	TotalAttachmentCount int64 `json:"total_attachment_count"`
}

type QuoteRepostLinkedNoteSummary struct {
	EventID      string `json:"event_id"`
	AuthorPubkey string `json:"author_pubkey"`
	CreatedAt    int64  `json:"created_at"`
	Content      string `json:"content"`
}

type QuoteRepostActivity struct {
	EventID     string                       `json:"event_id"`
	ActorPubkey string                       `json:"actor_pubkey"`
	CreatedAt   int64                        `json:"created_at"`
	Action      string                       `json:"action"`
	Quote       string                       `json:"quote,omitempty"`
	LinkedNote  QuoteRepostLinkedNoteSummary `json:"linked_note"`
}

type AuthorAnalyticsQuoteRepostWindow struct {
	QuotesMade      int64 `json:"quotes_made"`
	RepostsMade     int64 `json:"reposts_made"`
	QuotesReceived  int64 `json:"quotes_received"`
	RepostsReceived int64 `json:"reposts_received"`
}

type AuthorAnalyticsWindowSummary struct {
	Window                   string                           `json:"window"`
	PostCount                int64                            `json:"post_count"`
	NoteCount                int64                            `json:"note_count"`
	ReplyCount               int64                            `json:"reply_count"`
	ActiveDays               int                              `json:"active_days"`
	EngagementReceived       int64                            `json:"engagement_received"`
	EngagementGiven          int64                            `json:"engagement_given"`
	CadencePostsPerDay       float64                          `json:"cadence_posts_per_day"`
	CadencePostsPerActiveDay float64                          `json:"cadence_posts_per_active_day"`
	RecentActivityAt         *int64                           `json:"recent_activity_at,omitempty"`
	MediaMix                 AuthorAnalyticsMediaMix          `json:"media_mix"`
	QuoteRepost              AuthorAnalyticsQuoteRepostWindow `json:"quote_repost"`
}

type AuthorAnalyticsSummary struct {
	Pubkey                    string                         `json:"pubkey"`
	Windows                   []AuthorAnalyticsWindowSummary `json:"windows"`
	RecentQuoteRepostActivity []QuoteRepostActivity          `json:"recent_quote_repost_activity,omitempty"`
	TopLanguages              []LanguageSummary              `json:"top_languages,omitempty"`
}

type AuthorTopicStat struct {
	Hashtag    string `json:"hashtag"`
	UsageCount int64  `json:"usage_count"`
	ActiveDays int    `json:"active_days"`
}

type AuthorHourlyEngagementWindow struct {
	HourOfDay          int   `json:"hour_of_day"`
	EngagementReceived int64 `json:"engagement_received"`
	ReplyReceived      int64 `json:"reply_received"`
	ReactionReceived   int64 `json:"reaction_received"`
	RepostReceived     int64 `json:"repost_received"`
	ZapReceived        int64 `json:"zap_received"`
}

type AuthorDailyEngagementWindow struct {
	DayOfWeek          int   `json:"day_of_week"`
	EngagementReceived int64 `json:"engagement_received"`
	ReplyReceived      int64 `json:"reply_received"`
	ReactionReceived   int64 `json:"reaction_received"`
	RepostReceived     int64 `json:"repost_received"`
	ZapReceived        int64 `json:"zap_received"`
}

type AuthorEngagementHeatmapBucket struct {
	DayOfWeek          int   `json:"day_of_week"`
	HourOfDay          int   `json:"hour_of_day"`
	EngagementReceived int64 `json:"engagement_received"`
	ReplyReceived      int64 `json:"reply_received"`
	ReactionReceived   int64 `json:"reaction_received"`
	RepostReceived     int64 `json:"repost_received"`
	ZapReceived        int64 `json:"zap_received"`
}

type AuthorActivityWindows struct {
	Pubkey   string                          `json:"pubkey"`
	Window   string                          `json:"window"`
	Timezone string                          `json:"timezone"`
	ByHour   []AuthorHourlyEngagementWindow  `json:"by_hour"`
	ByDay    []AuthorDailyEngagementWindow   `json:"by_day"`
	Heatmap  []AuthorEngagementHeatmapBucket `json:"heatmap"`
}

type AuthorHourlyPostingPattern struct {
	HourOfDay  int   `json:"hour_of_day"`
	PostCount  int64 `json:"post_count"`
	NoteCount  int64 `json:"note_count"`
	ReplyCount int64 `json:"reply_count"`
}

type AuthorDailyPostingPattern struct {
	DayOfWeek  int   `json:"day_of_week"`
	PostCount  int64 `json:"post_count"`
	NoteCount  int64 `json:"note_count"`
	ReplyCount int64 `json:"reply_count"`
}

type AuthorPostingHeatmapBucket struct {
	DayOfWeek  int   `json:"day_of_week"`
	HourOfDay  int   `json:"hour_of_day"`
	PostCount  int64 `json:"post_count"`
	NoteCount  int64 `json:"note_count"`
	ReplyCount int64 `json:"reply_count"`
}

type AuthorPostingPatterns struct {
	Pubkey   string                       `json:"pubkey"`
	Window   string                       `json:"window"`
	Timezone string                       `json:"timezone"`
	ByHour   []AuthorHourlyPostingPattern `json:"by_hour"`
	ByDay    []AuthorDailyPostingPattern  `json:"by_day"`
	Heatmap  []AuthorPostingHeatmapBucket `json:"heatmap"`
}

type AuthorTopNote struct {
	EventID            string  `json:"event_id"`
	CreatedAt          int64   `json:"created_at"`
	Content            string  `json:"content"`
	ReplyCount         int64   `json:"reply_count"`
	ReactionCount      int64   `json:"reaction_count"`
	RepostCount        int64   `json:"repost_count"`
	ZapCount           int64   `json:"zap_count"`
	ZapMSats           int64   `json:"zap_msats"`
	WeightedEngagement float64 `json:"weighted_engagement"`
	MediaSegment       string  `json:"media_segment"`
	PrimaryTopic       string  `json:"primary_topic,omitempty"`
}

type AuthorRecycleCandidateFilter struct {
	Window                   string  `json:"window"`
	MinAge                   string  `json:"min_age"`
	MinPerformancePercentile float64 `json:"min_performance_percentile"`
	IncludeReplies           bool    `json:"include_replies"`
	ExcludeRecentlyReposted  bool    `json:"exclude_recently_reposted"`
	RecentRepostWindow       string  `json:"recent_repost_window"`
}

type AuthorRecycleCandidate struct {
	EventID               string  `json:"event_id"`
	CreatedAt             int64   `json:"created_at"`
	Content               string  `json:"content"`
	ReplyCount            int64   `json:"reply_count"`
	ReactionCount         int64   `json:"reaction_count"`
	RepostCount           int64   `json:"repost_count"`
	ZapCount              int64   `json:"zap_count"`
	ZapMSats              int64   `json:"zap_msats"`
	WeightedEngagement    float64 `json:"weighted_engagement"`
	PerformancePercentile float64 `json:"performance_percentile"`
	IsReply               bool    `json:"is_reply"`
	HasRecentRepostMarker bool    `json:"has_recent_repost_marker"`
	MediaSegment          string  `json:"media_segment"`
	PrimaryTopic          string  `json:"primary_topic,omitempty"`
}

type AuthorPerformanceStats struct {
	Average float64 `json:"average"`
	Median  float64 `json:"median"`
}

type AuthorPerformanceTotals struct {
	ReplyCount    int64 `json:"reply_count"`
	ReactionCount int64 `json:"reaction_count"`
	RepostCount   int64 `json:"repost_count"`
	ZapCount      int64 `json:"zap_count"`
	ZapMSats      int64 `json:"zap_msats"`
}

type AuthorPerformanceComparison struct {
	Window                         string  `json:"window"`
	NoteCountDelta                 int64   `json:"note_count_delta"`
	TotalWeightedEngagementDelta   float64 `json:"total_weighted_engagement_delta"`
	AverageWeightedEngagementDelta float64 `json:"average_weighted_engagement_delta"`
	MedianWeightedEngagementDelta  float64 `json:"median_weighted_engagement_delta"`
}

type AuthorPerformanceSummary struct {
	Pubkey                  string                      `json:"pubkey"`
	Window                  string                      `json:"window"`
	NoteCount               int64                       `json:"note_count"`
	TotalWeightedEngagement float64                     `json:"total_weighted_engagement"`
	WeightedEngagement      AuthorPerformanceStats      `json:"weighted_engagement"`
	ReplyCount              AuthorPerformanceStats      `json:"reply_count"`
	ReactionCount           AuthorPerformanceStats      `json:"reaction_count"`
	RepostCount             AuthorPerformanceStats      `json:"repost_count"`
	ZapCount                AuthorPerformanceStats      `json:"zap_count"`
	Totals                  AuthorPerformanceTotals     `json:"totals"`
	MediaMix                AuthorAnalyticsMediaMix     `json:"media_mix"`
	TopTopics               []AuthorTopicStat           `json:"top_topics"`
	Comparison              AuthorPerformanceComparison `json:"comparison"`
}

type NoteEngagementCounts struct {
	ReplyCount    int64 `json:"reply_count"`
	ReactionCount int64 `json:"reaction_count"`
	RepostCount   int64 `json:"repost_count"`
	ZapCount      int64 `json:"zap_count"`
	ZapMSats      int64 `json:"zap_msats"`
}

type NoteMediaFlags struct {
	HasImage        bool `json:"has_image"`
	HasVideo        bool `json:"has_video"`
	HasLink         bool `json:"has_link"`
	HasArticle      bool `json:"has_article"`
	AttachmentCount int  `json:"attachment_count"`
}

type NoteConversationActivity struct {
	Replies24h int64 `json:"replies_24h"`
	Replies7d  int64 `json:"replies_7d"`
}

type NoteSummary struct {
	EventID              string
	Event                json.RawMessage
	Author               ProfilePublicSummary
	Counts               NoteEngagementCounts
	Media                NoteMediaFlags
	RootEventID          *string
	ParentEventID        *string
	MissingAncestorIDs   []string
	ReferenceEventID     *string
	ReferenceEvent       json.RawMessage
	QuoteRepostLinkage   *NoteQuoteRepostLinkageSummary
	ConversationActivity *NoteConversationActivity
	Consistency          string
}

type NoteQuoteRepostLinkageSummary struct {
	EventID        string                `json:"event_id"`
	QuoteCount     int64                 `json:"quote_count"`
	RepostCount    int64                 `json:"repost_count"`
	RecentActivity []QuoteRepostActivity `json:"recent_activity"`
}

type RelatedNote struct {
	EventID      string
	AuthorPubkey string
	CreatedAt    int64
	Content      string
	Event        json.RawMessage
	Counts       NoteEngagementCounts
	Reasons      []string
	Score        int64
}

type ContactList struct {
	Pubkey          string
	EventID         string
	CreatedAt       int64
	DerivationVer   int
	ContactsJSONRaw json.RawMessage
}

type RelayList struct {
	Pubkey        string
	EventID       string
	CreatedAt     int64
	DerivationVer int
	RelaysJSONRaw json.RawMessage
}

type NetworkStats struct {
	Events   int64 `json:"events"`
	Profiles int64 `json:"profiles"`
	Relays   int64 `json:"relays"`
}

type WindowedCount struct {
	Last24h int64 `json:"24h"`
	Last7d  int64 `json:"7d"`
}

type LanguageSummary struct {
	Language string `json:"language"`
	Count    int64  `json:"count"`
}

type TrendingHashtagWindows struct {
	Last24h []TrendingHashtag `json:"24h"`
	Last7d  []TrendingHashtag `json:"7d"`
}

type PublicDiscoveryNetworkStats struct {
	EventsIngested    int64                   `json:"events_ingested"`
	ProjectedProfiles int64                   `json:"projected_profiles"`
	Relays            int64                   `json:"relays"`
	ActiveAuthors     WindowedCount           `json:"active_authors"`
	NoteVolume        WindowedCount           `json:"note_volume"`
	TopHashtags       *TrendingHashtagWindows `json:"top_hashtags,omitempty"`
	TopLanguages24h   []LanguageSummary       `json:"top_languages_24h,omitempty"`
	TopLanguages7d    []LanguageSummary       `json:"top_languages_7d,omitempty"`
}

type CuratedRecommendedRead struct {
	EventID string `json:"event_id"`
	Title   string `json:"title"`
	URL     string `json:"url"`
	Rank    int    `json:"rank"`
}

type CuratedReadsTopic struct {
	Topic string `json:"topic"`
	Rank  int    `json:"rank"`
}

type CuratedFeaturedAuthor struct {
	Pubkey string `json:"pubkey"`
	Rank   int    `json:"rank"`
}

type TrendingHashtag struct {
	Hashtag       string `json:"hashtag"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
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

type RelatedHashtag struct {
	Hashtag             string `json:"hashtag"`
	CoOccurrenceCount   int64  `json:"co_occurrence_count"`
	CoOccurrenceAuthors int64  `json:"co_occurrence_authors"`
}

type EventDomainLink struct {
	EventID string `json:"event_id"`
	URL     string `json:"url"`
	Domain  string `json:"domain"`
}

type DomainStat struct {
	Domain        string `json:"domain"`
	LinkCount     int64  `json:"link_count"`
	NoteCount     int64  `json:"note_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

type DomainActivity struct {
	LinkCount     int64 `json:"link_count"`
	NoteCount     int64 `json:"note_count"`
	UniqueAuthors int64 `json:"unique_authors"`
}

type DomainActivityStats struct {
	Last24h DomainActivity `json:"24h"`
	Last7d  DomainActivity `json:"7d"`
	Last30d DomainActivity `json:"30d"`
	All     DomainActivity `json:"all"`
}

type DomainSummary struct {
	Domain        string              `json:"domain"`
	LatestEventAt *int64              `json:"latest_event_at,omitempty"`
	Activity      DomainActivityStats `json:"activity"`
	RecentNotes   []TrendingNote      `json:"recent_notes"`
	TopNotes      []TrendingNote      `json:"top_notes"`
}

type TrendingNote struct {
	EventID       string  `json:"event_id"`
	AuthorPubkey  string  `json:"author_pubkey"`
	CreatedAt     int64   `json:"created_at"`
	Content       string  `json:"content"`
	Language      string  `json:"language,omitempty"`
	ReplyCount    int64   `json:"reply_count"`
	RepostCount   int64   `json:"repost_count"`
	ReactionCount int64   `json:"reaction_count"`
	ZapCount      int64   `json:"zap_count"`
	ZapMSats      int64   `json:"zap_msats"`
	Score         float64 `json:"score"`
}

type HotConversation struct {
	RootEventID      string  `json:"root_event_id"`
	AuthorPubkey     string  `json:"author_pubkey"`
	CreatedAt        int64   `json:"created_at"`
	Content          string  `json:"content"`
	ReplyCount       int64   `json:"reply_count"`
	ParticipantCount int     `json:"participant_count"`
	LastActivityAt   int64   `json:"last_activity_at"`
	Replies24h       int64   `json:"replies_24h"`
	Replies7d        int64   `json:"replies_7d"`
	VelocityScore    float64 `json:"velocity_score"`
	Consistency      string  `json:"consistency"`
}

type TrendingProfile struct {
	Pubkey                   string  `json:"pubkey"`
	Score                    float64 `json:"score"`
	RecentPostCount          int64   `json:"recent_post_count"`
	RecentReplyCount         int64   `json:"recent_reply_count"`
	RecentEngagementReceived int64   `json:"recent_engagement_received"`
	RecentZapVolumeMSats     int64   `json:"recent_zap_volume_msats"`
	RecentActiveDays         int     `json:"recent_active_days"`
	RecentActivityAt         *int64  `json:"recent_activity_at,omitempty"`
}

type TrustScore struct {
	Pubkey         string
	Score          float64
	Rank           int64
	RunID          int64
	DerivationName string
	TargetVersion  int
	ComputedAt     time.Time
}

type TrustQualificationPolicy struct {
	MaxHops      int
	MinimumScore float64
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
