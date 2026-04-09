package query

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
	RelayFootprint            *AuthorRelayFootprintSummary   `json:"relay_footprint,omitempty"`
	RecentQuoteRepostActivity []QuoteRepostActivity          `json:"recent_quote_repost_activity,omitempty"`
	TopLanguages              []LanguageSummary              `json:"top_languages,omitempty"`
}

type AuthorRelayFootprintSummary struct {
	RelayCount       int64               `json:"relay_count"`
	SeenOnEventCount int64               `json:"seen_on_event_count"`
	TopRelays        []RelayUsageSummary `json:"top_relays,omitempty"`
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

type GroupedNoteAnalyticsRequest struct {
	Pubkey        string
	WindowDays    int
	GroupKind     string
	GroupKey      string
	MetadataTag   string
	TopNotesLimit int
	TopicsLimit   int
}

type GroupedEngagementTotals struct {
	ReplyCount    int64 `json:"reply_count"`
	ReactionCount int64 `json:"reaction_count"`
	RepostCount   int64 `json:"repost_count"`
	ZapCount      int64 `json:"zap_count"`
	ZapMSats      int64 `json:"zap_msats"`
}

type GroupedMediaSummary struct {
	TotalPosts           int64 `json:"total_posts"`
	WithImageCount       int64 `json:"with_image_count"`
	WithVideoCount       int64 `json:"with_video_count"`
	WithLinkCount        int64 `json:"with_link_count"`
	WithArticleCount     int64 `json:"with_article_count"`
	TextOnlyCount        int64 `json:"text_only_count"`
	TotalAttachmentCount int64 `json:"total_attachment_count"`
}

type GroupedTopNote struct {
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

type GroupedTopicSummary struct {
	Hashtag    string `json:"hashtag"`
	UsageCount int64  `json:"usage_count"`
	ActiveDays int    `json:"active_days"`
}

type GroupedNoteAnalyticsSummary struct {
	Pubkey      string                  `json:"pubkey"`
	Window      string                  `json:"window"`
	GroupKind   string                  `json:"group_kind"`
	GroupKey    string                  `json:"group_key"`
	MetadataTag string                  `json:"metadata_tag,omitempty"`
	NoteCount   int64                   `json:"note_count"`
	Engagement  GroupedEngagementTotals `json:"engagement"`
	Media       GroupedMediaSummary     `json:"media"`
	TopNotes    []GroupedTopNote        `json:"top_notes"`
	TopTopics   []GroupedTopicSummary   `json:"top_topics"`
}
