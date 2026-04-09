package store

type NetworkStats struct {
	Events   int64 `json:"events"`
	Profiles int64 `json:"profiles"`
	Relays   int64 `json:"relays"`
}

type WindowedCount struct {
	Last24h int64 `json:"24h"`
	Last7d  int64 `json:"7d"`
}

type RelayUsageSummary struct {
	RelayURL      string `json:"relay_url"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

type RelaySummaryStats struct {
	Total         int64         `json:"total"`
	Active24h     int64         `json:"active_24h"`
	Active7d      int64         `json:"active_7d"`
	EventVolume   WindowedCount `json:"event_volume"`
	UniqueAuthors WindowedCount `json:"unique_authors"`
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
	RelaySummary      RelaySummaryStats       `json:"relay_summary"`
	TopRelays         []RelayUsageSummary     `json:"top_relays,omitempty"`
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
	Pubkey                   string  `json:"pubkey"`
	Score                    float64 `json:"score"`
	RecentPostCount          int64   `json:"recent_post_count"`
	RecentReplyCount         int64   `json:"recent_reply_count"`
	RecentEngagementReceived int64   `json:"recent_engagement_received"`
	RecentZapVolumeMSats     int64   `json:"recent_zap_volume_msats"`
	RecentActiveDays         int     `json:"recent_active_days"`
	RecentActivityAt         *int64  `json:"recent_activity_at,omitempty"`
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

type TrustQualifiedTrendingNote struct {
	Note    TrendingNote
	Trusted bool
}

type TrustQualifiedTrendingProfile struct {
	Profile TrendingProfile
	Trusted bool
}
