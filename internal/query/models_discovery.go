package query

import (
	"encoding/json"
	"time"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// Profile is re-exported from the neutral readmodel package so lower-level
// adapters (e.g. meili) can return it without importing query.
type Profile = readmodel.Profile

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
	T time.Time
	V int64
}

type DiscoveryStatsSeries struct {
	Metric     string
	Window     string
	ComputedAt *time.Time
	Points     []DiscoveryStatsSeriesPoint
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

type DiscoveryEvidence struct {
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
	Unit   string  `json:"unit,omitempty"`
}

type DiscoveryReason struct {
	Code     string              `json:"code"`
	Evidence []DiscoveryEvidence `json:"evidence,omitempty"`
}

type DiscoveryItemRanking struct {
	Rank          int               `json:"rank"`
	Score         float64           `json:"score"`
	PreviousRank  *int              `json:"previous_rank,omitempty"`
	RankDelta     *int              `json:"rank_delta,omitempty"`
	Reasons       []DiscoveryReason `json:"reasons,omitempty"`
	SourceBreadth *int64            `json:"source_breadth,omitempty"`
	Confidence    string            `json:"confidence,omitempty"`
}

type DiscoveryListMeta struct {
	Window         string     `json:"window"`
	ComputedAt     *time.Time `json:"computed_at,omitempty"`
	RankingVersion string     `json:"ranking_version"`
	Confidence     string     `json:"confidence"`
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
	RecentNewFollowers       int64   `json:"recent_new_followers"`
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
