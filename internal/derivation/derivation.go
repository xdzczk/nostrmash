package derivation

import (
	"time"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/nostr"
)

const (
	JobTypeDeriveEventBundle        = jobs.JobTypeDeriveEventBundle
	JobTypeDeriveEventRelationships = jobs.JobTypeDeriveEventRelationships
	JobTypeUpdateReplaceableState   = jobs.JobTypeUpdateReplaceableState
	JobTypeProjectProfilesLatest    = jobs.JobTypeProjectProfilesLatest
	JobTypeProjectAuthorRecentEvent = jobs.JobTypeProjectAuthorRecentEvent
	JobTypeProjectReplyCounts       = jobs.JobTypeProjectReplyCounts
	JobTypeProjectReactionCounts    = jobs.JobTypeProjectReactionCounts
	JobTypeProjectRepostCounts      = jobs.JobTypeProjectRepostCounts
	JobTypeProjectReactionEvents    = jobs.JobTypeProjectReactionEvents
	JobTypeProjectRepostEvents      = jobs.JobTypeProjectRepostEvents
	JobTypeProjectDeletionEvents    = jobs.JobTypeProjectDeletionEvents
	JobTypeProjectContactLists      = jobs.JobTypeProjectContactLists
	JobTypeProjectRelayLists        = jobs.JobTypeProjectRelayLists
	JobTypeProjectDMUnreadCounts    = jobs.JobTypeProjectDMUnreadCounts
	JobTypeProjectZapReceipts       = jobs.JobTypeProjectZapReceipts
	JobTypeUpdateThreadProjection   = jobs.JobTypeUpdateThreadProjection
	JobTypeRepairUnresolvedRefs     = jobs.JobTypeRepairUnresolvedRefs
	JobTypeRebuildProjectionScope   = jobs.JobTypeRebuildProjectionScope
)

const (
	DerivationEventRelationships          = "event_relationships"
	DerivationReplaceableState            = "replaceable_state"
	DerivationProfilesLatest              = "profiles_latest"
	DerivationAuthorRecentEvents          = "author_recent_events"
	DerivationReplyCounts                 = "reply_counts"
	DerivationReactionCounts              = "reaction_counts"
	DerivationRepostCounts                = "repost_counts"
	DerivationReactionEvents              = "reaction_events"
	DerivationRepostEvents                = "repost_events"
	DerivationDeletionEvents              = "deletion_events"
	DerivationContactListsLatest          = "contact_lists_latest"
	DerivationFollowerEdges               = "follower_edges"
	DerivationRelayListsLatest            = "relay_lists_latest"
	DerivationEventHashtags               = "event_hashtags"
	DerivationEventURLs                   = "event_urls"
	DerivationNoteDiscoveryStats          = "note_discovery_stats"
	DerivationProfileDiscoveryStats       = "profile_discovery_stats"
	DerivationTrustedNoteDiscovery        = "trusted_note_discovery_candidates"
	DerivationTrustedProfileDiscovery     = "trusted_profile_discovery_candidates"
	DerivationDMUnreadCounts              = "dm_unread_counts"
	DerivationZapReceipts                 = "zap_receipts"
	DerivationProfilePublicStats          = "profile_public_stats"
	DerivationAuthorActivityDaily         = "author_activity_daily"
	DerivationAuthorEngagementStats       = "author_engagement_stats"
	DerivationAuthorTopicStats            = "author_topic_stats"
	DerivationAuthorMediaMixStats         = "author_media_mix_stats"
	DerivationAuthorActivityWindows       = "author_activity_windows"
	DerivationAuthorPostingPatterns       = "author_posting_patterns"
	DerivationThreadProjection            = "thread_projection"
	DerivationThreadSummary               = "thread_summary"
	DerivationTrustScoresGlobal           = "trust_scores_global"
	DerivationTrustPubkeysLatest          = "trust_pubkeys_latest"
	DerivationTrustNeighborhoodMembers    = "trust_neighborhood_members"
	DerivationTrustInteractionEdgeWeights = "trust_interaction_edge_weights"
)

const (
	EventRelationshipsVersion = 1
	ReplaceableStateVersion   = 1
	ProfilesLatestVersion     = 1
	AuthorRecentEventsVersion = 1
	ReplyCountsVersion        = 2
	ReactionCountsVersion     = 1
	RepostCountsVersion       = 1
	ReactionEventsVersion     = 1
	RepostEventsVersion       = 1
	DeletionEventsVersion     = 1
	ContactListsLatestVersion = 1
	FollowerEdgesVersion      = 1
	RelayListsLatestVersion   = 1
	EventHashtagsVersion      = 1
	EventURLsVersion          = 2
	// v5: trending score inputs count each engager pubkey once per signal and
	// exclude the note author's self-engagement (optionally trust-weighted).
	NoteDiscoveryStatsVersion = 5
	// v2: score inputs exclude self-engagement in the legacy full-scan
	// loader and, with TRUST_DISCOVERY_ENGAGEMENT_WEIGHTING on, swap to
	// deduplicated trust-weighted engagement / zap / new-follower votes.
	ProfileDiscoveryStatsVersion   = 2
	TrustedNoteDiscoveryVersion    = 1
	TrustedProfileDiscoveryVersion = 1
	DMUnreadCountsVersion          = 1
	ZapReceiptsVersion             = 1
	ProfilePublicStatsVersion      = 1
	AuthorActivityDailyVersion     = 1
	AuthorEngagementStatsVersion   = 1
	AuthorTopicStatsVersion        = 1
	AuthorMediaMixStatsVersion     = 2
	AuthorActivityWindowsVersion   = 1
	AuthorPostingPatternsVersion   = 1
	ThreadProjectionVersion        = 1
	// v2: adds reply_weight_24h/7d — unique repliers excluding the root
	// author (optionally trust-weighted) — as the hot-conversation velocity
	// inputs, replacing raw reply counters a single account could inflate.
	ThreadSummaryVersion               = 2
	TrustScoresGlobalVersion           = 3
	TrustPubkeysLatestVersion          = 1
	TrustNeighborhoodMembersVersion    = 1
	TrustInteractionEdgeWeightsVersion = 1
)

const (
	RebuildScopeFull      = "full"
	RebuildScopeEvent     = "event"
	RebuildScopePubkey    = "pubkey"
	RebuildScopeTimeRange = "time_range"
)

const (
	RebuildStatusPending   = "pending"
	RebuildStatusRunning   = "running"
	RebuildStatusSucceeded = "succeeded"
	RebuildStatusFailed    = "failed"
)

type ProjectionRebuildScope struct {
	Type           string
	EventID        string
	Pubkey         string
	StartCreatedAt *int64
	EndCreatedAt   *int64
}

type ProjectionRebuildRun struct {
	ID             int64
	DerivationName string
	TargetVersion  int
	Scope          ProjectionRebuildScope
	Status         string
	JobID          *int64
	Attempts       int
	StartedAt      *time.Time
	FinishedAt     *time.Time
	LastError      *string
}

type TriggerProjectionRebuildParams struct {
	DerivationName string
	Scope          ProjectionRebuildScope
	TargetVersion  int
}

type RebuildProjectionScopeJobPayload struct {
	RunID int64 `json:"run_id"`
}

// ParseRelationMarker normalizes root/reply/mention markers.
func ParseRelationMarker(value string) (string, bool) {
	return nostr.ParseRelationMarker(value)
}
