package derivation

import (
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/jobs"
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
	DerivationEventRelationships      = "event_relationships"
	DerivationReplaceableState        = "replaceable_state"
	DerivationProfilesLatest          = "profiles_latest"
	DerivationAuthorRecentEvents      = "author_recent_events"
	DerivationReplyCounts             = "reply_counts"
	DerivationReactionCounts          = "reaction_counts"
	DerivationRepostCounts            = "repost_counts"
	DerivationReactionEvents          = "reaction_events"
	DerivationRepostEvents            = "repost_events"
	DerivationDeletionEvents          = "deletion_events"
	DerivationContactListsLatest      = "contact_lists_latest"
	DerivationFollowerEdges           = "follower_edges"
	DerivationRelayListsLatest        = "relay_lists_latest"
	DerivationEventHashtags           = "event_hashtags"
	DerivationNoteDiscoveryStats      = "note_discovery_stats"
	DerivationProfileDiscoveryStats   = "profile_discovery_stats"
	DerivationTrustedNoteDiscovery    = "trusted_note_discovery_candidates"
	DerivationTrustedProfileDiscovery = "trusted_profile_discovery_candidates"
	DerivationDMUnreadCounts          = "dm_unread_counts"
	DerivationZapReceipts             = "zap_receipts"
	DerivationProfilePublicStats      = "profile_public_stats"
	DerivationThreadProjection        = "thread_projection"
	DerivationTrustScoresGlobal       = "trust_scores_global"
)

const (
	EventRelationshipsVersion      = 1
	ReplaceableStateVersion        = 1
	ProfilesLatestVersion          = 1
	AuthorRecentEventsVersion      = 1
	ReplyCountsVersion             = 1
	ReactionCountsVersion          = 1
	RepostCountsVersion            = 1
	ReactionEventsVersion          = 1
	RepostEventsVersion            = 1
	DeletionEventsVersion          = 1
	ContactListsLatestVersion      = 1
	FollowerEdgesVersion           = 1
	RelayListsLatestVersion        = 1
	EventHashtagsVersion           = 1
	NoteDiscoveryStatsVersion      = 1
	ProfileDiscoveryStatsVersion   = 1
	TrustedNoteDiscoveryVersion    = 1
	TrustedProfileDiscoveryVersion = 1
	DMUnreadCountsVersion          = 1
	ZapReceiptsVersion             = 1
	ProfilePublicStatsVersion      = 1
	ThreadProjectionVersion        = 1
	TrustScoresGlobalVersion       = 3
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
	marker := strings.ToLower(strings.TrimSpace(value))
	switch marker {
	case "root", "reply", "mention":
		return marker, true
	default:
		return "", false
	}
}
