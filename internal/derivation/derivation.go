package derivation

import (
	"strings"
	"time"
)

const (
	JobTypeDeriveEventRelationships = "derive_event_relationships"
	JobTypeUpdateReplaceableState   = "update_replaceable_state"
	JobTypeProjectProfilesLatest    = "project_profiles_latest"
	JobTypeProjectAuthorRecentEvent = "project_author_recent_event"
	JobTypeProjectReplyCounts       = "project_reply_counts"
	JobTypeProjectReactionCounts    = "project_reaction_counts"
	JobTypeProjectRepostCounts      = "project_repost_counts"
	JobTypeProjectReactionEvents    = "project_reaction_events"
	JobTypeProjectRepostEvents      = "project_repost_events"
	JobTypeProjectDeletionEvents    = "project_deletion_events"
	JobTypeProjectContactLists      = "project_contact_lists_latest"
	JobTypeProjectRelayLists        = "project_relay_lists_latest"
	JobTypeUpdateThreadProjection   = "update_thread_projection"
	JobTypeRepairUnresolvedRefs     = "repair_unresolved_references"
	JobTypeRebuildProjectionScope   = "rebuild_projection_scope"
)

const (
	DerivationEventRelationships = "event_relationships"
	DerivationReplaceableState   = "replaceable_state"
	DerivationProfilesLatest     = "profiles_latest"
	DerivationAuthorRecentEvents = "author_recent_events"
	DerivationReplyCounts        = "reply_counts"
	DerivationReactionCounts     = "reaction_counts"
	DerivationRepostCounts       = "repost_counts"
	DerivationReactionEvents     = "reaction_events"
	DerivationRepostEvents       = "repost_events"
	DerivationDeletionEvents     = "deletion_events"
	DerivationContactListsLatest = "contact_lists_latest"
	DerivationRelayListsLatest   = "relay_lists_latest"
	DerivationThreadProjection   = "thread_projection"
)

const (
	EventRelationshipsVersion = 1
	ReplaceableStateVersion   = 1
	ProfilesLatestVersion     = 1
	AuthorRecentEventsVersion = 1
	ReplyCountsVersion        = 1
	ReactionCountsVersion     = 1
	RepostCountsVersion       = 1
	ReactionEventsVersion     = 1
	RepostEventsVersion       = 1
	DeletionEventsVersion     = 1
	ContactListsLatestVersion = 1
	RelayListsLatestVersion   = 1
	ThreadProjectionVersion   = 1
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
