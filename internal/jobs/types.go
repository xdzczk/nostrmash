package jobs

// Job type vocabulary for queue payload dispatch.
const (
	JobTypeDeriveEventBundle        = "derive_event_bundle"
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
	JobTypeProjectDMUnreadCounts    = "project_dm_unread_counts"
	JobTypeProjectZapReceipts       = "project_zap_receipts"
	JobTypeUpdateThreadProjection   = "update_thread_projection"
	JobTypeRepairUnresolvedRefs     = "repair_unresolved_references"
	JobTypeRebuildProjectionScope   = "rebuild_projection_scope"
)

// EventJobPayload is the common event-scoped job payload shape.
type EventJobPayload struct {
	EventID string `json:"event_id"`
}
