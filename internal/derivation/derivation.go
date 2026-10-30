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
	DerivationCommentEdges                = "comment_edges"
	DerivationReportTargets               = "report_targets"
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
	DerivationPrimalNotifications         = "primal_notifications"
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
	CommentEdgesVersion       = 1
	ReportTargetsVersion      = 1
	ReactionEventsVersion     = 1
	RepostEventsVersion       = 1
	DeletionEventsVersion     = 1
	ContactListsLatestVersion = 1
	FollowerEdgesVersion      = 1
	RelayListsLatestVersion   = 1
	// v2: all-numeric hashtags ("1" parsed from "#1 app") are not topics.
	EventHashtagsVersion = 2
	EventURLsVersion     = 2
	// v5: trending score inputs count each engager pubkey once per signal and
	// exclude the note author's self-engagement (optionally trust-weighted).
	// v6: repeated greetings and repeated short phrases are discounted in
	// the reply weight. A single "GM" still counts; a pile of them does not
	// count as one vote each.
	// v7: reactions and reposts beyond the budget replies and zaps unlock
	// count at a fraction. A boost farm (many reactions and reposts, ~0
	// replies, one zap) can no longer outrank a note people replied to.
	// v8: note language is classified from cleaned text, an explicit l/lang
	// tag, or the parent thread / author's content language. A settled label
	// is kept across later engagement refreshes. Short notes and text the
	// model refuses stay unmarked as separate chart buckets.
	// v9: score_30d ranks the 30-day window with the same formula as 24h
	// and 7d. Rows written before a refresh keep the column at 0.
	// v10: 7d recency keeps 85% at the end of the week, and 30d drops the
	// recency penalty. 24h is unchanged. score_7d and score_30d stay on
	// the previous curve until the next derive or a time-range rebuild.
	// v11: greeting variants and one-line wishes share one reply bucket.
	// That bucket still adds its discounted weight to the score, and it does
	// not unlock reactions or reposts. A single greeting still counts in full.
	// Rows keep the previous scores until the next derive or a time-range rebuild.
	NoteDiscoveryStatsVersion = 11
	// v3: persist the (optionally trust-weighted) engagement and new-follower
	// values the scores actually used, so ranking reasons/confidence can
	// cite the same inputs instead of raw display counters. Also requires
	// nonzero engagement for trending eligibility and steepens the rising
	// audience penalty above ~500 followers.
	// v4: rising momentum requires credited (above-noise) follower growth or
	// engagement -- trending alone no longer qualifies. Engagement gets a
	// 1-interaction noise floor, sample-size shrinkage, an audience prior in
	// the relative-engagement ratio, and a consistency multiplier that
	// discounts single-day bursts instead of dividing by sqrt(activeDays).
	// v5: follower growth below 10 credited followers scores nothing, and
	// credited growth ranks linearly under the 500-follower cap so a +3
	// account cannot outrank a larger absolute jump.
	// v6: primary_language_24h/7d record the author's dominant content
	// language in each window so profile rankings can filter on it. Rows
	// written before a refresh leave the columns null and drop out of a
	// language view until the hot path or the language backfill fills them.
	// v7: score_30d, rising_score_30d, and the matching scored-input and
	// language columns cover the 30-day window. Rows written before a
	// refresh keep the scores at 0 and the language null.
	// v8: trending engagement is distinct (signal, engager) votes on root
	// notes published in the window, and rising engagement is distinct
	// people in the window at any note age. scored_rising_engagement_*
	// holds the rising numerator. Rows written before a refresh keep the
	// previous scores until the profile-stats sweeper rewrites them.
	// v9: 7d trending keeps 85% at the end of the week, and 30d trending
	// drops the recency penalty. 24h is unchanged. The profile-stats
	// sweeper rewrites rows still inside the serving window.
	ProfileDiscoveryStatsVersion   = 9
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
	// v3: the same greeting and short-phrase discount as note discovery.
	// v4: authors_24h/7d count the root plus everyone who replied in that
	// window, so conversation rank can follow the window's headcount instead
	// of the lifetime participant total.
	// v5: replies_30d, reply_weight_30d, and authors_30d cover the 30-day
	// window. Readers use authors_30d only at this version; older rows fall
	// back to participant_count.
	// v6: greeting variants and one-line wishes share the reply-weight bucket
	// with identical "GM"s. Rows keep the previous weights until the thread
	// summary sweeper rewrites them.
	ThreadSummaryVersion               = 6
	TrustScoresGlobalVersion           = 3
	TrustPubkeysLatestVersion          = 1
	TrustNeighborhoodMembersVersion    = 1
	TrustInteractionEdgeWeightsVersion = 1
	PrimalNotificationsVersion         = 1
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
