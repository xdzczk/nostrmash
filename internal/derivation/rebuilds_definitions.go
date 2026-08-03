package derivation

import (
	"context"
	"fmt"
	"strings"
)

func (h *Handlers) projectionDefinition(derivationName string) (projectionDefinition, error) {
	normalized := strings.TrimSpace(derivationName)
	switch normalized {
	case DerivationProfilesLatest:
		return projectionDefinition{
			name:           DerivationProfilesLatest,
			compiled:       ProfilesLatestVersion,
			description:    "Project latest effective replaceable metadata (kind 0) per pubkey",
			rebuildProject: h.projectProfilesLatestWithVersion,
		}, nil
	case DerivationAuthorRecentEvents:
		return projectionDefinition{
			name:           DerivationAuthorRecentEvents,
			compiled:       AuthorRecentEventsVersion,
			description:    "Project author recent events ordered by created_at desc, id desc",
			rebuildProject: h.projectAuthorRecentEventWithVersion,
		}, nil
	case DerivationReplyCounts:
		return projectionDefinition{
			name:           DerivationReplyCounts,
			compiled:       ReplyCountsVersion,
			description:    "Project eventually-consistent reply counts from thread-parent references (reply, else root)",
			rebuildProject: h.projectReplyCountsWithVersion,
		}, nil
	case DerivationReactionCounts:
		return projectionDefinition{
			name:           DerivationReactionCounts,
			compiled:       ReactionCountsVersion,
			description:    "Project eventually-consistent reaction counts from kind=7 e references",
			rebuildProject: h.projectReactionCountsWithVersion,
		}, nil
	case DerivationRepostCounts:
		return projectionDefinition{
			name:           DerivationRepostCounts,
			compiled:       RepostCountsVersion,
			description:    "Project eventually-consistent repost counts from kind=6 e references",
			rebuildProject: h.projectRepostCountsWithVersion,
		}, nil
	case DerivationReactionEvents:
		return projectionDefinition{
			name:           DerivationReactionEvents,
			compiled:       ReactionEventsVersion,
			description:    "Project reaction_events records from kind=7 references",
			rebuildProject: h.projectReactionEventsWithVersion,
		}, nil
	case DerivationRepostEvents:
		return projectionDefinition{
			name:           DerivationRepostEvents,
			compiled:       RepostEventsVersion,
			description:    "Project repost_events records from kind=6 references",
			rebuildProject: h.projectRepostEventsWithVersion,
		}, nil
	case DerivationDeletionEvents:
		return projectionDefinition{
			name:           DerivationDeletionEvents,
			compiled:       DeletionEventsVersion,
			description:    "Project deletion_events records from kind=5 references",
			rebuildProject: h.projectDeletionEventsWithVersion,
		}, nil
	case DerivationContactListsLatest:
		return projectionDefinition{
			name:        DerivationContactListsLatest,
			compiled:    ContactListsLatestVersion,
			description: "Project contact_lists_latest from kind=3 replaceables",
			rebuildProject: func(ctx context.Context, eventID string, version *int) error {
				return h.projectContactListsLatestWithVersion(ctx, eventID, version)
			},
		}, nil
	case DerivationFollowerEdges:
		return projectionDefinition{
			name:        DerivationFollowerEdges,
			compiled:    FollowerEdgesVersion,
			description: "Project follower edges from latest contact_lists_latest state",
			rebuildProject: func(ctx context.Context, eventID string, version *int) error {
				return h.projectContactListsLatestWithVersion(ctx, eventID, version)
			},
		}, nil
	case DerivationRelayListsLatest:
		return projectionDefinition{
			name:        DerivationRelayListsLatest,
			compiled:    RelayListsLatestVersion,
			description: "Project relay_lists_latest from kind=10002 replaceables",
			rebuildProject: func(ctx context.Context, eventID string, version *int) error {
				return h.projectRelayListsLatestWithVersion(ctx, eventID, version)
			},
		}, nil
	case DerivationEventHashtags:
		return projectionDefinition{
			name:           DerivationEventHashtags,
			compiled:       EventHashtagsVersion,
			description:    "Project normalized hashtags from note-like events",
			rebuildProject: h.projectEventHashtagsWithVersion,
		}, nil
	case DerivationEventURLs:
		return projectionDefinition{
			name:           DerivationEventURLs,
			compiled:       EventURLsVersion,
			description:    "Project normalized URLs with observed and canonical domains from note-like events",
			rebuildProject: h.projectEventURLsWithVersion,
		}, nil
	case DerivationNoteDiscoveryStats:
		return projectionDefinition{
			name:           DerivationNoteDiscoveryStats,
			compiled:       NoteDiscoveryStatsVersion,
			description:    "Project per-note discovery counters and rolling scores",
			rebuildProject: h.projectNoteDiscoveryStatsWithVersion,
		}, nil
	case DerivationProfileDiscoveryStats:
		return projectionDefinition{
			name:           DerivationProfileDiscoveryStats,
			compiled:       ProfileDiscoveryStatsVersion,
			description:    "Project per-profile discovery counters and rolling scores",
			rebuildProject: h.projectProfileDiscoveryStatsWithVersion,
		}, nil
	case DerivationTrustedNoteDiscovery:
		return projectionDefinition{
			name:        DerivationTrustedNoteDiscovery,
			compiled:    TrustedNoteDiscoveryVersion,
			description: "Project trust-qualified discovery metadata for note candidates",
			rebuildFull: h.rebuildTrustedNoteDiscoveryWithVersion,
		}, nil
	case DerivationTrustedProfileDiscovery:
		return projectionDefinition{
			name:        DerivationTrustedProfileDiscovery,
			compiled:    TrustedProfileDiscoveryVersion,
			description: "Project trust-qualified discovery metadata for profile candidates",
			rebuildFull: h.rebuildTrustedProfileDiscoveryWithVersion,
		}, nil
	case DerivationThreadProjection:
		return projectionDefinition{
			name:           DerivationThreadProjection,
			compiled:       ThreadProjectionVersion,
			description:    "Project reply parent/root edges with unresolved reference tracking",
			rebuildProject: h.updateThreadProjectionWithVersion,
		}, nil
	case DerivationThreadSummary:
		return projectionDefinition{
			name:           DerivationThreadSummary,
			compiled:       ThreadSummaryVersion,
			description:    "Project root-level thread summary counters and velocity hints",
			rebuildProject: h.updateThreadSummaryWithVersion,
		}, nil
	case DerivationDMUnreadCounts:
		return projectionDefinition{
			name:           DerivationDMUnreadCounts,
			compiled:       DMUnreadCountsVersion,
			description:    "Track unread DM counters by receiver and sender",
			rebuildProject: h.projectDMUnreadCountsWithVersion,
		}, nil
	case DerivationZapReceipts:
		return projectionDefinition{
			name:           DerivationZapReceipts,
			compiled:       ZapReceiptsVersion,
			description:    "Project zap receipts by sender, receiver, target event, and sats",
			rebuildProject: h.projectZapReceiptsWithVersion,
		}, nil
	case DerivationProfilePublicStats:
		return projectionDefinition{
			name:           DerivationProfilePublicStats,
			compiled:       ProfilePublicStatsVersion,
			description:    "Project public profile counters and recent activity",
			rebuildProject: h.projectProfilePublicStatsWithVersion,
		}, nil
	case DerivationAuthorActivityDaily:
		return projectionDefinition{
			name:           DerivationAuthorActivityDaily,
			compiled:       AuthorActivityDailyVersion,
			description:    "Project per-author daily post cadence and engagement aggregates",
			rebuildProject: h.projectAuthorAnalyticsWithVersion,
			rebuildFull:    h.rebuildAuthorAnalyticsWithVersion,
		}, nil
	case DerivationAuthorEngagementStats:
		return projectionDefinition{
			name:           DerivationAuthorEngagementStats,
			compiled:       AuthorEngagementStatsVersion,
			description:    "Project windowed per-author engagement and cadence summaries",
			rebuildProject: h.projectAuthorAnalyticsWithVersion,
			rebuildFull:    h.rebuildAuthorAnalyticsWithVersion,
		}, nil
	case DerivationAuthorTopicStats:
		return projectionDefinition{
			name:           DerivationAuthorTopicStats,
			compiled:       AuthorTopicStatsVersion,
			description:    "Project windowed per-author hashtag usage summaries",
			rebuildProject: h.projectAuthorAnalyticsWithVersion,
			rebuildFull:    h.rebuildAuthorAnalyticsWithVersion,
		}, nil
	case DerivationAuthorMediaMixStats:
		return projectionDefinition{
			name:           DerivationAuthorMediaMixStats,
			compiled:       AuthorMediaMixStatsVersion,
			description:    "Project windowed per-author media mix summaries",
			rebuildProject: h.projectAuthorAnalyticsWithVersion,
			rebuildFull:    h.rebuildAuthorAnalyticsWithVersion,
		}, nil
	case DerivationAuthorActivityWindows:
		return projectionDefinition{
			name:           DerivationAuthorActivityWindows,
			compiled:       AuthorActivityWindowsVersion,
			description:    "Project windowed per-author engagement timing buckets by UTC day/hour",
			rebuildProject: h.projectAuthorAnalyticsWithVersion,
			rebuildFull:    h.rebuildAuthorAnalyticsWithVersion,
		}, nil
	case DerivationAuthorPostingPatterns:
		return projectionDefinition{
			name:           DerivationAuthorPostingPatterns,
			compiled:       AuthorPostingPatternsVersion,
			description:    "Project windowed per-author posting cadence buckets by UTC day/hour",
			rebuildProject: h.projectAuthorAnalyticsWithVersion,
			rebuildFull:    h.rebuildAuthorAnalyticsWithVersion,
		}, nil
	default:
		return projectionDefinition{}, fmt.Errorf("projection rebuild is not supported for derivation %q", normalized)
	}
}
