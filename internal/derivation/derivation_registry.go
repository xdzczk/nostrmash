package derivation

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisteredDerivation captures the deploy-time metadata for a single
// projection derivation. Entries here are the source of truth used by
// EnsureRegisteredDerivations at process startup; the per-job hot path then
// only needs to SELECT the active version, never to write.
type RegisteredDerivation struct {
	Name        string
	Version     int
	Description string
}

// RegisteredDerivations lists every derivation that worker/ingestor may run. Adding
// a new derivation requires appending it here so that EnsureRegisteredDerivations
// can pre-create the rows that resolveDerivationWriteVersion expects to read.
var RegisteredDerivations = []RegisteredDerivation{
	{Name: DerivationEventRelationships, Version: EventRelationshipsVersion, Description: "Derive e references with v1 root/reply/mention semantics (p references read from event_tags directly)"},
	{Name: DerivationReplaceableState, Version: ReplaceableStateVersion, Description: "Track deterministic latest-wins replaceable event state"},
	{Name: DerivationProfilesLatest, Version: ProfilesLatestVersion, Description: "Project latest effective replaceable metadata (kind 0) per pubkey"},
	{Name: DerivationAuthorRecentEvents, Version: AuthorRecentEventsVersion, Description: "Project author recent events ordered by created_at desc, id desc"},
	{Name: DerivationReplyCounts, Version: ReplyCountsVersion, Description: "Project eventually-consistent reply counts from relation=reply references"},
	{Name: DerivationReactionCounts, Version: ReactionCountsVersion, Description: "Project eventually-consistent reaction counts from kind=7 e references"},
	{Name: DerivationRepostCounts, Version: RepostCountsVersion, Description: "Project eventually-consistent repost counts from kind=6 e references"},
	{Name: DerivationReactionEvents, Version: ReactionEventsVersion, Description: "Project reaction_events records from kind=7 references"},
	{Name: DerivationRepostEvents, Version: RepostEventsVersion, Description: "Project repost_events records from kind=6 references"},
	{Name: DerivationDeletionEvents, Version: DeletionEventsVersion, Description: "Project deletion_events records from kind=5 references"},
	{Name: DerivationContactListsLatest, Version: ContactListsLatestVersion, Description: "Project contact_lists_latest from kind=3 replaceables"},
	{Name: DerivationFollowerEdges, Version: FollowerEdgesVersion, Description: "Project follower edges from latest contact_lists_latest state"},
	{Name: DerivationRelayListsLatest, Version: RelayListsLatestVersion, Description: "Project relay_lists_latest from kind=10002 replaceables"},
	{Name: DerivationEventHashtags, Version: EventHashtagsVersion, Description: "Project normalized hashtags from note-like events"},
	{Name: DerivationEventURLs, Version: EventURLsVersion, Description: "Project normalized URLs with observed and canonical domains from note-like events"},
	{Name: DerivationNoteDiscoveryStats, Version: NoteDiscoveryStatsVersion, Description: "Project per-note discovery counters and rolling scores"},
	{Name: DerivationProfileDiscoveryStats, Version: ProfileDiscoveryStatsVersion, Description: "Project per-profile discovery counters and rolling scores"},
	{Name: DerivationTrustedNoteDiscovery, Version: TrustedNoteDiscoveryVersion, Description: "Project trust-qualified discovery metadata for note candidates"},
	{Name: DerivationTrustedProfileDiscovery, Version: TrustedProfileDiscoveryVersion, Description: "Project trust-qualified discovery metadata for profile candidates"},
	{Name: DerivationDMUnreadCounts, Version: DMUnreadCountsVersion, Description: "Track unread DM counters by receiver and sender"},
	{Name: DerivationZapReceipts, Version: ZapReceiptsVersion, Description: "Project zap receipts by sender, receiver, target event, and sats"},
	{Name: DerivationProfilePublicStats, Version: ProfilePublicStatsVersion, Description: "Project public profile counters and recent activity"},
	{Name: DerivationAuthorActivityDaily, Version: AuthorActivityDailyVersion, Description: "Project per-author daily post cadence and engagement aggregates"},
	{Name: DerivationAuthorEngagementStats, Version: AuthorEngagementStatsVersion, Description: "Project windowed per-author engagement and cadence summaries"},
	{Name: DerivationAuthorTopicStats, Version: AuthorTopicStatsVersion, Description: "Project windowed per-author hashtag usage summaries"},
	{Name: DerivationAuthorMediaMixStats, Version: AuthorMediaMixStatsVersion, Description: "Project windowed per-author media mix summaries"},
	{Name: DerivationAuthorActivityWindows, Version: AuthorActivityWindowsVersion, Description: "Project windowed per-author engagement timing buckets by UTC day/hour"},
	{Name: DerivationAuthorPostingPatterns, Version: AuthorPostingPatternsVersion, Description: "Project windowed per-author posting cadence buckets by UTC day/hour"},
	{Name: DerivationThreadProjection, Version: ThreadProjectionVersion, Description: "Project reply parent/root edges with unresolved reference tracking"},
	{Name: DerivationThreadSummary, Version: ThreadSummaryVersion, Description: "Project root-level thread summary counters and velocity hints"},
	{Name: DerivationTrustScoresGlobal, Version: TrustScoresGlobalVersion, Description: "Compute global trust scores"},
}

// EnsureRegisteredDerivations performs a one-time, process-startup
// reconciliation of derivation metadata rows. It runs OUTSIDE the per-job hot
// path so that workers never have to UPSERT into the high-contention
// derivation_versions / derivation_active_versions tables while processing
// individual events.
//
// Without this hook, every job execution would fight for row locks on the same
// (projection_name, version) tuple in derivation_versions and on
// (derivation_name) in derivation_active_versions, serializing the entire
// derivation pipeline behind a handful of metadata rows.
func EnsureRegisteredDerivations(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("ensure registered derivations: pool is nil")
	}
	codeVersion := strings.TrimSpace(os.Getenv("APP_VERSION"))
	if codeVersion == "" {
		codeVersion = "dev"
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, d := range RegisteredDerivations {
		if _, err := tx.Exec(ctx, `
			INSERT INTO derivation_versions (projection_name, version, code_version, description, activated_at)
			VALUES ($1, $2, $3, $4, now())
			ON CONFLICT (projection_name, version) DO UPDATE
			SET code_version = EXCLUDED.code_version,
			    description = EXCLUDED.description
		`, d.Name, d.Version, codeVersion, d.Description); err != nil {
			return fmt.Errorf("upsert derivation_versions for %q: %w", d.Name, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO derivation_active_versions (
				derivation_name, active_version, target_version, description
			)
			VALUES ($1, $2, $2, $3)
			ON CONFLICT (derivation_name) DO UPDATE
			SET target_version = EXCLUDED.target_version,
			    description = EXCLUDED.description,
			    updated_at = now()
		`, d.Name, d.Version, d.Description); err != nil {
			return fmt.Errorf("upsert derivation_active_versions for %q: %w", d.Name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit registered derivations: %w", err)
	}
	return nil
}
