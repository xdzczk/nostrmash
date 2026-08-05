package derivation_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	storeread "github.com/xdzczk/nostrmash/internal/store/read"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestDiscoveryProjections_RebuildAfterTruncateMatchesBaseline(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newEventForTest("discover_note_hot", "alice", now.Add(-2*time.Hour).Unix(), 1, [][]string{{"t", "nostr"}}, "hot note", now.Add(-2*time.Hour)),
		newEventForTest("discover_note_warm", "bob", now.Add(-3*time.Hour).Unix(), 1, [][]string{{"t", "bitcoin"}}, "warm note", now.Add(-3*time.Hour)),
		newEventForTest("discover_note_tag", "alice", now.Add(-90*time.Minute).Unix(), 1, [][]string{{"t", "nostr"}}, "tag note", now.Add(-90*time.Minute)),
		newEventForTest("discover_reply_hot_1", "carol", now.Add(-80*time.Minute).Unix(), 1, [][]string{{"e", "discover_note_hot", "", "reply"}}, "reply1", now.Add(-80*time.Minute)),
		newEventForTest("discover_reply_hot_2", "dave", now.Add(-70*time.Minute).Unix(), 1, [][]string{{"e", "discover_note_hot", "", "reply"}}, "reply2", now.Add(-70*time.Minute)),
		newEventForTest("discover_react_hot_1", "erin", now.Add(-60*time.Minute).Unix(), 7, [][]string{{"e", "discover_note_hot"}}, "+", now.Add(-60*time.Minute)),
		newEventForTest("discover_react_hot_2", "frank", now.Add(-50*time.Minute).Unix(), 7, [][]string{{"e", "discover_note_hot"}}, "+", now.Add(-50*time.Minute)),
		newEventForTest("discover_repost_hot", "grace", now.Add(-40*time.Minute).Unix(), 6, [][]string{{"e", "discover_note_hot"}}, "rp", now.Add(-40*time.Minute)),
		newEventForTest("discover_zap_hot", "heidi", now.Add(-30*time.Minute).Unix(), 9735, [][]string{{"e", "discover_note_hot"}, {"p", "alice"}, {"amount", "42000"}}, "", now.Add(-30*time.Minute)),
		newEventForTest("discover_contacts_bob", "bob", now.Add(-20*time.Minute).Unix(), 3, [][]string{{"p", "alice"}}, "contacts", now.Add(-20*time.Minute)),
		newEventForTest("discover_contacts_carol", "carol", now.Add(-10*time.Minute).Unix(), 3, [][]string{{"p", "alice"}}, "contacts", now.Add(-10*time.Minute)),
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	drainPendingProfileStatsForTest(t, ctx, handlers)

	baselineState := captureDiscoveryProjectionState(t, ctx, pool)
	baselineReads := captureDiscoveryReadSnapshot(t, ctx, pgStore)

	if _, err := pool.Exec(ctx, `
		TRUNCATE TABLE
			event_hashtags,
			note_discovery_stats,
			profile_discovery_stats,
			profile_public_stats,
			follower_edges
		CASCADE
	`); err != nil {
		t.Fatalf("truncate discovery projection tables: %v", err)
	}

	for _, derivationName := range []string{
		derivation.DerivationEventHashtags,
		derivation.DerivationNoteDiscoveryStats,
		derivation.DerivationFollowerEdges,
		derivation.DerivationProfilePublicStats,
		derivation.DerivationProfileDiscoveryStats,
	} {
		run := triggerAndExecuteFullRebuild(t, ctx, handlers, derivationName, 2)
		assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
		assertActiveAndTargetVersion(t, ctx, pool, derivationName, 2, 2)
	}

	rebuiltState := captureDiscoveryProjectionState(t, ctx, pool)
	if !reflect.DeepEqual(stripDiscoveryProjectionVersions(rebuiltState), stripDiscoveryProjectionVersions(baselineState)) {
		t.Fatalf("rebuilt discovery projection state mismatch\nbaseline=%#v\nrebuilt=%#v", baselineState, rebuiltState)
	}

	rebuiltReads := captureDiscoveryReadSnapshot(t, ctx, pgStore)
	if !reflect.DeepEqual(rebuiltReads, baselineReads) {
		t.Fatalf("rebuilt discovery read snapshot mismatch\nbaseline=%#v\nrebuilt=%#v", baselineReads, rebuiltReads)
	}
}

func triggerAndExecuteFullRebuild(
	t *testing.T,
	ctx context.Context,
	handlers *derivation.Handlers,
	derivationName string,
	targetVersion int,
) derivation.ProjectionRebuildRun {
	t.Helper()
	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivationName,
		TargetVersion:  targetVersion,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger rebuild %s: %v", derivationName, err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err != nil {
		t.Fatalf("execute rebuild %s run %d: %v", derivationName, run.ID, err)
	}
	return run
}

type discoveryProjectionState struct {
	Hashtags         []hashtagProjectionRow
	NoteStats        []noteStatsProjectionRow
	ProfileStats     []profileDiscoveryProjectionRow
	ProfilePublic    []profilePublicProjectionRow
	FollowerEdgeRows []followerEdgeProjectionRow
}

type hashtagProjectionRow struct {
	EventID        string
	AuthorPubkey   string
	CreatedAt      int64
	Hashtag        string
	DerivedVersion int
}

type noteStatsProjectionRow struct {
	EventID        string
	AuthorPubkey   string
	CreatedAt      int64
	ReplyCount     int64
	RepostCount    int64
	ReactionCount  int64
	ZapCount       int64
	ZapMSats       int64
	DerivedVersion int
}

type profileDiscoveryProjectionRow struct {
	Pubkey                   string
	RecentPostCount          int64
	RecentReplyCount         int64
	RecentEngagementReceived int64
	RecentZapVolumeMSats     int64
	RecentActiveDays         int
	RecentActivityAtUnix     int64
	DerivedVersion           int
}

type profilePublicProjectionRow struct {
	Pubkey             string
	FollowerCount      int64
	FollowingCount     int64
	NoteCount          int64
	ReplyCount         int64
	RecentActivityUnix int64
	DerivedVersion     int
}

type followerEdgeProjectionRow struct {
	FollowedPubkey   string
	FollowerPubkey   string
	SourceEventID    string
	ContactCreatedAt int64
	DerivedVersion   int
}

func captureDiscoveryProjectionState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) discoveryProjectionState {
	t.Helper()
	return discoveryProjectionState{
		Hashtags:         readHashtagProjectionRows(t, ctx, pool),
		NoteStats:        readNoteProjectionRows(t, ctx, pool),
		ProfileStats:     readProfileDiscoveryRows(t, ctx, pool),
		ProfilePublic:    readProfilePublicRows(t, ctx, pool),
		FollowerEdgeRows: readFollowerEdgeRows(t, ctx, pool),
	}
}

func stripDiscoveryProjectionVersions(state discoveryProjectionState) discoveryProjectionState {
	out := state
	for i := range out.Hashtags {
		out.Hashtags[i].DerivedVersion = 0
	}
	for i := range out.NoteStats {
		out.NoteStats[i].DerivedVersion = 0
	}
	for i := range out.ProfileStats {
		out.ProfileStats[i].DerivedVersion = 0
	}
	for i := range out.ProfilePublic {
		out.ProfilePublic[i].DerivedVersion = 0
	}
	for i := range out.FollowerEdgeRows {
		out.FollowerEdgeRows[i].DerivedVersion = 0
	}
	return out
}

func readHashtagProjectionRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []hashtagProjectionRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT event_id, author_pubkey, created_at, hashtag, derivation_version
		FROM event_hashtags
		ORDER BY event_id ASC, hashtag ASC
	`)
	if err != nil {
		t.Fatalf("query event_hashtags rows: %v", err)
	}
	defer rows.Close()
	out := make([]hashtagProjectionRow, 0)
	for rows.Next() {
		var row hashtagProjectionRow
		if err := rows.Scan(&row.EventID, &row.AuthorPubkey, &row.CreatedAt, &row.Hashtag, &row.DerivedVersion); err != nil {
			t.Fatalf("scan event_hashtags row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read event_hashtags rows: %v", err)
	}
	return out
}

func readNoteProjectionRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []noteStatsProjectionRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT
			event_id,
			author_pubkey,
			created_at,
			reply_count,
			repost_count,
			reaction_count,
			zap_count,
			zap_msats,
			derivation_version
		FROM note_discovery_stats
		ORDER BY event_id ASC
	`)
	if err != nil {
		t.Fatalf("query note_discovery_stats rows: %v", err)
	}
	defer rows.Close()
	out := make([]noteStatsProjectionRow, 0)
	for rows.Next() {
		var row noteStatsProjectionRow
		if err := rows.Scan(
			&row.EventID,
			&row.AuthorPubkey,
			&row.CreatedAt,
			&row.ReplyCount,
			&row.RepostCount,
			&row.ReactionCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.DerivedVersion,
		); err != nil {
			t.Fatalf("scan note_discovery_stats row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read note_discovery_stats rows: %v", err)
	}
	return out
}

func readProfileDiscoveryRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []profileDiscoveryProjectionRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT
			pubkey,
			recent_post_count,
			recent_reply_count,
			recent_engagement_received,
			recent_zap_volume_msats,
			recent_active_days,
			COALESCE(recent_activity_at, 0),
			derivation_version
		FROM profile_discovery_stats
		ORDER BY pubkey ASC
	`)
	if err != nil {
		t.Fatalf("query profile_discovery_stats rows: %v", err)
	}
	defer rows.Close()
	out := make([]profileDiscoveryProjectionRow, 0)
	for rows.Next() {
		var row profileDiscoveryProjectionRow
		if err := rows.Scan(
			&row.Pubkey,
			&row.RecentPostCount,
			&row.RecentReplyCount,
			&row.RecentEngagementReceived,
			&row.RecentZapVolumeMSats,
			&row.RecentActiveDays,
			&row.RecentActivityAtUnix,
			&row.DerivedVersion,
		); err != nil {
			t.Fatalf("scan profile_discovery_stats row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read profile_discovery_stats rows: %v", err)
	}
	return out
}

func readProfilePublicRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []profilePublicProjectionRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT
			pubkey,
			follower_count,
			following_count,
			note_count,
			reply_count,
			COALESCE(recent_activity_at, 0),
			derivation_version
		FROM profile_public_stats
		ORDER BY pubkey ASC
	`)
	if err != nil {
		t.Fatalf("query profile_public_stats rows: %v", err)
	}
	defer rows.Close()
	out := make([]profilePublicProjectionRow, 0)
	for rows.Next() {
		var row profilePublicProjectionRow
		if err := rows.Scan(
			&row.Pubkey,
			&row.FollowerCount,
			&row.FollowingCount,
			&row.NoteCount,
			&row.ReplyCount,
			&row.RecentActivityUnix,
			&row.DerivedVersion,
		); err != nil {
			t.Fatalf("scan profile_public_stats row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read profile_public_stats rows: %v", err)
	}
	return out
}

func readFollowerEdgeRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []followerEdgeProjectionRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT
			followed_pubkey,
			follower_pubkey,
			source_event_id,
			contact_list_created_at,
			derivation_version
		FROM follower_edges
		ORDER BY followed_pubkey ASC, follower_pubkey ASC
	`)
	if err != nil {
		t.Fatalf("query follower_edges rows: %v", err)
	}
	defer rows.Close()
	out := make([]followerEdgeProjectionRow, 0)
	for rows.Next() {
		var row followerEdgeProjectionRow
		if err := rows.Scan(
			&row.FollowedPubkey,
			&row.FollowerPubkey,
			&row.SourceEventID,
			&row.ContactCreatedAt,
			&row.DerivedVersion,
		); err != nil {
			t.Fatalf("scan follower_edges row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read follower_edges rows: %v", err)
	}
	return out
}

type discoveryReadSnapshot struct {
	TrendingNotes    []trendingNoteReadRow
	TrendingProfiles []trendingProfileReadRow
	RisingProfiles   []trendingProfileReadRow
	Hashtags         []storeread.TrendingHashtag
	Network          publicNetworkReadRow
}

type trendingNoteReadRow struct {
	EventID       string
	ReplyCount    int64
	RepostCount   int64
	ReactionCount int64
	ZapCount      int64
}

type trendingProfileReadRow struct {
	Pubkey                   string
	RecentPostCount          int64
	RecentReplyCount         int64
	RecentEngagementReceived int64
	RecentZapVolumeMSats     int64
	RecentActiveDays         int
	RecentActivityAtUnix     int64
}

type publicNetworkReadRow struct {
	EventsIngested    int64
	ProjectedProfiles int64
	Relays            int64
	ActiveAuthors24h  int64
	ActiveAuthors7d   int64
	NoteVolume24h     int64
	NoteVolume7d      int64
	TopHashtag24h     string
	TopHashtag7d      string
}

func captureDiscoveryReadSnapshot(t *testing.T, ctx context.Context, pgStore *store.PostgresStore) discoveryReadSnapshot {
	t.Helper()
	trendingNotes, err := pgStore.GetTrendingNotes(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("get trending notes: %v", err)
	}
	trendingProfiles, err := pgStore.GetTrendingProfiles(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("get trending profiles: %v", err)
	}
	risingProfiles, err := pgStore.GetRisingProfiles(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("get rising profiles: %v", err)
	}
	hashtags, err := pgStore.GetTrendingHashtags(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("get trending hashtags: %v", err)
	}
	networkStats, err := pgStore.GetPublicDiscoveryNetworkStats(ctx, 10)
	if err != nil {
		t.Fatalf("get network stats: %v", err)
	}

	return discoveryReadSnapshot{
		TrendingNotes:    normalizeTrendingNotes(trendingNotes),
		TrendingProfiles: normalizeTrendingProfiles(trendingProfiles),
		RisingProfiles:   normalizeTrendingProfiles(risingProfiles),
		Hashtags:         hashtags,
		Network:          normalizeNetworkStats(networkStats),
	}
}

func normalizeTrendingNotes(rows []storeread.TrendingNote) []trendingNoteReadRow {
	out := make([]trendingNoteReadRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingNoteReadRow{
			EventID:       row.EventID,
			ReplyCount:    row.ReplyCount,
			RepostCount:   row.RepostCount,
			ReactionCount: row.ReactionCount,
			ZapCount:      row.ZapCount,
		})
	}
	return out
}

func normalizeTrendingProfiles(rows []storeread.TrendingProfile) []trendingProfileReadRow {
	out := make([]trendingProfileReadRow, 0, len(rows))
	for _, row := range rows {
		recentActivityAt := int64(0)
		if row.RecentActivityAt != nil {
			recentActivityAt = *row.RecentActivityAt
		}
		out = append(out, trendingProfileReadRow{
			Pubkey:                   row.Pubkey,
			RecentPostCount:          row.RecentPostCount,
			RecentReplyCount:         row.RecentReplyCount,
			RecentEngagementReceived: row.RecentEngagementReceived,
			RecentZapVolumeMSats:     row.RecentZapVolumeMSats,
			RecentActiveDays:         row.RecentActiveDays,
			RecentActivityAtUnix:     recentActivityAt,
		})
	}
	return out
}

func normalizeNetworkStats(stats storeread.PublicDiscoveryNetworkStats) publicNetworkReadRow {
	out := publicNetworkReadRow{
		// EventsIngested comes from pg_class.reltuples (approxLiveTupleCount)
		// and can move after ANALYZE during rebuilds; it is not a projection
		// invariant worth asserting here.
		EventsIngested:    0,
		ProjectedProfiles: stats.ProjectedProfiles,
		Relays:            stats.Relays,
		ActiveAuthors24h:  stats.ActiveAuthors.Last24h,
		ActiveAuthors7d:   stats.ActiveAuthors.Last7d,
		NoteVolume24h:     stats.NoteVolume.Last24h,
		NoteVolume7d:      stats.NoteVolume.Last7d,
	}
	if stats.TopHashtags != nil {
		if len(stats.TopHashtags.Last24h) > 0 {
			out.TopHashtag24h = stats.TopHashtags.Last24h[0].Hashtag
		}
		if len(stats.TopHashtags.Last7d) > 0 {
			out.TopHashtag7d = stats.TopHashtags.Last7d[0].Hashtag
		}
	}
	return out
}
