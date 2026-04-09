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
)

func TestAuthorAnalyticsProjections_RebuildAfterTruncateMatchesBaseline(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newEventForTest("aa_cov_target", "bob", now.Add(-48*time.Hour).Unix(), 1, nil, `{"content":"seed"}`, now),
		newEventForTest(
			"aa_cov_note",
			"alice",
			now.Add(-24*time.Hour).Unix(),
			1,
			[][]string{{"t", "nostr"}, {"image", "https://cdn/img.png"}},
			`{"content":"note"}`,
			now,
		),
		newEventForTest(
			"aa_cov_reply",
			"alice",
			now.Add(-23*time.Hour).Unix(),
			1,
			[][]string{{"e", "aa_cov_target", "", "reply"}, {"t", "nostr"}},
			`{"content":"reply"}`,
			now,
		),
		newEventForTest(
			"aa_cov_reactor_reply",
			"carol",
			now.Add(-22*time.Hour).Unix(),
			1,
			[][]string{{"e", "aa_cov_note", "", "reply"}},
			`{"content":"nice"}`,
			now,
		),
		newEventForTest(
			"aa_cov_reaction",
			"dave",
			now.Add(-21*time.Hour).Unix(),
			7,
			[][]string{{"e", "aa_cov_note"}},
			`{"content":"+"}`,
			now,
		),
		newEventForTest(
			"aa_cov_repost",
			"erin",
			now.Add(-20*time.Hour).Unix(),
			6,
			[][]string{{"e", "aa_cov_note"}},
			`{"content":"rp"}`,
			now,
		),
	}
	for _, event := range events {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	baselineState := captureAuthorAnalyticsProjectionState(t, ctx, pool, "alice")
	baselineReads := captureAuthorAnalyticsReadSnapshot(t, ctx, pgStore, "alice")

	if _, err := pool.Exec(ctx, `
		TRUNCATE TABLE
			author_activity_daily,
			author_engagement_stats,
			author_topic_stats,
			author_media_mix_stats,
			author_activity_windows,
			author_posting_patterns
		CASCADE
	`); err != nil {
		t.Fatalf("truncate author analytics projection tables: %v", err)
	}

	for _, derivationName := range []string{
		derivation.DerivationAuthorActivityDaily,
		derivation.DerivationAuthorEngagementStats,
		derivation.DerivationAuthorTopicStats,
		derivation.DerivationAuthorMediaMixStats,
		derivation.DerivationAuthorActivityWindows,
		derivation.DerivationAuthorPostingPatterns,
	} {
		run := triggerAndExecuteFullRebuild(t, ctx, handlers, derivationName, 2)
		assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
		assertActiveAndTargetVersion(t, ctx, pool, derivationName, 2, 2)
	}

	rebuiltState := captureAuthorAnalyticsProjectionState(t, ctx, pool, "alice")
	if !reflect.DeepEqual(rebuiltState, baselineState) {
		t.Fatalf("rebuilt author analytics projection state mismatch\nbaseline=%#v\nrebuilt=%#v", baselineState, rebuiltState)
	}
	rebuiltReads := captureAuthorAnalyticsReadSnapshot(t, ctx, pgStore, "alice")
	if !reflect.DeepEqual(rebuiltReads, baselineReads) {
		t.Fatalf("rebuilt author analytics read snapshot mismatch\nbaseline=%#v\nrebuilt=%#v", baselineReads, rebuiltReads)
	}
}

func TestConversationProjections_RebuildAfterTruncateMatchesBaseline(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newEventForTest("conv_cov_root", "root_author", now.Add(-4*time.Hour).Unix(), 1, nil, `{"content":"root"}`, now),
		newEventForTest(
			"conv_cov_reply_a",
			"alice",
			now.Add(-3*time.Hour).Unix(),
			1,
			[][]string{{"e", "conv_cov_root", "", "reply"}, {"e", "conv_cov_root", "", "root"}},
			`{"content":"reply a"}`,
			now,
		),
		newEventForTest(
			"conv_cov_reply_b",
			"bob",
			now.Add(-2*time.Hour).Unix(),
			1,
			[][]string{{"e", "conv_cov_root", "", "reply"}, {"e", "conv_cov_root", "", "root"}},
			`{"content":"reply b"}`,
			now,
		),
		newEventForTest(
			"conv_cov_reply_nested",
			"carol",
			now.Add(-90*time.Minute).Unix(),
			1,
			[][]string{{"e", "conv_cov_root", "", "root"}, {"e", "conv_cov_reply_a", "", "reply"}},
			`{"content":"nested reply"}`,
			now,
		),
	}
	for _, event := range events {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	baselineState := captureConversationProjectionState(t, ctx, pool)
	baselineSummary, err := pgStore.GetThreadSummary(ctx, "conv_cov_root")
	if err != nil {
		t.Fatalf("GetThreadSummary baseline: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		TRUNCATE TABLE
			thread_edges,
			unresolved_thread_references,
			thread_summaries
		CASCADE
	`); err != nil {
		t.Fatalf("truncate conversation projection tables: %v", err)
	}

	for _, derivationName := range []string{
		derivation.DerivationThreadProjection,
		derivation.DerivationThreadSummary,
	} {
		run := triggerAndExecuteFullRebuild(t, ctx, handlers, derivationName, 2)
		assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
		assertActiveAndTargetVersion(t, ctx, pool, derivationName, 2, 2)
	}

	rebuiltState := captureConversationProjectionState(t, ctx, pool)
	if !reflect.DeepEqual(rebuiltState, baselineState) {
		t.Fatalf("rebuilt conversation projection state mismatch\nbaseline=%#v\nrebuilt=%#v", baselineState, rebuiltState)
	}
	rebuiltSummary, err := pgStore.GetThreadSummary(ctx, "conv_cov_root")
	if err != nil {
		t.Fatalf("GetThreadSummary rebuilt: %v", err)
	}
	if !reflect.DeepEqual(rebuiltSummary, baselineSummary) {
		t.Fatalf("rebuilt thread summary mismatch\nbaseline=%#v\nrebuilt=%#v", baselineSummary, rebuiltSummary)
	}
}

type authorAnalyticsProjectionState struct {
	ActivityDaily   []authorActivityDailyRow
	EngagementStats []authorEngagementStatsRow
	TopicStats      []authorTopicStatsRow
	MediaMixStats   []authorMediaMixStatsRow
	ActivityWindows []authorActivityWindowRow
	PostingPatterns []authorPostingPatternRow
}

type authorActivityDailyRow struct {
	ActivityDate       time.Time
	PostCount          int64
	NoteCount          int64
	ReplyCount         int64
	EngagementReceived int64
	EngagementGiven    int64
}

type authorEngagementStatsRow struct {
	WindowDays               int
	PostCount                int64
	NoteCount                int64
	ReplyCount               int64
	ActiveDays               int
	EngagementReceived       int64
	EngagementGiven          int64
	CadencePostsPerDay       float64
	CadencePostsPerActiveDay float64
	RecentActivityAt         *int64
}

type authorTopicStatsRow struct {
	WindowDays int
	Hashtag    string
	UsageCount int64
	ActiveDays int
}

type authorMediaMixStatsRow struct {
	WindowDays           int
	TotalPosts           int64
	WithImageCount       int64
	WithVideoCount       int64
	WithLinkCount        int64
	WithArticleCount     int64
	TextOnlyCount        int64
	TotalAttachmentCount int64
}

type authorActivityWindowRow struct {
	WindowDays         int
	DayOfWeek          int
	HourOfDay          int
	EngagementReceived int64
	ReplyReceived      int64
	ReactionReceived   int64
	RepostReceived     int64
	ZapReceived        int64
}

type authorPostingPatternRow struct {
	WindowDays int
	DayOfWeek  int
	HourOfDay  int
	PostCount  int64
	NoteCount  int64
	ReplyCount int64
}

type authorAnalyticsReadSnapshot struct {
	Summary          []store.AuthorAnalyticsSummaryProjection
	TopicStats       []store.AuthorTopicStatsProjection
	MediaMix         store.AuthorMediaMixStatsProjection
	ActivityWindows  []store.AuthorActivityWindowBucketProjection
	PostingPatterns  []store.AuthorPostingPatternBucketProjection
	GroupedAnalytics store.GroupedNoteAnalyticsProjection
}

func captureAuthorAnalyticsProjectionState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	pubkey string,
) authorAnalyticsProjectionState {
	t.Helper()
	return authorAnalyticsProjectionState{
		ActivityDaily:   readAuthorActivityDailyRows(t, ctx, pool, pubkey),
		EngagementStats: readAuthorEngagementStatsRows(t, ctx, pool, pubkey),
		TopicStats:      readAuthorTopicStatsRows(t, ctx, pool, pubkey),
		MediaMixStats:   readAuthorMediaMixStatsRows(t, ctx, pool, pubkey),
		ActivityWindows: readAuthorActivityWindowRows(t, ctx, pool, pubkey),
		PostingPatterns: readAuthorPostingPatternRows(t, ctx, pool, pubkey),
	}
}

func captureAuthorAnalyticsReadSnapshot(
	t *testing.T,
	ctx context.Context,
	pgStore *store.PostgresStore,
	pubkey string,
) authorAnalyticsReadSnapshot {
	t.Helper()
	summary, err := pgStore.GetAuthorAnalyticsSummary(ctx, pubkey)
	if err != nil {
		t.Fatalf("GetAuthorAnalyticsSummary: %v", err)
	}
	topics, err := pgStore.GetAuthorTopicStats(ctx, pubkey, 30, 10)
	if err != nil {
		t.Fatalf("GetAuthorTopicStats: %v", err)
	}
	mediaMix, err := pgStore.GetAuthorMediaMixStats(ctx, pubkey, 30)
	if err != nil {
		t.Fatalf("GetAuthorMediaMixStats: %v", err)
	}
	activityWindows, err := pgStore.GetAuthorActivityWindowBuckets(ctx, pubkey, 30)
	if err != nil {
		t.Fatalf("GetAuthorActivityWindowBuckets: %v", err)
	}
	postingPatterns, err := pgStore.GetAuthorPostingPatternBuckets(ctx, pubkey, 30)
	if err != nil {
		t.Fatalf("GetAuthorPostingPatternBuckets: %v", err)
	}
	grouped, err := pgStore.GetGroupedNoteAnalytics(ctx, store.GroupedNoteAnalyticsQuery{
		Pubkey:        pubkey,
		WindowDays:    30,
		GroupKind:     "hashtag",
		GroupKey:      "nostr",
		TopNotesLimit: 5,
		TopicsLimit:   5,
	})
	if err != nil {
		t.Fatalf("GetGroupedNoteAnalytics: %v", err)
	}
	return authorAnalyticsReadSnapshot{
		Summary:          summary,
		TopicStats:       topics,
		MediaMix:         mediaMix,
		ActivityWindows:  activityWindows,
		PostingPatterns:  postingPatterns,
		GroupedAnalytics: grouped,
	}
}

func readAuthorActivityDailyRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) []authorActivityDailyRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT activity_date, post_count, note_count, reply_count, engagement_received, engagement_given
		FROM author_activity_daily
		WHERE pubkey = $1
		ORDER BY activity_date ASC
	`, pubkey)
	if err != nil {
		t.Fatalf("query author_activity_daily rows: %v", err)
	}
	defer rows.Close()
	out := make([]authorActivityDailyRow, 0)
	for rows.Next() {
		var row authorActivityDailyRow
		if err := rows.Scan(
			&row.ActivityDate,
			&row.PostCount,
			&row.NoteCount,
			&row.ReplyCount,
			&row.EngagementReceived,
			&row.EngagementGiven,
		); err != nil {
			t.Fatalf("scan author_activity_daily row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read author_activity_daily rows: %v", err)
	}
	return out
}

func readAuthorEngagementStatsRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) []authorEngagementStatsRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT
			window_days,
			post_count,
			note_count,
			reply_count,
			active_days,
			engagement_received,
			engagement_given,
			cadence_posts_per_day,
			cadence_posts_per_active_day,
			recent_activity_at
		FROM author_engagement_stats
		WHERE pubkey = $1
		ORDER BY window_days ASC
	`, pubkey)
	if err != nil {
		t.Fatalf("query author_engagement_stats rows: %v", err)
	}
	defer rows.Close()
	out := make([]authorEngagementStatsRow, 0)
	for rows.Next() {
		var row authorEngagementStatsRow
		if err := rows.Scan(
			&row.WindowDays,
			&row.PostCount,
			&row.NoteCount,
			&row.ReplyCount,
			&row.ActiveDays,
			&row.EngagementReceived,
			&row.EngagementGiven,
			&row.CadencePostsPerDay,
			&row.CadencePostsPerActiveDay,
			&row.RecentActivityAt,
		); err != nil {
			t.Fatalf("scan author_engagement_stats row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read author_engagement_stats rows: %v", err)
	}
	return out
}

func readAuthorTopicStatsRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) []authorTopicStatsRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT window_days, hashtag, usage_count, active_days
		FROM author_topic_stats
		WHERE pubkey = $1
		ORDER BY window_days ASC, hashtag ASC
	`, pubkey)
	if err != nil {
		t.Fatalf("query author_topic_stats rows: %v", err)
	}
	defer rows.Close()
	out := make([]authorTopicStatsRow, 0)
	for rows.Next() {
		var row authorTopicStatsRow
		if err := rows.Scan(&row.WindowDays, &row.Hashtag, &row.UsageCount, &row.ActiveDays); err != nil {
			t.Fatalf("scan author_topic_stats row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read author_topic_stats rows: %v", err)
	}
	return out
}

func readAuthorMediaMixStatsRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) []authorMediaMixStatsRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT
			window_days,
			total_posts,
			with_image_count,
			with_video_count,
			with_link_count,
			with_article_count,
			text_only_count,
			total_attachment_count
		FROM author_media_mix_stats
		WHERE pubkey = $1
		ORDER BY window_days ASC
	`, pubkey)
	if err != nil {
		t.Fatalf("query author_media_mix_stats rows: %v", err)
	}
	defer rows.Close()
	out := make([]authorMediaMixStatsRow, 0)
	for rows.Next() {
		var row authorMediaMixStatsRow
		if err := rows.Scan(
			&row.WindowDays,
			&row.TotalPosts,
			&row.WithImageCount,
			&row.WithVideoCount,
			&row.WithLinkCount,
			&row.WithArticleCount,
			&row.TextOnlyCount,
			&row.TotalAttachmentCount,
		); err != nil {
			t.Fatalf("scan author_media_mix_stats row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read author_media_mix_stats rows: %v", err)
	}
	return out
}

func readAuthorActivityWindowRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) []authorActivityWindowRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT
			window_days,
			day_of_week,
			hour_of_day,
			engagement_received,
			reply_received,
			reaction_received,
			repost_received,
			zap_received
		FROM author_activity_windows
		WHERE pubkey = $1
		ORDER BY window_days ASC, day_of_week ASC, hour_of_day ASC
	`, pubkey)
	if err != nil {
		t.Fatalf("query author_activity_windows rows: %v", err)
	}
	defer rows.Close()
	out := make([]authorActivityWindowRow, 0)
	for rows.Next() {
		var row authorActivityWindowRow
		if err := rows.Scan(
			&row.WindowDays,
			&row.DayOfWeek,
			&row.HourOfDay,
			&row.EngagementReceived,
			&row.ReplyReceived,
			&row.ReactionReceived,
			&row.RepostReceived,
			&row.ZapReceived,
		); err != nil {
			t.Fatalf("scan author_activity_windows row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read author_activity_windows rows: %v", err)
	}
	return out
}

func readAuthorPostingPatternRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) []authorPostingPatternRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT
			window_days,
			day_of_week,
			hour_of_day,
			post_count,
			note_count,
			reply_count
		FROM author_posting_patterns
		WHERE pubkey = $1
		ORDER BY window_days ASC, day_of_week ASC, hour_of_day ASC
	`, pubkey)
	if err != nil {
		t.Fatalf("query author_posting_patterns rows: %v", err)
	}
	defer rows.Close()
	out := make([]authorPostingPatternRow, 0)
	for rows.Next() {
		var row authorPostingPatternRow
		if err := rows.Scan(
			&row.WindowDays,
			&row.DayOfWeek,
			&row.HourOfDay,
			&row.PostCount,
			&row.NoteCount,
			&row.ReplyCount,
		); err != nil {
			t.Fatalf("scan author_posting_patterns row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read author_posting_patterns rows: %v", err)
	}
	return out
}

type conversationProjectionState struct {
	ThreadEdges []conversationThreadEdgeRow
	Unresolved  []conversationUnresolvedRow
	Summaries   []conversationThreadSummaryRow
}

type conversationThreadEdgeRow struct {
	ChildEventID  string
	ParentEventID string
	RootEventID   string
	ParentMissing bool
	RootMissing   bool
}

type conversationUnresolvedRow struct {
	SourceEventID  string
	MissingEventID string
}

type conversationThreadSummaryRow struct {
	RootEventID      string
	ReplyCount       int64
	ParticipantCount int
	MaxDepth         int
	LastActivityAt   int64
	Replies24h       int64
	Replies7d        int64
}

func captureConversationProjectionState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) conversationProjectionState {
	t.Helper()
	return conversationProjectionState{
		ThreadEdges: readConversationThreadEdges(t, ctx, pool),
		Unresolved:  readConversationUnresolvedRows(t, ctx, pool),
		Summaries:   readConversationThreadSummaries(t, ctx, pool),
	}
}

func readConversationThreadEdges(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []conversationThreadEdgeRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT child_event_id, parent_event_id, root_event_id, parent_missing, root_missing
		FROM thread_edges
		ORDER BY child_event_id ASC
	`)
	if err != nil {
		t.Fatalf("query thread_edges rows: %v", err)
	}
	defer rows.Close()
	out := make([]conversationThreadEdgeRow, 0)
	for rows.Next() {
		var row conversationThreadEdgeRow
		if err := rows.Scan(
			&row.ChildEventID,
			&row.ParentEventID,
			&row.RootEventID,
			&row.ParentMissing,
			&row.RootMissing,
		); err != nil {
			t.Fatalf("scan thread_edges row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read thread_edges rows: %v", err)
	}
	return out
}

func readConversationUnresolvedRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []conversationUnresolvedRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT source_event_id, missing_event_id
		FROM unresolved_thread_references
		ORDER BY source_event_id ASC, missing_event_id ASC
	`)
	if err != nil {
		t.Fatalf("query unresolved_thread_references rows: %v", err)
	}
	defer rows.Close()
	out := make([]conversationUnresolvedRow, 0)
	for rows.Next() {
		var row conversationUnresolvedRow
		if err := rows.Scan(&row.SourceEventID, &row.MissingEventID); err != nil {
			t.Fatalf("scan unresolved_thread_references row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read unresolved_thread_references rows: %v", err)
	}
	return out
}

func readConversationThreadSummaries(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []conversationThreadSummaryRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT root_event_id, reply_count, participant_count, max_depth, last_activity_at, replies_24h, replies_7d
		FROM thread_summaries
		ORDER BY root_event_id ASC
	`)
	if err != nil {
		t.Fatalf("query thread_summaries rows: %v", err)
	}
	defer rows.Close()
	out := make([]conversationThreadSummaryRow, 0)
	for rows.Next() {
		var row conversationThreadSummaryRow
		if err := rows.Scan(
			&row.RootEventID,
			&row.ReplyCount,
			&row.ParticipantCount,
			&row.MaxDepth,
			&row.LastActivityAt,
			&row.Replies24h,
			&row.Replies7d,
		); err != nil {
			t.Fatalf("scan thread_summaries row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read thread_summaries rows: %v", err)
	}
	return out
}
