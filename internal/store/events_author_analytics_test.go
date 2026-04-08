package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestGetAuthorTopNotes_OrderingAndWindow(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC().Unix()

	mustInsertEventRow(t, pool, "note_high", "author_x", now-3600, 1)
	mustInsertEventRow(t, pool, "note_low", "author_x", now-5400, 1)
	mustInsertEventRow(t, pool, "note_old", "author_x", now-(40*24*60*60), 1)

	mustInsertEventHashtag(t, pool, "note_high", "author_x", now-3600, "nostr")
	mustInsertEventHashtag(t, pool, "note_low", "author_x", now-5400, "bitcoin")
	mustInsertEventTag(t, pool, "note_high", "image", 0, 0, "https://img.example/high.png")

	mustInsertEventRow(t, pool, "reply_h_1", "other_1", now-3000, 1)
	mustInsertEventRow(t, pool, "reply_h_2", "other_2", now-2900, 1)
	mustInsertEventRow(t, pool, "reply_h_3", "other_3", now-2800, 1)
	mustInsertEventRow(t, pool, "reply_l_1", "other_4", now-2700, 1)
	mustInsertReplyContribution(t, pool, "reply_h_1", "note_high")
	mustInsertReplyContribution(t, pool, "reply_h_2", "note_high")
	mustInsertReplyContribution(t, pool, "reply_h_3", "note_high")
	mustInsertReplyContribution(t, pool, "reply_l_1", "note_low")

	rows, err := pgStore.GetAuthorTopNotes(ctx, "author_x", 30, 10)
	if err != nil {
		t.Fatalf("GetAuthorTopNotes: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 notes in 30d window, got %d", len(rows))
	}
	if rows[0].EventID != "note_high" || rows[1].EventID != "note_low" {
		t.Fatalf("unexpected top-note ordering: %#v", rows)
	}
	if rows[0].WeightedEngagement <= rows[1].WeightedEngagement {
		t.Fatalf("expected descending weighted engagement ordering: %#v", rows)
	}
	if rows[0].MediaSegment != "image" {
		t.Fatalf("expected media segment classification, got %#v", rows[0])
	}
	if rows[0].PrimaryTopicHashtag == nil || *rows[0].PrimaryTopicHashtag != "nostr" {
		t.Fatalf("expected primary topic hashtag, got %#v", rows[0].PrimaryTopicHashtag)
	}
}

func TestAuthorQuoteRepostRollupsAndRecentActivity(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC().Unix()

	mustInsertEventRow(t, pool, "author_note", "author_rollup", now-3600, 1)
	mustInsertEventRow(t, pool, "target_external", "author_other", now-3500, 1)
	mustInsertRepostEventWithQuote(t, pool, "author_quote_out", "author_rollup", "target_external", "opinion", now-3000)
	mustInsertRepostEventWithQuote(t, pool, "author_repost_out", "author_rollup", "target_external", "", now-2900)
	mustInsertRepostEventWithQuote(t, pool, "incoming_quote", "other_actor", "author_note", "nice", now-2800)
	mustInsertRepostEventWithQuote(t, pool, "incoming_repost", "other_actor_2", "author_note", "", now-2700)

	if _, err := pool.Exec(ctx, `
		INSERT INTO author_engagement_stats (
			pubkey, window_days, post_count, note_count, reply_count, active_days,
			engagement_received, engagement_given, cadence_posts_per_day, cadence_posts_per_active_day,
			recent_activity_at, derivation_version
		)
		VALUES ($1, 7, 2, 2, 0, 1, 4, 2, 0.2, 2.0, $2, 1)
	`, "author_rollup", now-1000); err != nil {
		t.Fatalf("insert author_engagement_stats: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO author_media_mix_stats (
			pubkey, window_days, total_posts, with_image_count, with_video_count, with_link_count, text_only_count, derivation_version
		)
		VALUES ($1, 7, 2, 0, 0, 0, 2, 1)
	`, "author_rollup"); err != nil {
		t.Fatalf("insert author_media_mix_stats: %v", err)
	}

	summary, err := pgStore.GetAuthorAnalyticsSummary(ctx, "author_rollup")
	if err != nil {
		t.Fatalf("GetAuthorAnalyticsSummary: %v", err)
	}
	if len(summary) != 1 {
		t.Fatalf("expected 1 summary window, got %d", len(summary))
	}
	window := summary[0].QuoteRepost
	if window.QuotesMade != 1 || window.RepostsMade != 1 || window.QuotesReceived != 1 || window.RepostsReceived != 1 {
		t.Fatalf("unexpected quote/repost window rollup: %#v", window)
	}

	recent, err := pgStore.GetAuthorQuoteRepostRecentActivity(ctx, "author_rollup", 5)
	if err != nil {
		t.Fatalf("GetAuthorQuoteRepostRecentActivity: %v", err)
	}
	if len(recent) != 2 {
		t.Fatalf("expected 2 recent outgoing activities, got %d", len(recent))
	}
	if recent[0].Action != "repost" || recent[1].Action != "quote" {
		t.Fatalf("unexpected action ordering for recent activities: %#v", recent)
	}
	if recent[0].LinkedNote.EventID != "target_external" {
		t.Fatalf("unexpected linked note in recent activity: %#v", recent[0].LinkedNote)
	}
}

func TestGetAuthorPerformanceAggregate_SummaryCorrectness(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC().Unix()

	mustInsertEventRow(t, pool, "current_1", "author_y", now-(2*24*60*60), 1)
	mustInsertEventRow(t, pool, "current_2", "author_y", now-(3*24*60*60), 1)
	mustInsertEventRow(t, pool, "previous_1", "author_y", now-(10*24*60*60), 1)

	mustInsertEventRow(t, pool, "reply_c1_1", "other_1", now-(2*24*60*60)+120, 1)
	mustInsertEventRow(t, pool, "reply_c1_2", "other_2", now-(2*24*60*60)+130, 1)
	mustInsertEventRow(t, pool, "reply_p1_1", "other_3", now-(10*24*60*60)+180, 1)
	mustInsertReplyContribution(t, pool, "reply_c1_1", "current_1")
	mustInsertReplyContribution(t, pool, "reply_c1_2", "current_1")
	mustInsertReplyContribution(t, pool, "reply_p1_1", "previous_1")

	mustInsertEventRow(t, pool, "react_c2_1", "other_4", now-(2*24*60*60)+240, 7)
	mustInsertReactionEvent(t, pool, "react_c2_1", "current_2", "other_4", now-(2*24*60*60)+240)

	current, previous, err := pgStore.GetAuthorPerformanceAggregate(ctx, "author_y", 7)
	if err != nil {
		t.Fatalf("GetAuthorPerformanceAggregate: %v", err)
	}
	if current.NoteCount != 2 {
		t.Fatalf("expected current note count 2, got %d", current.NoteCount)
	}
	if current.TotalWeightedEngagement != 7 {
		t.Fatalf("expected current weighted total 7, got %v", current.TotalWeightedEngagement)
	}
	if current.AverageWeightedEngagement != 3.5 || current.MedianWeightedEngagement != 3.5 {
		t.Fatalf("unexpected current weighted stats: %#v", current)
	}
	if previous.NoteCount != 1 || previous.TotalWeightedEngagement != 3 {
		t.Fatalf("unexpected previous stats: %#v", previous)
	}
}

func TestGetAuthorRecycleCandidates_AppliesAgeReplyAndRecentRepostFilters(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC().Unix()

	mustInsertEventRow(t, pool, "recycle_root", "author_z", now-(50*24*60*60), 1)
	mustInsertEventRow(t, pool, "recycle_reply", "author_z", now-(45*24*60*60), 1)
	mustInsertEventRow(t, pool, "recycle_recent", "author_z", now-(10*24*60*60), 1)
	mustInsertEventRow(t, pool, "other_parent", "author_o", now-(60*24*60*60), 1)
	mustInsertThreadEdge(t, pool, "recycle_reply", "other_parent", "other_parent", now-(45*24*60*60))

	mustInsertEventRow(t, pool, "root_reply_1", "other_1", now-(49*24*60*60), 1)
	mustInsertEventRow(t, pool, "root_reply_2", "other_2", now-(48*24*60*60), 1)
	mustInsertReplyContribution(t, pool, "root_reply_1", "recycle_root")
	mustInsertReplyContribution(t, pool, "root_reply_2", "recycle_root")

	mustInsertEventRow(t, pool, "reply_reply_1", "other_3", now-(44*24*60*60), 1)
	mustInsertReplyContribution(t, pool, "reply_reply_1", "recycle_reply")

	mustInsertRepostMarker(t, pool, "repost_marker_1", "someone", "recycle_root", now-(2*24*60*60))

	rows, err := pgStore.GetAuthorRecycleCandidates(ctx, "author_z", 90, 30, 0, false, true, 30, 20)
	if err != nil {
		t.Fatalf("GetAuthorRecycleCandidates: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected all candidates filtered out, got %#v", rows)
	}
}

func TestGetAuthorRecycleCandidates_RanksByWeightedEngagement(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC().Unix()

	mustInsertEventRow(t, pool, "cand_high", "author_rank", now-(55*24*60*60), 1)
	mustInsertEventRow(t, pool, "cand_mid", "author_rank", now-(50*24*60*60), 1)
	mustInsertEventRow(t, pool, "cand_low", "author_rank", now-(48*24*60*60), 1)

	mustInsertEventRow(t, pool, "h_reply_1", "other_1", now-(54*24*60*60), 1)
	mustInsertEventRow(t, pool, "h_reply_2", "other_2", now-(53*24*60*60), 1)
	mustInsertEventRow(t, pool, "m_reply_1", "other_3", now-(49*24*60*60), 1)
	mustInsertReplyContribution(t, pool, "h_reply_1", "cand_high")
	mustInsertReplyContribution(t, pool, "h_reply_2", "cand_high")
	mustInsertReplyContribution(t, pool, "m_reply_1", "cand_mid")

	rows, err := pgStore.GetAuthorRecycleCandidates(ctx, "author_rank", 90, 30, 50, false, false, 30, 20)
	if err != nil {
		t.Fatalf("GetAuthorRecycleCandidates: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 candidates with percentile filter, got %d (%#v)", len(rows), rows)
	}
	if rows[0].EventID != "cand_high" || rows[1].EventID != "cand_mid" {
		t.Fatalf("unexpected candidate ordering: %#v", rows)
	}
	if rows[0].WeightedEngagement <= rows[1].WeightedEngagement {
		t.Fatalf("expected descending weighted engagement ordering: %#v", rows)
	}
	if rows[0].PerformancePercentile < rows[1].PerformancePercentile {
		t.Fatalf("expected percentile ordering to follow rank: %#v", rows)
	}
}

func mustInsertEventRow(t *testing.T, pool *pgxpool.Pool, id, pubkey string, createdAt int64, kind int) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       kind,
		"tags":       [][]string{},
		"content":    "analytics test",
		"sig":        "sig_" + id,
	})
	if err != nil {
		t.Fatalf("marshal event raw json: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
	`, id, pubkey, createdAt, kind, "sig_"+id, "analytics test", string(raw)); err != nil {
		t.Fatalf("insert event %s: %v", id, err)
	}
}

func mustInsertReplyContribution(t *testing.T, pool *pgxpool.Pool, sourceID, targetID string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO reply_count_contributions (source_event_id, target_event_id, derivation_version)
		VALUES ($1, $2, 1)
	`, sourceID, targetID); err != nil {
		t.Fatalf("insert reply contribution %s->%s: %v", sourceID, targetID, err)
	}
}

func mustInsertReactionEvent(t *testing.T, pool *pgxpool.Pool, eventID, targetID, reactor string, createdAt int64) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO reaction_events (
			event_id, target_event_id, reactor_pubkey, content, created_at, derivation_version
		)
		VALUES ($1, $2, $3, '+', $4, 1)
	`, eventID, targetID, reactor, createdAt); err != nil {
		t.Fatalf("insert reaction event %s: %v", eventID, err)
	}
}

func mustInsertEventHashtag(t *testing.T, pool *pgxpool.Pool, eventID, pubkey string, createdAt int64, hashtag string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO event_hashtags (event_id, author_pubkey, created_at, hashtag, derivation_version)
		VALUES ($1, $2, $3, $4, 1)
	`, eventID, pubkey, createdAt, hashtag); err != nil {
		t.Fatalf("insert event hashtag for %s: %v", eventID, err)
	}
}

func mustInsertEventTag(t *testing.T, pool *pgxpool.Pool, eventID, tagName string, tagIndex int, valueIndex int, value string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO event_tags (event_id, tag_name, tag_index, value_index, value, raw_values)
		VALUES ($1, $2, $3, $4, $5, '[]'::jsonb)
	`, eventID, tagName, tagIndex, valueIndex, value); err != nil {
		t.Fatalf("insert event tag for %s: %v", eventID, err)
	}
}

func mustInsertThreadEdge(
	t *testing.T,
	pool *pgxpool.Pool,
	childEventID string,
	parentEventID string,
	rootEventID string,
	childCreatedAt int64,
) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO thread_edges (
			child_event_id, child_created_at, parent_event_id, root_event_id, parent_missing, root_missing, derivation_version
		)
		VALUES ($1, $2, $3, $4, FALSE, FALSE, 1)
	`, childEventID, childCreatedAt, parentEventID, rootEventID); err != nil {
		t.Fatalf("insert thread edge for %s: %v", childEventID, err)
	}
}

func mustInsertRepostMarker(
	t *testing.T,
	pool *pgxpool.Pool,
	eventID string,
	reposterPubkey string,
	targetEventID string,
	createdAt int64,
) {
	t.Helper()
	mustInsertEventRow(t, pool, eventID, reposterPubkey, createdAt, 6)
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO repost_events (event_id, target_event_id, reposter_pubkey, quote, created_at, derivation_version)
		VALUES ($1, $2, $3, '', $4, 1)
	`, eventID, targetEventID, reposterPubkey, createdAt); err != nil {
		t.Fatalf("insert repost marker %s: %v", eventID, err)
	}
}

func mustInsertRepostEventWithQuote(
	t *testing.T,
	pool *pgxpool.Pool,
	eventID string,
	reposterPubkey string,
	targetEventID string,
	quote string,
	createdAt int64,
) {
	t.Helper()
	mustInsertEventRow(t, pool, eventID, reposterPubkey, createdAt, 6)
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO repost_events (event_id, target_event_id, reposter_pubkey, quote, created_at, derivation_version)
		VALUES ($1, $2, $3, $4, $5, 1)
	`, eventID, targetEventID, reposterPubkey, quote, createdAt); err != nil {
		t.Fatalf("insert repost event %s: %v", eventID, err)
	}
}
