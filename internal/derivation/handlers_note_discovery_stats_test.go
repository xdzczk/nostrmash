package derivation_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectNoteDiscoveryStats_CountAccumulationAndScoreUpdate(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	seed := []model.Event{
		newEventForTest("note_target", "author_target", now.Add(-2*time.Hour).Unix(), 1, nil, "target note", now.Add(-2*time.Hour)),
		newEventForTest("reply_1", "author_reply", now.Add(-90*time.Minute).Unix(), 1, [][]string{{"e", "note_target", "", "reply"}}, "reply", now.Add(-90*time.Minute)),
		newEventForTest("reaction_1", "author_react", now.Add(-80*time.Minute).Unix(), 7, [][]string{{"e", "note_target"}}, "+", now.Add(-80*time.Minute)),
		newEventForTest("repost_1", "author_repost", now.Add(-70*time.Minute).Unix(), 6, [][]string{{"e", "note_target"}}, "rp", now.Add(-70*time.Minute)),
		newEventForTest("zap_1", "author_zap", now.Add(-60*time.Minute).Unix(), 9735, [][]string{{"e", "note_target"}, {"p", "author_target"}, {"amount", "21000"}}, "", now.Add(-60*time.Minute)),
	}
	for _, event := range seed {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert seed event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive seed event bundle %s: %v", event.ID, err)
		}
	}

	row := readNoteDiscoveryStatsRow(t, ctx, pool, "note_target")
	if row.ReplyCount != 1 || row.ReactionCount != 1 || row.RepostCount != 1 || row.ZapCount != 1 {
		t.Fatalf("unexpected accumulated counts: %#v", row)
	}
	if row.ZapMSats != 21000 {
		t.Fatalf("unexpected zap_msats: got=%d want=21000", row.ZapMSats)
	}
	if row.Score24h <= 0 || row.Score7d <= 0 {
		t.Fatalf("expected positive scores, got: score_24h=%f score_7d=%f", row.Score24h, row.Score7d)
	}

	reaction2 := newEventForTest("reaction_2", "author_react2", now.Add(-10*time.Minute).Unix(), 7, [][]string{{"e", "note_target"}}, "++", now.Add(-10*time.Minute))
	if err := pgStore.InsertCanonicalEvent(ctx, reaction2, extractTagsFromRaw(t, reaction2.RawJSON), "wss://relay.one", reaction2.FirstSeenAt); err != nil {
		t.Fatalf("insert second reaction: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, reaction2.ID); err != nil {
		t.Fatalf("derive second reaction bundle: %v", err)
	}

	updated := readNoteDiscoveryStatsRow(t, ctx, pool, "note_target")
	if updated.ReactionCount != 2 {
		t.Fatalf("unexpected reaction_count after update: got=%d want=2", updated.ReactionCount)
	}
	if updated.Score24h <= row.Score24h {
		t.Fatalf("expected score_24h to increase after new interaction: old=%f new=%f", row.Score24h, updated.Score24h)
	}
}

func TestProjectNoteDiscoveryStats_DerivesMediaFlags(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	note := newEventForTest(
		"note_media_target",
		"author_media",
		now.Add(-45*time.Minute).Unix(),
		1,
		[][]string{
			{"image", "https://cdn.example/photo.jpg"},
			{"video", "https://cdn.example/video.mp4"},
			{"r", "https://example.com/article"},
		},
		"see https://example.com/preview.webp and https://example.com/watch.mp4",
		now.Add(-45*time.Minute),
	)
	longform := newEventForTest(
		"note_media_article",
		"author_media",
		now.Add(-30*time.Minute).Unix(),
		30023,
		nil,
		strings.Repeat("x", 1300),
		now.Add(-30*time.Minute),
	)

	for _, event := range []model.Event{note, longform} {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	row := readNoteDiscoveryStatsRow(t, ctx, pool, "note_media_target")
	if !row.HasImage || !row.HasVideo || !row.HasLink {
		t.Fatalf("expected image/video/link media flags: %#v", row)
	}
	if row.HasArticle {
		t.Fatalf("did not expect article flag on regular note: %#v", row)
	}
	if row.AttachmentCount < 3 {
		t.Fatalf("expected attachment count >= 3, got %d", row.AttachmentCount)
	}

	articleRow := readNoteDiscoveryStatsRow(t, ctx, pool, "note_media_article")
	if !articleRow.HasArticle {
		t.Fatalf("expected article flag for longform note: %#v", articleRow)
	}
}

func TestProjectionRebuildScopes_NoteDiscoveryStatsFullRebuild(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	note := newEventForTest("note_rebuild", "author_rebuild", time.Now().UTC().Add(-1*time.Hour).Unix(), 1, nil, "rebuild me", time.Now().UTC().Add(-1*time.Hour))
	if err := pgStore.InsertCanonicalEvent(ctx, note, extractTagsFromRaw(t, note.RawJSON), "wss://relay.one", note.FirstSeenAt); err != nil {
		t.Fatalf("insert note: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, note.ID); err != nil {
		t.Fatalf("derive note bundle: %v", err)
	}

	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationNoteDiscoveryStats,
		TargetVersion:  derivation.NoteDiscoveryStatsVersion,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger note discovery rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err != nil {
		t.Fatalf("execute note discovery rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationNoteDiscoveryStats, derivation.NoteDiscoveryStatsVersion, derivation.NoteDiscoveryStatsVersion)

	var version int
	if err := pool.QueryRow(ctx, `SELECT derivation_version FROM note_discovery_stats WHERE event_id = $1`, note.ID).Scan(&version); err != nil {
		t.Fatalf("query note discovery derivation version: %v", err)
	}
	if version != derivation.NoteDiscoveryStatsVersion {
		t.Fatalf("unexpected note discovery derivation version: got=%d want=%d", version, derivation.NoteDiscoveryStatsVersion)
	}
}

func TestProjectNoteDiscoveryStats_UsesThreadWideReplyCount(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	root := newEventForTest("thread_root", "author_root", now.Add(-3*time.Hour).Unix(), 1, nil, "root", now.Add(-3*time.Hour))
	direct := newEventForTest(
		"thread_direct",
		"author_direct",
		now.Add(-2*time.Hour).Unix(),
		1,
		[][]string{{"e", "thread_root", "", "root"}, {"e", "thread_root", "", "reply"}},
		"direct reply",
		now.Add(-2*time.Hour),
	)
	nested := newEventForTest(
		"thread_nested",
		"author_nested",
		now.Add(-1*time.Hour).Unix(),
		1,
		[][]string{{"e", "thread_root", "", "root"}, {"e", "thread_direct", "", "reply"}},
		"nested reply",
		now.Add(-1*time.Hour),
	)
	for _, event := range []model.Event{root, direct, nested} {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	row := readNoteDiscoveryStatsRow(t, ctx, pool, "thread_root")
	if row.ReplyCount != 2 {
		t.Fatalf("discover reply_count should be thread-wide: got=%d want=2", row.ReplyCount)
	}
	counts, err := pgStore.GetEventCounts(ctx, "thread_root")
	if err != nil {
		t.Fatalf("get event counts: %v", err)
	}
	if counts.ReplyCount != 2 {
		t.Fatalf("event counts reply_count should be thread-wide: got=%d want=2", counts.ReplyCount)
	}
}

func TestProjectNoteDiscoveryStats_LanguageClassificationFixtures(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_ENABLED", "true")
	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_MIN_CHARS", "10")
	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_MIN_CONFIDENCE", "0.55")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	fixtures := []struct {
		id       string
		content  string
		expected string
	}{
		{id: "lang_note_en", content: "This is a simple note about nostr and how the network grows.", expected: "en"},
		{id: "lang_note_es", content: "Hola amigos, este es un mensaje para la comunidad de nostr.", expected: "es"},
		{id: "lang_note_ja", content: "こんにちは、これはノストルの言語判定テストです。", expected: "ja"},
		{id: "lang_note_ko", content: "안녕하세요, 이것은 노스트르 언어 감지 테스트입니다.", expected: "ko"},
		{id: "lang_note_ar", content: "مرحبا، هذا اختبار لاكتشاف اللغة في نوستر.", expected: "ar"},
		{id: "lang_note_hi", content: "नमस्ते, यह नोस्ट्र में भाषा पहचान का परीक्षण है।", expected: "hi"},
		{id: "lang_note_th", content: "สวัสดี นี่คือการทดสอบการตรวจจับภาษาในโนสตร", expected: "th"},
		{id: "lang_note_bn", content: "হ্যালো, এটি নস্টরে ভাষা শনাক্তকরণের একটি পরীক্ষা।", expected: "bn"},
		{id: "lang_note_id", content: "Halo, ini adalah catatan untuk pengujian bahasa dengan komunitas nostr.", expected: "id"},
		{id: "lang_note_tr", content: "Merhaba bu bir not ve bu test icin yazildi, nasil gidiyor.", expected: "tr"},
		{id: "lang_note_it", content: "Ciao, questo e un messaggio con parole italiane per il test lingua.", expected: "it"},
	}
	for idx, fixture := range fixtures {
		ts := now.Add(-time.Duration(idx+1) * time.Minute)
		note := newEventForTest(fixture.id, "author_"+fixture.id, ts.Unix(), 1, nil, fixture.content, ts)
		if err := pgStore.InsertCanonicalEvent(ctx, note, extractTagsFromRaw(t, note.RawJSON), "wss://relay.one", note.FirstSeenAt); err != nil {
			t.Fatalf("insert note %s: %v", fixture.id, err)
		}
		if err := handlers.DeriveEventBundle(ctx, note.ID); err != nil {
			t.Fatalf("derive note %s: %v", fixture.id, err)
		}
		row := readNoteDiscoveryStatsRow(t, ctx, pool, note.ID)
		if row.PrimaryLanguage == nil || *row.PrimaryLanguage != fixture.expected {
			t.Fatalf("unexpected language for %s: got=%v want=%s", fixture.id, row.PrimaryLanguage, fixture.expected)
		}
		if row.LanguageConf == nil || *row.LanguageConf <= 0 {
			t.Fatalf("expected language confidence for %s, got=%v", fixture.id, row.LanguageConf)
		}
	}
}

func TestProjectNoteDiscoveryStats_LanguageUnknownShortAndDisabled(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_ENABLED", "true")
	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_MIN_CHARS", "20")

	short := newEventForTest("lang_short_note", "author_short", now.Add(-2*time.Minute).Unix(), 1, nil, "gm", now.Add(-2*time.Minute))
	if err := pgStore.InsertCanonicalEvent(ctx, short, extractTagsFromRaw(t, short.RawJSON), "wss://relay.one", short.FirstSeenAt); err != nil {
		t.Fatalf("insert short note: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, short.ID); err != nil {
		t.Fatalf("derive short note: %v", err)
	}
	shortRow := readNoteDiscoveryStatsRow(t, ctx, pool, short.ID)
	if shortRow.PrimaryLanguage != nil || shortRow.LanguageConf != nil {
		t.Fatalf("expected unknown language for short text, got=%#v", shortRow)
	}

	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_ENABLED", "false")
	disabled := newEventForTest("lang_disabled_note", "author_disabled", now.Add(-time.Minute).Unix(), 1, nil, "This sentence is long enough for detection.", now.Add(-time.Minute))
	if err := pgStore.InsertCanonicalEvent(ctx, disabled, extractTagsFromRaw(t, disabled.RawJSON), "wss://relay.one", disabled.FirstSeenAt); err != nil {
		t.Fatalf("insert disabled note: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, disabled.ID); err != nil {
		t.Fatalf("derive disabled note: %v", err)
	}
	disabledRow := readNoteDiscoveryStatsRow(t, ctx, pool, disabled.ID)
	if disabledRow.PrimaryLanguage != nil || disabledRow.LanguageConf != nil {
		t.Fatalf("expected nil language when detection disabled, got=%#v", disabledRow)
	}
}

type noteDiscoveryRow struct {
	ReplyCount      int64
	RepostCount     int64
	ReactionCount   int64
	ZapCount        int64
	ZapMSats        int64
	HasImage        bool
	HasVideo        bool
	HasLink         bool
	HasArticle      bool
	AttachmentCount int
	PrimaryLanguage *string
	LanguageConf    *float64
	Score24h        float64
	Score7d         float64
}

func readNoteDiscoveryStatsRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string) noteDiscoveryRow {
	t.Helper()
	var out noteDiscoveryRow
	if err := pool.QueryRow(ctx, `
		SELECT
			reply_count,
			repost_count,
			reaction_count,
			zap_count,
			zap_msats,
			has_image,
			has_video,
			has_link,
			has_article,
			attachment_count,
			primary_language,
			language_confidence,
			score_24h,
			score_7d
		FROM note_discovery_stats
		WHERE event_id = $1
	`, eventID).Scan(
		&out.ReplyCount,
		&out.RepostCount,
		&out.ReactionCount,
		&out.ZapCount,
		&out.ZapMSats,
		&out.HasImage,
		&out.HasVideo,
		&out.HasLink,
		&out.HasArticle,
		&out.AttachmentCount,
		&out.PrimaryLanguage,
		&out.LanguageConf,
		&out.Score24h,
		&out.Score7d,
	); err != nil {
		t.Fatalf("query note discovery row %s: %v", eventID, err)
	}
	return out
}
