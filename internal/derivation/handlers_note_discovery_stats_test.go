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

// A single account emitting many engagement events (or the author engaging
// with their own note) must not buy trending score: score inputs count each
// engager pubkey once per signal and exclude the author, while the raw
// display counters keep counting events.
func TestProjectNoteDiscoveryStats_ScoreDedupesEngagersAndExcludesAuthor(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()
	noteCreatedAt := now.Add(-1 * time.Hour)

	seed := []model.Event{
		newEventForTest("farm_note", "farm_author", noteCreatedAt.Unix(), 1, nil, "farmed note", noteCreatedAt),
		newEventForTest("honest_note", "honest_author", noteCreatedAt.Unix(), 1, nil, "honest note", noteCreatedAt),
		newEventForTest("self_note", "self_author", noteCreatedAt.Unix(), 1, nil, "self promoted note", noteCreatedAt),
		// One bot reacts to farm_note three times.
		newEventForTest("farm_react_1", "farm_bot", now.Add(-50*time.Minute).Unix(), 7, [][]string{{"e", "farm_note"}}, "+", now.Add(-50*time.Minute)),
		newEventForTest("farm_react_2", "farm_bot", now.Add(-49*time.Minute).Unix(), 7, [][]string{{"e", "farm_note"}}, "+", now.Add(-49*time.Minute)),
		newEventForTest("farm_react_3", "farm_bot", now.Add(-48*time.Minute).Unix(), 7, [][]string{{"e", "farm_note"}}, "+", now.Add(-48*time.Minute)),
		// One genuine account reacts to honest_note once.
		newEventForTest("honest_react", "honest_fan", now.Add(-50*time.Minute).Unix(), 7, [][]string{{"e", "honest_note"}}, "+", now.Add(-50*time.Minute)),
		// self_note gets engagement only from its own author: reaction plus a
		// large self-zap.
		newEventForTest("self_react", "self_author", now.Add(-50*time.Minute).Unix(), 7, [][]string{{"e", "self_note"}}, "+", now.Add(-50*time.Minute)),
		newEventForTest("self_zap", "self_author", now.Add(-45*time.Minute).Unix(), 9735, [][]string{{"e", "self_note"}, {"p", "self_author"}, {"amount", "100000000"}}, "", now.Add(-45*time.Minute)),
	}
	for _, event := range seed {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert seed event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive seed event bundle %s: %v", event.ID, err)
		}
	}

	farm := readNoteDiscoveryStatsRow(t, ctx, pool, "farm_note")
	honest := readNoteDiscoveryStatsRow(t, ctx, pool, "honest_note")
	selfPromoted := readNoteDiscoveryStatsRow(t, ctx, pool, "self_note")

	// Display counters keep counting raw events.
	if farm.ReactionCount != 3 {
		t.Fatalf("expected raw display reaction_count 3 for farmed note, got %d", farm.ReactionCount)
	}
	if selfPromoted.ReactionCount != 1 || selfPromoted.ZapCount != 1 {
		t.Fatalf("expected raw self-engagement display counters, got %#v", selfPromoted)
	}

	// Score inputs are deduplicated: 3 reactions from one pubkey are worth the
	// same as 1 reaction from one pubkey. The two projections run milliseconds
	// apart with identical note created_at, so scores match within decay jitter.
	if honest.Score24h <= 0 {
		t.Fatalf("expected positive honest note score, got %f", honest.Score24h)
	}
	if diff := (farm.Score24h - honest.Score24h) / honest.Score24h; diff > 0.02 || diff < -0.02 {
		t.Fatalf("expected deduped farm score ~= honest score, got farm=%f honest=%f", farm.Score24h, honest.Score24h)
	}

	// Self-engagement (including self-zaps) buys exactly zero score.
	if selfPromoted.Score24h != 0 || selfPromoted.Score7d != 0 {
		t.Fatalf("expected zero score for self-engagement-only note, got %#v", selfPromoted)
	}
}

// With trust weighting enabled and untrusted weight 0, engagement from
// accounts outside the trust graph buys no trending score, and closer
// trust-graph hops weigh more than farther ones.
func TestProjectNoteDiscoveryStats_TrustWeightedEngagement(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		EngagementWeighting: derivation.EngagementWeightingOptions{
			TrustWeighted:   true,
			UntrustedWeight: 0,
			MaxHops:         3,
		},
	})
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()
	noteCreatedAt := now.Add(-1 * time.Hour)

	for _, seedRow := range []struct {
		pubkey string
		hops   int
	}{{"tw_hop1", 1}, {"tw_hop2", 2}} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO trust_pubkeys_latest (pubkey, min_hops, score, rank)
			VALUES ($1, $2, 0.5, 1)
		`, seedRow.pubkey, seedRow.hops); err != nil {
			t.Fatalf("seed trust_pubkeys_latest %s: %v", seedRow.pubkey, err)
		}
	}

	seed := []model.Event{
		newEventForTest("tw_note_bots", "tw_author_a", noteCreatedAt.Unix(), 1, nil, "bot-farmed", noteCreatedAt),
		newEventForTest("tw_note_hop1", "tw_author_b", noteCreatedAt.Unix(), 1, nil, "hop1 engaged", noteCreatedAt),
		newEventForTest("tw_note_hop2", "tw_author_c", noteCreatedAt.Unix(), 1, nil, "hop2 engaged", noteCreatedAt),
		// Untrusted bot ring engagement: reactions, repost, and a huge zap.
		newEventForTest("tw_bot_react_1", "tw_bot_1", now.Add(-50*time.Minute).Unix(), 7, [][]string{{"e", "tw_note_bots"}}, "+", now.Add(-50*time.Minute)),
		newEventForTest("tw_bot_react_2", "tw_bot_2", now.Add(-49*time.Minute).Unix(), 7, [][]string{{"e", "tw_note_bots"}}, "+", now.Add(-49*time.Minute)),
		newEventForTest("tw_bot_repost", "tw_bot_3", now.Add(-48*time.Minute).Unix(), 6, [][]string{{"e", "tw_note_bots"}}, "rp", now.Add(-48*time.Minute)),
		newEventForTest("tw_bot_zap", "tw_bot_4", now.Add(-47*time.Minute).Unix(), 9735, [][]string{{"e", "tw_note_bots"}, {"p", "tw_author_a"}, {"amount", "500000000"}}, "", now.Add(-47*time.Minute)),
		// One trusted reaction each at hops 1 and 2.
		newEventForTest("tw_hop1_react", "tw_hop1", now.Add(-50*time.Minute).Unix(), 7, [][]string{{"e", "tw_note_hop1"}}, "+", now.Add(-50*time.Minute)),
		newEventForTest("tw_hop2_react", "tw_hop2", now.Add(-50*time.Minute).Unix(), 7, [][]string{{"e", "tw_note_hop2"}}, "+", now.Add(-50*time.Minute)),
	}
	for _, event := range seed {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert seed event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive seed event bundle %s: %v", event.ID, err)
		}
	}

	bots := readNoteDiscoveryStatsRow(t, ctx, pool, "tw_note_bots")
	hop1 := readNoteDiscoveryStatsRow(t, ctx, pool, "tw_note_hop1")
	hop2 := readNoteDiscoveryStatsRow(t, ctx, pool, "tw_note_hop2")

	// Display counters still reflect raw activity.
	if bots.ReactionCount != 2 || bots.RepostCount != 1 || bots.ZapCount != 1 {
		t.Fatalf("expected raw display counters on bot-farmed note, got %#v", bots)
	}
	// Untrusted engagement (weight 0) buys exactly zero score.
	if bots.Score24h != 0 || bots.Score7d != 0 {
		t.Fatalf("expected zero score for untrusted-only engagement, got %#v", bots)
	}
	if hop1.Score24h <= 0 || hop2.Score24h <= 0 {
		t.Fatalf("expected positive trusted scores, got hop1=%f hop2=%f", hop1.Score24h, hop2.Score24h)
	}
	// hops=2 weighs 0.5 relative to hops<=1 (same note age, decay jitter only).
	ratio := hop2.Score24h / hop1.Score24h
	if ratio < 0.45 || ratio > 0.55 {
		t.Fatalf("expected hop2/hop1 score ratio ~0.5, got %f (hop1=%f hop2=%f)", ratio, hop1.Score24h, hop2.Score24h)
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
