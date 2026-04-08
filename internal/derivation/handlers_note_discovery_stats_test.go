package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestProjectNoteDiscoveryStats_CountAccumulationAndScoreUpdate(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
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

func TestProjectionRebuildScopes_NoteDiscoveryStatsFullRebuild(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
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
		TargetVersion:  2,
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
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationNoteDiscoveryStats, 2, 2)

	var version int
	if err := pool.QueryRow(ctx, `SELECT derivation_version FROM note_discovery_stats WHERE event_id = $1`, note.ID).Scan(&version); err != nil {
		t.Fatalf("query note discovery derivation version: %v", err)
	}
	if version != 2 {
		t.Fatalf("unexpected note discovery derivation version: got=%d want=2", version)
	}
}

type noteDiscoveryRow struct {
	ReplyCount    int64
	RepostCount   int64
	ReactionCount int64
	ZapCount      int64
	ZapMSats      int64
	Score24h      float64
	Score7d       float64
}

func readNoteDiscoveryStatsRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string) noteDiscoveryRow {
	t.Helper()
	var out noteDiscoveryRow
	if err := pool.QueryRow(ctx, `
		SELECT reply_count, repost_count, reaction_count, zap_count, zap_msats, score_24h, score_7d
		FROM note_discovery_stats
		WHERE event_id = $1
	`, eventID).Scan(
		&out.ReplyCount,
		&out.RepostCount,
		&out.ReactionCount,
		&out.ZapCount,
		&out.ZapMSats,
		&out.Score24h,
		&out.Score7d,
	); err != nil {
		t.Fatalf("query note discovery row %s: %v", eventID, err)
	}
	return out
}
