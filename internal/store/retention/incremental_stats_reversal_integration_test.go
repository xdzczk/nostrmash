package retention_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/retention"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func boolPtr(v bool) *bool { return &v }

// newDerivedEvent builds a minimal-but-complete model.Event with a matching
// raw_json payload, suitable for pgStore.InsertCanonicalEvent +
// handlers.DeriveEventBundle (unlike the package's raw-SQL insertEvent
// helper, which stubs raw_json as '{}' and is too bare for derivation).
func newDerivedEvent(id, pubkey string, kind int, createdAt int64, tags [][]string, content string, firstSeenAt time.Time) model.Event {
	payload := map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       kind,
		"tags":       tags,
		"content":    content,
		"sig":        "sig_" + id,
	}
	raw, _ := json.Marshal(payload)
	return model.Event{
		ID:          id,
		Pubkey:      pubkey,
		CreatedAt:   createdAt,
		Kind:        kind,
		Sig:         "sig_" + id,
		Content:     content,
		RawJSON:     raw,
		FirstSeenAt: firstSeenAt,
		InsertedAt:  firstSeenAt,
	}
}

// TestPurgeUntrustedAuthorEvents_ReversesIncrementalStats is the end-to-end
// regression test for the retention decrement path: an untrusted author's
// note contributes O(1) incremental deltas on ingest, then gets hard-deleted
// by PurgeUntrustedAuthorEvents once the trust graph excludes them. Without
// IncrementalStatsReverser wired in, profile_public_stats /
// author_activity_daily / author_hashtag_daily / author_media_daily /
// author_hourly_activity would silently retain the deleted note's counts
// forever (upward drift). With it wired in, the purge must leave those
// counters exactly as if the note had never existed.
func TestPurgeUntrustedAuthorEvents_ReversesIncrementalStats(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "retention-reversal"), "retention_reversal")
	derivationbootstrap.MustMigrate(t, ctx, pool, "retention-reversal")

	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		IncrementalProfilePublicStats:  boolPtr(true),
		IncrementalAuthorActivityDaily: boolPtr(true),
		IncrementalWindowedRollups:     boolPtr(true),
	})
	pgStore := store.NewPostgresStore(pool)
	s := retention.New(pool)
	s.SetIncrementalStatsReverser(handlers)

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	oldUnix := ref.Add(-30 * 24 * time.Hour).Unix()
	oldSeen := ref.Add(-30 * 24 * time.Hour)

	// Trust graph must be non-empty (fail-safe) but must not contain the
	// untrusted author.
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed)
		VALUES ('trusted_pub', 1, false)
	`); err != nil {
		t.Fatalf("insert trust snapshot: %v", err)
	}

	note := newDerivedEvent(
		"untrusted_note",
		"untrusted_pub",
		1,
		oldUnix,
		[][]string{{"t", "ai"}, {"image", "https://cdn/img.png"}},
		"hello",
		oldSeen,
	)
	if err := pgStore.InsertCanonicalEvent(ctx, note, [][]string{{"t", "ai"}, {"image", "https://cdn/img.png"}}, "wss://relay.one", oldSeen); err != nil {
		t.Fatalf("insert canonical event: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, note.ID); err != nil {
		t.Fatalf("derive event bundle: %v", err)
	}
	// InsertCanonicalEvent enqueues a derive_event_bundle job; since this
	// test drives derivation directly (not via the job runner), that job
	// would otherwise sit "pending" forever and block every purge's
	// in-flight-derivation guard. Clear it to simulate the job having
	// completed normally.
	if _, err := pool.Exec(ctx, `DELETE FROM jobs WHERE idempotency_key = $1`, "derive_event_bundle:"+note.ID); err != nil {
		t.Fatalf("clear derive_event_bundle job: %v", err)
	}

	// Sanity: forward deltas landed before the purge. Note that
	// author_hashtag_daily stays at 0 throughout: the write-time trust gate
	// (authorOutsideTrustGraph) never writes event_hashtags for an author
	// outside trust_graph_snapshot in the first place, so there is nothing
	// to claim or reverse for that projection here. note_discovery_stats
	// (which feeds author_media_daily) and profile_public_stats /
	// author_activity_daily / author_hourly_activity are NOT trust-gated,
	// so they still pick up the untrusted author's own note.
	assertRetentionScalar(t, ctx, pool, `SELECT note_count FROM profile_public_stats WHERE pubkey='untrusted_pub'`, 1)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(post_count),0) FROM author_activity_daily WHERE pubkey='untrusted_pub'`, 1)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(usage_count),0) FROM author_hashtag_daily WHERE pubkey='untrusted_pub' AND hashtag='ai'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(total_posts),0) FROM author_media_daily WHERE pubkey='untrusted_pub'`, 1)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(post_count),0) FROM author_hourly_activity WHERE pubkey='untrusted_pub'`, 1)
	assertRetentionScalar(t, ctx, pool, `SELECT COUNT(*) FROM applied_stat_deltas WHERE event_id='untrusted_note'`, 4)

	deleted, err := s.PurgeUntrustedAuthorEvents(ctx, ref, ref.Add(-7*24*time.Hour), 100)
	if err != nil {
		t.Fatalf("purge untrusted author events: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deletion, got %d", deleted)
	}

	assertRetentionScalar(t, ctx, pool, `SELECT COUNT(*) FROM events WHERE id='untrusted_note'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT note_count FROM profile_public_stats WHERE pubkey='untrusted_pub'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(post_count),0) FROM author_activity_daily WHERE pubkey='untrusted_pub'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(usage_count),0) FROM author_hashtag_daily WHERE pubkey='untrusted_pub' AND hashtag='ai'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(total_posts),0) FROM author_media_daily WHERE pubkey='untrusted_pub'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(post_count),0) FROM author_hourly_activity WHERE pubkey='untrusted_pub'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT COUNT(*) FROM applied_stat_deltas WHERE event_id='untrusted_note'`, 0)
}

// TestPurgeExpiredEngagementEvents_ReversesIncrementalStats covers the other
// hard-delete purge that can carry incremental author-stat deltas: an old
// reaction (kind=7) gives the reactor an engagement_given credit and the
// target note's author an engagement_received credit. Once the reaction
// ages past the engagement-retention horizon it is hard-deleted, and that
// credit must be reversed on both sides.
func TestPurgeExpiredEngagementEvents_ReversesIncrementalStats(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "retention-reversal-engagement"), "retention_reversal_engagement")
	derivationbootstrap.MustMigrate(t, ctx, pool, "retention-reversal-engagement")

	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		IncrementalProfilePublicStats:  boolPtr(true),
		IncrementalAuthorActivityDaily: boolPtr(true),
		IncrementalWindowedRollups:     boolPtr(true),
	})
	pgStore := store.NewPostgresStore(pool)
	s := retention.New(pool)
	s.SetIncrementalStatsReverser(handlers)

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	oldUnix := ref.Add(-30 * 24 * time.Hour).Unix()
	oldSeen := ref.Add(-30 * 24 * time.Hour)

	targetNote := newDerivedEvent("engage_target_note", "alice", 1, oldUnix, nil, "hi", oldSeen)
	if err := pgStore.InsertCanonicalEvent(ctx, targetNote, nil, "wss://relay.one", oldSeen); err != nil {
		t.Fatalf("insert target note: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, targetNote.ID); err != nil {
		t.Fatalf("derive target note: %v", err)
	}

	reaction := newDerivedEvent("engage_reaction", "bob", 7, oldUnix+60, [][]string{{"e", "engage_target_note"}}, "+", oldSeen)
	if err := pgStore.InsertCanonicalEvent(ctx, reaction, [][]string{{"e", "engage_target_note"}}, "wss://relay.one", oldSeen); err != nil {
		t.Fatalf("insert reaction: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, reaction.ID); err != nil {
		t.Fatalf("derive reaction: %v", err)
	}
	for _, id := range []string{targetNote.ID, reaction.ID} {
		if _, err := pool.Exec(ctx, `DELETE FROM jobs WHERE idempotency_key = $1`, "derive_event_bundle:"+id); err != nil {
			t.Fatalf("clear derive_event_bundle job for %s: %v", id, err)
		}
	}

	// Sanity: forward deltas landed before the purge.
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(engagement_given),0) FROM author_activity_daily WHERE pubkey='bob'`, 1)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(engagement_received),0) FROM author_activity_daily WHERE pubkey='alice'`, 1)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(reaction_received),0) FROM author_hourly_activity WHERE pubkey='alice'`, 1)
	assertRetentionScalar(t, ctx, pool, `SELECT COUNT(*) FROM applied_stat_deltas WHERE event_id='engage_reaction'`, 2)

	deleted, err := s.PurgeExpiredEngagementEvents(ctx, ref, ref.Add(-7*24*time.Hour), 100)
	if err != nil {
		t.Fatalf("purge expired engagement events: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deletion, got %d", deleted)
	}

	assertRetentionScalar(t, ctx, pool, `SELECT COUNT(*) FROM events WHERE id='engage_reaction'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(engagement_given),0) FROM author_activity_daily WHERE pubkey='bob'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(engagement_received),0) FROM author_activity_daily WHERE pubkey='alice'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT COALESCE(SUM(reaction_received),0) FROM author_hourly_activity WHERE pubkey='alice'`, 0)
	assertRetentionScalar(t, ctx, pool, `SELECT COUNT(*) FROM applied_stat_deltas WHERE event_id='engage_reaction'`, 0)

	// The target note itself was untouched by this purge (only its
	// engagement was hard-deleted), so its own counters must be unaffected.
	assertRetentionScalar(t, ctx, pool, `SELECT note_count FROM profile_public_stats WHERE pubkey='alice'`, 1)
}

func assertRetentionScalar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, want int64) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(ctx, query).Scan(&got); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("query %q: got %d want %d", query, got, want)
	}
}
