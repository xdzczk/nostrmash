package read

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestPersonalizedCohortAggregates(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "personalized_cohort"), "personalized_cohort")
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	reader := New(pool)

	viewer := strings.Repeat("a", 64)
	trusted := strings.Repeat("b", 64)
	plain := strings.Repeat("c", 64)
	author := strings.Repeat("d", 64)
	outsider := strings.Repeat("e", 64)
	receiver := strings.Repeat("f", 64)
	now := time.Now().UTC()
	recent := now.Add(-time.Hour).Unix()
	stale := now.Add(-48 * time.Hour).Unix()

	insertEvent := func(id, pubkey string, createdAt int64, kind int) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
			VALUES ($1, $2, $3, $4, 'sig', '', '{}'::jsonb)
		`, id, pubkey, createdAt, kind); err != nil {
			t.Fatalf("insert event %s: %v", id, err)
		}
	}
	for _, row := range []struct {
		id, pubkey string
		kind       int
	}{
		{"contact", viewer, 3},
		{"meta_author", author, 0},
		{"meta_receiver", receiver, 0},
		{"note", author, 1},
		{"list_follow", trusted, 10003},
		{"list_author", author, 10003},
		{"list_out", outsider, 10003},
		{"react_follow", trusted, 7},
		{"react_author", author, 7},
		{"react_out", outsider, 7},
		{"zap_recent", trusted, 9735},
		{"zap_old", plain, 9735},
		{"zap_self", author, 9735},
		{"zap_support", trusted, 9735},
		{"zap_support_2", plain, 9735},
		{"zap_out", outsider, 9735},
		{"tag_kept", trusted, 1},
		{"tag_old", trusted, 1},
		{"tag_out", outsider, 1},
	} {
		at := recent
		if row.id == "tag_old" || row.id == "zap_old" {
			at = stale
		}
		insertEvent(row.id, row.pubkey, at, row.kind)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_pubkeys_latest (pubkey, min_hops, score, rank)
		VALUES ($1, 1, 0.9, 1)
	`, trusted); err != nil {
		t.Fatalf("insert trust rank: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO follower_edges (followed_pubkey, follower_pubkey, source_event_id, contact_list_created_at, derivation_version)
		VALUES ($1, $3, 'contact', $4, 1), ($2, $3, 'contact', $4, 1)
	`, plain, trusted, viewer, recent); err != nil {
		t.Fatalf("insert follows: %v", err)
	}
	followed, err := reader.ListFollowedPubkeys(ctx, viewer, 10)
	if err != nil {
		t.Fatalf("list follows: %v", err)
	}
	if len(followed) != 2 || followed[0] != trusted || followed[1] != plain {
		t.Fatalf("followed order = %#v", followed)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO note_discovery_stats (event_id, author_pubkey, created_at, derivation_version, score_24h)
		VALUES ('note', $1, $2, 1, 3)
	`, author, recent); err != nil {
		t.Fatalf("insert note stats: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO reaction_events (event_id, target_event_id, reactor_pubkey, content, created_at, derivation_version, target_pubkey)
		VALUES
			('react_follow', 'note', $1, '+', $3, 1, $2),
			('react_author', 'note', $2, '+', $3, 1, $2),
			('react_out', 'note', $4, '+', $3, 1, $2)
	`, trusted, author, recent, outsider); err != nil {
		t.Fatalf("insert reactions: %v", err)
	}
	notes, err := reader.GetCohortTrendingNotes(ctx, []string{trusted, plain}, []int{1}, 24*time.Hour, "", 20)
	if err != nil {
		t.Fatalf("cohort notes: %v", err)
	}
	if len(notes) != 1 || notes[0].Note.EventID != "note" || notes[0].ReactionWeight != 1 {
		t.Fatalf("cohort notes = %+v", notes)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO profiles_latest (pubkey, metadata_event_id, metadata_created_at, derivation_version)
		VALUES ($1, 'meta_author', $2, 1)
	`, author, recent); err != nil {
		t.Fatalf("insert profile: %v", err)
	}
	trendingProfiles, err := reader.GetCohortTrendingProfiles(ctx, []string{trusted, plain}, 24*time.Hour, "", 20)
	if err != nil {
		t.Fatalf("cohort trending profiles: %v", err)
	}
	if len(trendingProfiles) != 1 || trendingProfiles[0].Profile.Pubkey != author || trendingProfiles[0].CohortScore != 1 {
		t.Fatalf("trending profiles = %+v", trendingProfiles)
	}
	if _, err := pool.Exec(ctx, `UPDATE author_feed_index_state SET complete = true WHERE id`); err != nil {
		t.Fatalf("mark author feed complete: %v", err)
	}
	unindexed, err := reader.GetCohortTrendingProfiles(ctx, []string{trusted, plain}, 24*time.Hour, "", 20)
	if err != nil {
		t.Fatalf("cohort trending profiles from feed index: %v", err)
	}
	if len(unindexed) != 0 {
		t.Fatalf("unindexed root = %+v", unindexed)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO author_feed_events (event_id, pubkey, kind, created_at)
		VALUES ('note', $1, 1, $2)
	`, author, recent); err != nil {
		t.Fatalf("insert author feed row: %v", err)
	}
	indexed, err := reader.GetCohortTrendingProfiles(ctx, []string{trusted, plain}, 24*time.Hour, "", 20)
	if err != nil {
		t.Fatalf("cohort trending profiles indexed: %v", err)
	}
	if len(indexed) != 1 || indexed[0].Profile.Pubkey != author {
		t.Fatalf("indexed profiles = %+v", indexed)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO follower_edges (followed_pubkey, follower_pubkey, source_event_id, contact_list_created_at, derivation_version)
		VALUES ($1, $2, 'contact', $3, 1)
	`, author, trusted, recent); err != nil {
		t.Fatalf("insert cohort follow: %v", err)
	}
	followedProfiles, err := reader.GetCohortFollowedProfiles(ctx, viewer, []string{trusted, plain}, 20)
	if err != nil {
		t.Fatalf("cohort followed: %v", err)
	}
	if len(followedProfiles) != 1 || followedProfiles[0].Profile.Pubkey != author || followedProfiles[0].CohortScore != 1 || followedProfiles[0].ViewerFollows {
		t.Fatalf("followed profiles = %+v", followedProfiles)
	}
	if _, err := reader.GetCohortHotConversations(ctx, []string{trusted, plain}, 24*time.Hour, "", 20); err != nil {
		t.Fatalf("cohort conversations: %v", err)
	}
	if _, err := reader.GetCohortRisingProfiles(ctx, viewer, []string{trusted, plain}, 24*time.Hour, "", 20); err != nil {
		t.Fatalf("cohort rising: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO bookmark_edges (
			bookmarker_pubkey, target_event_id, source_kind, list_event_id, list_created_at, derivation_version
		) VALUES
			($1, 'note', 10003, 'list_follow', $4, 1),
			($2, 'note', 10003, 'list_author', $4, 1),
			($3, 'note', 10003, 'list_out', $4, 1)
	`, trusted, author, outsider, recent); err != nil {
		t.Fatalf("insert bookmarks: %v", err)
	}
	saved, err := reader.GetCohortBookmarkedNotes(ctx, []string{trusted, plain}, "", 20)
	if err != nil {
		t.Fatalf("cohort bookmarks: %v", err)
	}
	if len(saved) != 1 || saved[0].CohortScore != 1 {
		t.Fatalf("bookmarks = %+v", saved)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO zap_receipts (zap_receipt_id, created_at, event_id, sender_pubkey, receiver_pubkey, amount_sats, derivation_version)
		VALUES
			('zap_recent', $1, 'note', $2, $3, 20, 1),
			('zap_old', $4, 'note', $5, $3, 500, 1),
			('zap_self', $1, 'note', $3, $3, 900, 1),
			('zap_support', $1, NULL, $2, $6, 250000, 1),
			('zap_support_2', $1, NULL, $5, $6, 1000, 1),
			('zap_out', $1, NULL, $7, $6, 999999, 1)
	`, recent, trusted, author, stale, plain, receiver, outsider); err != nil {
		t.Fatalf("insert zaps: %v", err)
	}
	zapped, err := reader.GetCohortZappedNotes(ctx, []string{trusted, plain}, 24*time.Hour, "", 20)
	if err != nil {
		t.Fatalf("cohort zaps: %v", err)
	}
	if len(zapped) != 1 || zapped[0].CohortScore != 20 {
		t.Fatalf("zapped = %+v", zapped)
	}
	supported, err := reader.GetCohortSupportedProfiles(ctx, []string{trusted, plain}, 24*time.Hour, 100000, 20)
	if err != nil {
		t.Fatalf("cohort supported: %v", err)
	}
	var receiverScore float64
	for _, row := range supported {
		if row.Profile.Pubkey == receiver {
			receiverScore = row.CohortScore
		}
	}
	// 250000 from one follow is capped at 100000, plus 1000 from the other.
	// The note zap is support for the note author and is a separate row.
	if receiverScore != 101000 {
		t.Fatalf("supported = %+v", supported)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO event_hashtags (event_id, author_pubkey, created_at, hashtag, derivation_version)
		VALUES
			('tag_kept', $1, $2, 'kept', 1),
			('tag_old', $1, $3, 'stale', 1),
			('tag_out', $4, $2, 'other', 1)
	`, trusted, recent, stale, outsider); err != nil {
		t.Fatalf("insert hashtags: %v", err)
	}
	tags, err := reader.GetCohortTrendingHashtags(ctx, []string{trusted}, 24*time.Hour, "", 20)
	if err != nil {
		t.Fatalf("cohort hashtags: %v", err)
	}
	if len(tags) != 1 || tags[0].Hashtag != "kept" || tags[0].UniqueAuthors != 1 {
		t.Fatalf("hashtags = %+v", tags)
	}
}
