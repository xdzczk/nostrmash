package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
)

func TestBookmarkedNotes_DistinctSaversExcludeSelfAndIncludeArticles(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	author := strings.Repeat("a", 64)
	saver := strings.Repeat("b", 64)
	noteID := strings.Repeat("1", 64)
	articleID := strings.Repeat("2", 64)
	articleAddress := "30023:" + author + ":Essay"

	insertDerived := func(eventID, pubkey string, at time.Time, kind int, tags [][]string, content string) {
		t.Helper()
		event := newDiscoveryEvent(eventID, pubkey, at, kind, tags, content)
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractDiscoveryTagsForStoreTest(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", eventID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, eventID); err != nil {
			t.Fatalf("derive %s: %v", eventID, err)
		}
	}

	insertDerived(strings.Repeat("8", 64), author, now.Add(-time.Hour), 0, nil, `{"name":"Author"}`)
	insertDerived(strings.Repeat("9", 64), saver, now.Add(-time.Hour), 0, nil, `{"name":"Saver"}`)
	insertDerived(noteID, author, now.Add(-30*time.Minute), 1, nil, "save this note")
	insertDerived(strings.Repeat("3", 64), saver, now.Add(-20*time.Minute), 10003, [][]string{
		{"e", strings.ToUpper(noteID)},
		{"e", "not-an-id"},
		{"a", articleAddress},
		{"t", "nostr"},
	}, "")
	insertDerived(strings.Repeat("4", 64), saver, now.Add(-19*time.Minute), 30003, [][]string{
		{"d", "reads"},
		{"e", noteID},
	}, "")
	insertDerived(strings.Repeat("5", 64), author, now.Add(-18*time.Minute), 10003, [][]string{
		{"e", noteID},
	}, "")

	notes, err := pgStore.GetBookmarkedNotes(ctx, "", 10, 0)
	if err != nil {
		t.Fatalf("GetBookmarkedNotes before article: %v", err)
	}
	if len(notes) != 1 || notes[0].EventID != noteID || notes[0].BookmarkCount != 1 {
		t.Fatalf("before article = %#v", notes)
	}

	// An older list must not wipe the current one.
	insertDerived(strings.Repeat("6", 64), saver, now.Add(-2*time.Hour), 10003, [][]string{
		{"e", strings.Repeat("f", 64)},
	}, "")
	notes, err = pgStore.GetBookmarkedNotes(ctx, "", 10, 0)
	if err != nil {
		t.Fatalf("GetBookmarkedNotes after stale list: %v", err)
	}
	if len(notes) != 1 || notes[0].BookmarkCount != 1 {
		t.Fatalf("stale list changed the rank: %#v", notes)
	}

	insertDerived(articleID, author, now.Add(-10*time.Minute), 30023, [][]string{
		{"d", "Essay"},
		{"title", "Saved essay"},
	}, "the essay")
	notes, err = pgStore.GetBookmarkedNotes(ctx, "", 10, 0)
	if err != nil {
		t.Fatalf("GetBookmarkedNotes with article: %v", err)
	}
	if len(notes) != 2 {
		t.Fatalf("expected note and article, got %#v", notes)
	}
	// Equal save counts break ties toward the newer event.
	if notes[0].EventID != articleID || notes[0].Kind != 30023 || notes[0].BookmarkCount != 1 {
		t.Fatalf("article rank = %#v", notes[0])
	}
	if notes[1].EventID != noteID || notes[1].BookmarkCount != 1 {
		t.Fatalf("note rank = %#v", notes[1])
	}

	insertDerived(strings.Repeat("7", 64), saver, now.Add(-5*time.Minute), 10003, [][]string{
		{"a", articleAddress},
	}, "")
	notes, err = pgStore.GetBookmarkedNotes(ctx, "", 10, 0)
	if err != nil {
		t.Fatalf("GetBookmarkedNotes after dropping the note from the main list: %v", err)
	}
	if len(notes) != 2 || notes[1].EventID != noteID || notes[1].BookmarkCount != 1 {
		t.Fatalf("bookmark set should still keep the note, got %#v", notes)
	}

	insertDerived(strings.Repeat("e", 64), saver, now.Add(-4*time.Minute), 30003, [][]string{
		{"d", "reads"},
	}, "")
	notes, err = pgStore.GetBookmarkedNotes(ctx, "", 10, 0)
	if err != nil {
		t.Fatalf("GetBookmarkedNotes after clearing the set: %v", err)
	}
	if len(notes) != 1 || notes[0].EventID != articleID {
		t.Fatalf("only the article should remain, got %#v", notes)
	}
}

func TestMostFollowedProfiles_RequiresProfileAndKeepsSmallAccounts(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	large := strings.Repeat("a", 64)
	small := strings.Repeat("b", 64)
	ghost := strings.Repeat("c", 64)
	farm := strings.Repeat("d", 64)
	for _, pubkey := range []string{large, small, farm} {
		event := newDiscoveryEvent("meta_"+pubkey[:8], pubkey, now, 0, nil, `{"name":"Person"}`)
		if err := pgStore.InsertCanonicalEvent(ctx, event, nil, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert profile %s: %v", pubkey, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive profile %s: %v", pubkey, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO profile_public_stats (pubkey, follower_count, trusted_follower_count, derivation_version)
		VALUES ($1, 50, 40, 1), ($2, 5000, 4000, 1), ($3, 9000, 9000, 1), ($4, 80000, 0, 1)
		ON CONFLICT (pubkey) DO UPDATE
		SET follower_count = EXCLUDED.follower_count,
		    trusted_follower_count = EXCLUDED.trusted_follower_count,
		    recent_activity_at = NULL
	`, small, large, ghost, farm); err != nil {
		t.Fatalf("set follower counts: %v", err)
	}

	profiles, err := pgStore.GetMostFollowedProfiles(ctx, 10, 0)
	if err != nil {
		t.Fatalf("GetMostFollowedProfiles: %v", err)
	}
	if len(profiles) != 2 {
		t.Fatalf("expected the two profiles with a trusted audience, got %#v", profiles)
	}
	if profiles[0].Pubkey != large || profiles[0].FollowerCount != 5000 || profiles[0].TrustedFollowerCount != 4000 || profiles[0].Score != 4000 {
		t.Fatalf("largest trusted audience = %#v", profiles[0])
	}
	if profiles[1].Pubkey != small || profiles[1].FollowerCount != 50 || profiles[1].TrustedFollowerCount != 40 || profiles[1].Score != 40 {
		t.Fatalf("small account = %#v", profiles[1])
	}
	if profiles[1].RecentActivityAt != nil {
		t.Fatalf("dormant account should stay ranked, got recent_activity_at=%v", *profiles[1].RecentActivityAt)
	}
}

func TestPaidRankings_WindowedZapsAndSenderCap(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	author := strings.Repeat("a", 64)
	whale := strings.Repeat("b", 64)
	patron1 := strings.Repeat("c", 64)
	patron2 := strings.Repeat("d", 64)
	patron3 := strings.Repeat("e", 64)
	big := strings.Repeat("f", 64)
	rich := strings.Repeat("1", 64)
	quiet := strings.Repeat("2", 64)
	reply := strings.Repeat("3", 64)
	week := strings.Repeat("4", 64)

	insert := func(eventID, pubkey string, at time.Time, kind int, content string) {
		t.Helper()
		event := newDiscoveryEvent(eventID, pubkey, at, kind, nil, content)
		if err := pgStore.InsertCanonicalEvent(ctx, event, nil, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", eventID, err)
		}
	}
	insert("meta_"+author[:8], author, now, 0, `{"name":"Author"}`)
	insert("meta_"+whale[:8], whale, now, 0, `{"name":"Whale"}`)
	insert("meta_"+big[:8], big, now, 0, `{"name":"Big"}`)
	for _, pubkey := range []string{patron1, patron2, patron3} {
		insert("meta_"+pubkey[:8], pubkey, now, 0, `{"name":"Patron"}`)
	}
	insert(rich, author, now.Add(-40*24*time.Hour), 1, "old note, paid today")
	insert(week, author, now.Add(-3*24*time.Hour), 1, "three day note")
	insert(quiet, author, now.Add(-time.Hour), 1, "small zap")
	insert(reply, author, now.Add(-time.Hour), 1, "a reply")
	insert("zap_rich", whale, now, 9735, "")
	insert("zap_week", patron1, now, 9735, "")
	insert("zap_quiet", patron1, now, 9735, "")
	insert("zap_reply", whale, now, 9735, "")
	insert("zap_self", author, now, 9735, "")
	insert("zap_whale_person", whale, now, 9735, "")
	insert("zap_p1", patron1, now, 9735, "")
	insert("zap_p2", patron2, now, 9735, "")
	insert("zap_p3", patron3, now, 9735, "")

	for _, pubkey := range []string{author, whale, big, patron1, patron2, patron3} {
		if err := handlers.DeriveEventBundle(ctx, "meta_"+pubkey[:8]); err != nil {
			t.Fatalf("derive profile %s: %v", pubkey, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO thread_edges (
			child_event_id, child_created_at, parent_event_id, root_event_id, derivation_version
		) VALUES ($1, $2, $3, $3, 1)
	`, reply, now.Add(-time.Hour).Unix(), rich); err != nil {
		t.Fatalf("insert reply edge: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO zap_receipts (
			zap_receipt_id, created_at, event_id, sender_pubkey, receiver_pubkey, amount_sats, derivation_version
		) VALUES
			('zap_rich', $1, $2, $3, $4, 80000, 1),
			('zap_week', $1, $11, $6, $4, 5000, 1),
			('zap_quiet', $1, $5, $6, $4, 10, 1),
			('zap_reply', $1, $7, $3, $4, 90000, 1),
			('zap_self', $1, $2, $4, $4, 1000000, 1),
			('zap_whale_person', $1, NULL, $3, $8, 500000, 1),
			('zap_p1', $1, NULL, $6, $3, 50000, 1),
			('zap_p2', $1, NULL, $9, $3, 50000, 1),
			('zap_p3', $1, NULL, $10, $3, 50000, 1)
	`, now.Unix(), rich, whale, author, quiet, patron1, reply, big, patron2, patron3, week); err != nil {
		t.Fatalf("insert zap receipts: %v", err)
	}
	if err := handlers.RefreshPaidRankings(ctx); err != nil {
		t.Fatalf("RefreshPaidRankings: %v", err)
	}

	zapped, err := pgStore.GetMostZappedNotes(ctx, 24*time.Hour, "", 10, 0)
	if err != nil {
		t.Fatalf("GetMostZappedNotes: %v", err)
	}
	if len(zapped) != 1 || zapped[0].EventID != quiet || zapped[0].Score != 10 {
		t.Fatalf("24h zapped = %#v", zapped)
	}
	weekZapped, err := pgStore.GetMostZappedNotes(ctx, 7*24*time.Hour, "", 10, 0)
	if err != nil {
		t.Fatalf("GetMostZappedNotes 7d: %v", err)
	}
	if len(weekZapped) != 2 || weekZapped[0].EventID != week || weekZapped[0].Score != 5000 || weekZapped[1].EventID != quiet {
		t.Fatalf("7d zapped = %#v", weekZapped)
	}

	supported, err := pgStore.GetMostSupportedProfiles(ctx, 30*24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetMostSupportedProfiles: %v", err)
	}
	if len(supported) == 0 {
		t.Fatal("expected supported profiles")
	}
	var gotWhale, gotBig = supported[0], supported[0]
	seenWhale, seenBig := false, false
	for _, profile := range supported {
		switch profile.Pubkey {
		case whale:
			gotWhale = profile
			seenWhale = true
		case big:
			gotBig = profile
			seenBig = true
		}
	}
	if !seenWhale || gotWhale.CappedSats != 150_000 || gotWhale.RawSats != 150_000 || gotWhale.SupporterCount != 3 {
		t.Fatalf("patrons = %#v", gotWhale)
	}
	if !seenBig || gotBig.CappedSats != 100_000 || gotBig.RawSats != 500_000 || gotBig.SupporterCount != 1 {
		t.Fatalf("capped account = %#v", gotBig)
	}
	if len(supported) == 0 || supported[0].Pubkey != whale {
		t.Fatalf("expected the patron-funded account first, got %#v", supported)
	}
}
