package derivation_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectQuoteCounts_Kind1EventAndAddress(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	base := time.Date(2026, 4, 4, 20, 0, 0, 0, time.UTC)
	author := strings.Repeat("ab", 32)
	noteID := strings.Repeat("12", 32)
	articleID := strings.Repeat("34", 32)
	quoteNoteID := strings.Repeat("56", 32)
	quoteArticleID := strings.Repeat("78", 32)

	note := newEventForTest(noteID, author, 3000, 1, nil, "hello", base)
	article := newEventForTest(articleID, author, 3001, 30023, [][]string{{"d", "post"}}, "article", base)
	quoteNote := newEventForTest(quoteNoteID, author, 3002, 1, [][]string{
		{"q", noteID, "wss://relay.example", author},
	}, "nostr:"+noteID, base.Add(time.Second))
	quoteArticle := newEventForTest(quoteArticleID, author, 3003, 1, [][]string{
		{"q", "30023:" + strings.ToUpper(author) + ":post"},
	}, "quoted article", base.Add(2*time.Second))
	repost := newEventForTest(strings.Repeat("9a", 32), author, 3004, 6, [][]string{
		{"e", noteID},
	}, "", base.Add(3*time.Second))

	for _, event := range []model.Event{note, article, quoteNote, quoteArticle, repost} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
	}
	if err := handlers.UpdateReplaceableState(ctx, article.ID); err != nil {
		t.Fatalf("replaceable state: %v", err)
	}
	for _, id := range []string{quoteNote.ID, quoteArticle.ID, repost.ID} {
		if err := handlers.ProjectQuoteCounts(ctx, id); err != nil {
			t.Fatalf("project quotes %s: %v", id, err)
		}
	}

	noteQuotes, err := pgStore.QuoteCountsByEventIDs(ctx, []string{note.ID})
	if err != nil {
		t.Fatalf("note quote counts: %v", err)
	}
	if noteQuotes[note.ID] != 1 {
		t.Fatalf("note quote count = %d, want 1 (%v)", noteQuotes[note.ID], noteQuotes)
	}
	articleQuotes, err := pgStore.QuoteCountsByEventIDs(ctx, []string{article.ID})
	if err != nil {
		t.Fatalf("article quote counts: %v", err)
	}
	if articleQuotes[article.ID] != 1 {
		t.Fatalf("article quote count = %d, want 1 (%v)", articleQuotes[article.ID], articleQuotes)
	}

	counts, err := pgStore.GetEventCounts(ctx, note.ID)
	if err != nil {
		t.Fatalf("event counts: %v", err)
	}
	if counts.QuoteCount != 1 {
		t.Fatalf("event quote_count = %d, want 1", counts.QuoteCount)
	}
	if counts.RepostCount != 0 {
		t.Fatalf("kind-6 repost was not projected, repost_count = %d", counts.RepostCount)
	}
}

func TestProjectZapReceipts_AddressZapCountsOnArticleAndLive(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()
	author := strings.Repeat("cd", 32)
	articleID := strings.Repeat("a1", 32)
	liveID := strings.Repeat("b2", 32)

	article := newEventForTest(articleID, author, now.Unix(), 30023, [][]string{{"d", "essay"}}, "long", now)
	live := newEventForTest(liveID, author, now.Unix(), 30311, [][]string{
		{"d", "stream"},
		{"status", "live"},
	}, "", now)
	articleZap := newEventForTest(strings.Repeat("c3", 32), strings.Repeat("e4", 32), now.Unix(), 9735, [][]string{
		{"p", author},
		{"a", "30023:" + author + ":essay"},
		{"bolt11", "lnbc210n1pwarticle"},
		{"description", `{"content":"for the essay","tags":[]}`},
	}, "", now)
	liveZap := newEventForTest(strings.Repeat("d5", 32), strings.Repeat("f6", 32), now.Add(time.Second).Unix(), 9735, [][]string{
		{"p", author},
		{"a", "30311:" + author + ":stream"},
		{"bolt11", "lnbc250n1pwlive"},
		{"description", `{"content":"on air","tags":[]}`},
	}, "", now.Add(time.Second))

	for _, event := range []model.Event{article, live, articleZap, liveZap} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
	}
	if err := handlers.UpdateReplaceableState(ctx, article.ID); err != nil {
		t.Fatalf("article replaceable: %v", err)
	}
	if err := handlers.UpdateReplaceableState(ctx, live.ID); err != nil {
		t.Fatalf("live replaceable: %v", err)
	}
	if err := handlers.ProjectZapReceipts(ctx, articleZap.ID); err != nil {
		t.Fatalf("project article zap: %v", err)
	}
	if err := handlers.ProjectZapReceipts(ctx, liveZap.ID); err != nil {
		t.Fatalf("project live zap: %v", err)
	}
	if err := handlers.ProjectNoteDiscoveryStats(ctx, articleZap.ID); err != nil {
		t.Fatalf("refresh article zap target: %v", err)
	}
	if err := handlers.ProjectNoteDiscoveryStats(ctx, liveZap.ID); err != nil {
		t.Fatalf("refresh live zap target: %v", err)
	}

	var articleZaps, articleMsats, liveZaps, liveMsats int64
	if err := pool.QueryRow(ctx, `
		SELECT zap_count, zap_msats FROM note_discovery_stats WHERE event_id = $1
	`, article.ID).Scan(&articleZaps, &articleMsats); err != nil {
		t.Fatalf("article stats: %v", err)
	}
	if articleZaps != 1 || articleMsats != 21000 {
		t.Fatalf("article zaps = %d msats = %d, want 1 and 21000", articleZaps, articleMsats)
	}
	if err := pool.QueryRow(ctx, `
		SELECT zap_count, zap_msats FROM note_discovery_stats WHERE event_id = $1
	`, live.ID).Scan(&liveZaps, &liveMsats); err != nil {
		t.Fatalf("live stats: %v", err)
	}
	if liveZaps != 1 || liveMsats != 25000 {
		t.Fatalf("live zaps = %d msats = %d, want 1 and 25000", liveZaps, liveMsats)
	}

	ads, err := pgStore.TopZapAdsByEventIDs(ctx, []string{article.ID, live.ID})
	if err != nil {
		t.Fatalf("top zaps: %v", err)
	}
	if ads[article.ID].Sats != 21 {
		t.Fatalf("article top zap = %+v, want 21 sats", ads[article.ID])
	}
	if ads[live.ID].Sats != 25 {
		t.Fatalf("live top zap = %+v, want 25 sats", ads[live.ID])
	}
}
