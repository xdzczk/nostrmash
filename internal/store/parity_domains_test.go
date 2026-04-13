package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestDomainQueries_PerNoteAndAggregates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newDomainEvent("dom_evt_1", "author_a", now.Add(-2*time.Hour), "https://alpha.example/a https://beta.example/b https://alpha.example/a"),
		newDomainEvent("dom_evt_2", "author_a", now.Add(-6*time.Hour), "https://alpha.example/c https://gamma.example/g"),
		newDomainEvent("dom_evt_3", "author_b", now.Add(-3*time.Hour), "https://alpha.example/z https://delta.example/d"),
		newDomainEvent("dom_evt_4", "author_b", now.Add(-8*24*time.Hour), "https://old.example/ignore"),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	perNote, err := pgStore.GetEventLinkedDomains(ctx, "dom_evt_1", 10)
	if err != nil {
		t.Fatalf("GetEventLinkedDomains: %v", err)
	}
	if len(perNote) != 2 {
		t.Fatalf("unexpected per-note domain count: got=%d want=2 rows=%#v", len(perNote), perNote)
	}
	if perNote[0].Domain != "alpha.example" || perNote[1].Domain != "beta.example" {
		t.Fatalf("unexpected per-note domains: %#v", perNote)
	}

	authorTop, err := pgStore.GetTopDomainsByAuthor(ctx, "author_a", 7*24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTopDomainsByAuthor: %v", err)
	}
	if len(authorTop) != 3 {
		t.Fatalf("unexpected author domain count: got=%d want=3 rows=%#v", len(authorTop), authorTop)
	}
	if authorTop[0].Domain != "alpha.example" || authorTop[0].LinkCount != 2 || authorTop[0].NoteCount != 2 {
		t.Fatalf("unexpected top author domain row: %#v", authorTop[0])
	}

	discoveryTop, err := pgStore.GetTopDomains(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTopDomains: %v", err)
	}
	if len(discoveryTop) != 4 {
		t.Fatalf("unexpected discovery domain count: got=%d want=4 rows=%#v", len(discoveryTop), discoveryTop)
	}
	if discoveryTop[0].Domain != "alpha.example" || discoveryTop[0].LinkCount != 3 || discoveryTop[0].NoteCount != 3 || discoveryTop[0].UniqueAuthors != 2 {
		t.Fatalf("unexpected top discovery domain row: %#v", discoveryTop[0])
	}
}

func TestDomainSummaryAndNotesQueryBehavior(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()
	events := []model.Event{
		newDomainEvent("dom_page_1", "author_a", now.Add(-6*time.Hour), "https://example.com/a"),
		newDomainEvent("dom_page_2", "author_b", now.Add(-1*time.Hour), "https://example.com/b"),
		newDomainEvent("dom_page_3", "author_c", now.Add(-3*time.Hour), "https://example.com/c"),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	if err := handlers.ProjectNoteDiscoveryStats(ctx, "dom_page_1"); err != nil {
		t.Fatalf("project note stats: %v", err)
	}
	if err := handlers.ProjectNoteDiscoveryStats(ctx, "dom_page_2"); err != nil {
		t.Fatalf("project note stats: %v", err)
	}
	if err := handlers.ProjectNoteDiscoveryStats(ctx, "dom_page_3"); err != nil {
		t.Fatalf("project note stats: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE note_discovery_stats SET reply_count = 50 WHERE event_id = 'dom_page_1'`); err != nil {
		t.Fatalf("boost top-note engagement: %v", err)
	}

	summary, err := pgStore.GetDomainSummary(ctx, "  HTTPS://Example.com. ", 2, 2)
	if err != nil {
		t.Fatalf("GetDomainSummary: %v", err)
	}
	if summary.Domain != "example.com" {
		t.Fatalf("unexpected normalized domain: got=%q want=example.com", summary.Domain)
	}
	if summary.Activity.All.NoteCount != 3 || summary.Activity.All.UniqueAuthors != 3 {
		t.Fatalf("unexpected all-time activity: %#v", summary.Activity.All)
	}
	if len(summary.RecentNotes) == 0 || len(summary.TopNotes) == 0 {
		t.Fatalf("expected both recent/top notes in summary: %#v", summary)
	}
	if summary.RecentNotes[0].EventID == summary.TopNotes[0].EventID {
		t.Fatalf("expected recent and top notes heads to differ")
	}

	notes, err := pgStore.GetDomainNotes(ctx, "example.com", "top", "30d", 3, 0)
	if err != nil {
		t.Fatalf("GetDomainNotes: %v", err)
	}
	if len(notes) == 0 || notes[0].EventID != "dom_page_1" {
		t.Fatalf("unexpected top domain notes ordering: %#v", notes)
	}
}

func TestGetTrendingDomains_PrioritizesUniqueAuthorsAndDiversity(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()
	events := []model.Event{
		newDomainEvent("trend_dom_1", "author_a", now.Add(-1*time.Hour), "https://lowdiv.example/1"),
		newDomainEvent("trend_dom_2", "author_a", now.Add(-2*time.Hour), "https://lowdiv.example/2"),
		newDomainEvent("trend_dom_3", "author_a", now.Add(-3*time.Hour), "https://lowdiv.example/3"),
		newDomainEvent("trend_dom_4", "author_b", now.Add(-4*time.Hour), "https://lowdiv.example/4"),
		newDomainEvent("trend_dom_5", "author_c", now.Add(-1*time.Hour), "https://highdiv.example/1"),
		newDomainEvent("trend_dom_6", "author_d", now.Add(-2*time.Hour), "https://highdiv.example/2"),
		newDomainEvent("trend_dom_7", "author_e", now.Add(-1*time.Hour), "https://moreauthors.example/1"),
		newDomainEvent("trend_dom_8", "author_f", now.Add(-2*time.Hour), "https://moreauthors.example/2"),
		newDomainEvent("trend_dom_9", "author_g", now.Add(-3*time.Hour), "https://moreauthors.example/3"),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	out, err := pgStore.GetTrendingDomains(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingDomains: %v", err)
	}
	if len(out) < 3 {
		t.Fatalf("unexpected result count: got=%d want>=3", len(out))
	}
	if out[0].Domain != "moreauthors.example" {
		t.Fatalf("expected domain with most unique authors first, got %#v", out[0])
	}
	if out[1].Domain != "highdiv.example" {
		t.Fatalf("expected highdiv.example second due to better diversity among same unique authors, got %#v", out[1])
	}
	if out[2].Domain != "lowdiv.example" {
		t.Fatalf("expected lowdiv.example third due to lower diversity, got %#v", out[2])
	}
}

func TestGetTopDomains_PrioritizesUniqueAuthorsAndDiversity(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()
	events := []model.Event{
		newDomainEvent("top_dom_1", "author_a", now.Add(-1*time.Hour), "https://lowdiv.example/1"),
		newDomainEvent("top_dom_2", "author_a", now.Add(-2*time.Hour), "https://lowdiv.example/2"),
		newDomainEvent("top_dom_3", "author_a", now.Add(-3*time.Hour), "https://lowdiv.example/3"),
		newDomainEvent("top_dom_4", "author_b", now.Add(-4*time.Hour), "https://lowdiv.example/4"),
		newDomainEvent("top_dom_5", "author_c", now.Add(-1*time.Hour), "https://highdiv.example/1"),
		newDomainEvent("top_dom_6", "author_d", now.Add(-2*time.Hour), "https://highdiv.example/2"),
		newDomainEvent("top_dom_7", "author_e", now.Add(-1*time.Hour), "https://moreauthors.example/1"),
		newDomainEvent("top_dom_8", "author_f", now.Add(-2*time.Hour), "https://moreauthors.example/2"),
		newDomainEvent("top_dom_9", "author_g", now.Add(-3*time.Hour), "https://moreauthors.example/3"),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	out, err := pgStore.GetTopDomains(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTopDomains: %v", err)
	}
	if len(out) < 3 {
		t.Fatalf("unexpected result count: got=%d want>=3", len(out))
	}
	if out[0].Domain != "moreauthors.example" {
		t.Fatalf("expected domain with most unique authors first, got %#v", out[0])
	}
	if out[1].Domain != "highdiv.example" {
		t.Fatalf("expected highdiv.example second due to better diversity among same unique authors, got %#v", out[1])
	}
	if out[2].Domain != "lowdiv.example" {
		t.Fatalf("expected lowdiv.example third due to lower diversity, got %#v", out[2])
	}
}

func TestDomainCalculations_ExcludeMediaLinks(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()
	events := []model.Event{
		newDomainEvent("media_dom_1", "author_a", now.Add(-1*time.Hour), "https://cdn.example/photo.jpg https://cdn.example/video.mp4"),
		newDomainEvent("media_dom_2", "author_b", now.Add(-2*time.Hour), "https://cdn.example/clip.webm"),
		newDomainEvent("media_dom_3", "author_c", now.Add(-1*time.Hour), "https://news.example/story"),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	topDomains, err := pgStore.GetTopDomains(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTopDomains: %v", err)
	}
	if len(topDomains) != 1 || topDomains[0].Domain != "news.example" {
		t.Fatalf("expected only non-media domain in top domains, got %#v", topDomains)
	}

	trendingDomains, err := pgStore.GetTrendingDomains(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingDomains: %v", err)
	}
	if len(trendingDomains) != 1 || trendingDomains[0].Domain != "news.example" {
		t.Fatalf("expected only non-media domain in trending domains, got %#v", trendingDomains)
	}

	if _, err := pgStore.GetDomainSummary(ctx, "cdn.example", 2, 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected media-only domain summary to be absent, got %v", err)
	}
}

func TestDomainQueries_NormalizationAndMissingBehavior(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()
	event := newDomainEvent("dom_norm_1", "author_a", now.Add(-time.Hour), "https://example.com/a")
	tags := extractTagsForStoreTest(t, event.RawJSON)
	if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
		t.Fatalf("derive event bundle: %v", err)
	}
	if err := handlers.ProjectNoteDiscoveryStats(ctx, event.ID); err != nil {
		t.Fatalf("project note stats: %v", err)
	}

	if _, err := pgStore.GetDomainSummary(ctx, "HTTPS://EXAMPLE.COM", 1, 1); err != nil {
		t.Fatalf("expected normalized URL-form domain lookup to work, got: %v", err)
	}
	_, err := pgStore.GetDomainSummary(ctx, "missing.example", 1, 1)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing domain, got %v", err)
	}
}

func newDomainEvent(id, pubkey string, ts time.Time, content string) model.Event {
	createdAt := ts.Unix()
	raw, _ := json.Marshal(map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       1,
		"tags":       [][]string{},
		"content":    content,
		"sig":        "sig_" + id,
	})
	return model.Event{
		ID:          id,
		Pubkey:      pubkey,
		CreatedAt:   createdAt,
		Kind:        1,
		Sig:         "sig_" + id,
		Content:     content,
		RawJSON:     raw,
		FirstSeenAt: ts,
		InsertedAt:  ts,
	}
}
