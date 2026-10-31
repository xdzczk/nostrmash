package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetHotConversations_WindowsAndOrdering(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	rootA := newThreadEventForHotConversations("hot_root_a", "author_a", now.Add(-10*time.Hour), nil, "root a")
	rootB := newThreadEventForHotConversations("hot_root_b", "author_b", now.Add(-6*24*time.Hour), nil, "root b")
	// Farmed thread: the root author replying to themselves in a loop. Raw
	// reply counters record it, but reply weights (unique repliers excluding
	// the root author) stay zero, so it must never surface as hot.
	rootC := newThreadEventForHotConversations("hot_root_c", "author_c", now.Add(-8*time.Hour), nil, "farmed root")
	events := []model.Event{
		rootA,
		rootB,
		rootC,
		newThreadEventForHotConversations("hot_c_self_1", "author_c", now.Add(-3*time.Hour),
			[][]string{{"e", rootC.ID, "", "reply"}, {"e", rootC.ID, "", "root"}}, "self reply 1"),
		newThreadEventForHotConversations("hot_c_self_2", "author_c", now.Add(-2*time.Hour),
			[][]string{{"e", rootC.ID, "", "reply"}, {"e", rootC.ID, "", "root"}}, "self reply 2"),
		newThreadEventForHotConversations("hot_c_self_3", "author_c", now.Add(-1*time.Hour),
			[][]string{{"e", rootC.ID, "", "reply"}, {"e", rootC.ID, "", "root"}}, "self reply 3"),
		newThreadEventForHotConversations("hot_a_reply_1", "author_a_1", now.Add(-3*time.Hour),
			[][]string{{"e", rootA.ID, "", "reply"}, {"e", rootA.ID, "", "root"}}, "a reply 1"),
		newThreadEventForHotConversations("hot_a_reply_2", "author_a_2", now.Add(-2*time.Hour),
			[][]string{{"e", rootA.ID, "", "reply"}, {"e", rootA.ID, "", "root"}}, "a reply 2"),
		newThreadEventForHotConversations("hot_b_reply_1", "author_b_1", now.Add(-5*24*time.Hour),
			[][]string{{"e", rootB.ID, "", "reply"}, {"e", rootB.ID, "", "root"}}, "b reply 1"),
		newThreadEventForHotConversations("hot_b_reply_2", "author_b_2", now.Add(-4*24*time.Hour),
			[][]string{{"e", rootB.ID, "", "reply"}, {"e", rootB.ID, "", "root"}}, "b reply 2"),
		newThreadEventForHotConversations("hot_b_reply_3", "author_b_3", now.Add(-2*24*time.Hour),
			[][]string{{"e", rootB.ID, "", "reply"}, {"e", rootB.ID, "", "root"}}, "b reply 3"),
		newThreadEventForHotConversations("hot_b_reply_4", "author_b_4", now.Add(-20*time.Hour),
			[][]string{{"e", rootB.ID, "", "reply"}, {"e", rootB.ID, "", "root"}}, "b reply 4"),
	}

	for _, event := range events {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := s.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.UpdateThreadProjection(ctx, event.ID); err != nil {
			t.Fatalf("project thread for %s: %v", event.ID, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO note_discovery_stats (
			event_id, author_pubkey, created_at,
			reply_count, repost_count, reaction_count, zap_count, zap_msats,
			score_24h, score_7d, derivation_version
		)
		VALUES
			($1, $2, $3, 2, 3, 5, 1, 15000, 1.0, 1.0, 1),
			($4, $5, $6, 4, 0, 0, 0, 0, 1.0, 1.0, 1)
	`, rootA.ID, rootA.Pubkey, rootA.CreatedAt, rootB.ID, rootB.Pubkey, rootB.CreatedAt); err != nil {
		t.Fatalf("insert note_discovery_stats: %v", err)
	}

	last24h, err := s.GetHotConversations(ctx, 24*time.Hour, "", 10, 0)
	if err != nil {
		t.Fatalf("GetHotConversations 24h: %v", err)
	}
	if len(last24h) != 2 {
		t.Fatalf("unexpected 24h hot conversation count (farmed self-reply thread must be excluded): got=%d want=2", len(last24h))
	}
	for _, row := range last24h {
		if row.RootEventID == rootC.ID {
			t.Fatalf("farmed self-reply thread must not surface as hot: %#v", row)
		}
	}
	if last24h[0].RootEventID != rootA.ID {
		t.Fatalf("expected root_a first in 24h, got %#v", last24h[0])
	}
	if last24h[0].Replies24h != 2 {
		t.Fatalf("unexpected 24h replies for root_a: got=%d want=2", last24h[0].Replies24h)
	}
	if last24h[0].RepostCount != 3 || last24h[0].ReactionCount != 5 || last24h[0].ZapCount != 1 || last24h[0].ZapMSats != 15000 {
		t.Fatalf("unexpected root_a engagement from note_discovery_stats: %#v", last24h[0])
	}
	if last24h[1].RepostCount != 0 || last24h[1].ReactionCount != 0 || last24h[1].ZapCount != 0 {
		t.Fatalf("unexpected root_b zero engagement: %#v", last24h[1])
	}

	last7d, err := s.GetHotConversations(ctx, 7*24*time.Hour, "", 10, 0)
	if err != nil {
		t.Fatalf("GetHotConversations 7d: %v", err)
	}
	if len(last7d) != 2 {
		t.Fatalf("unexpected 7d hot conversation count (farmed self-reply thread must be excluded): got=%d want=2", len(last7d))
	}
	if last7d[0].RootEventID != rootB.ID {
		t.Fatalf("expected root_b first in 7d, got %#v", last7d[0])
	}
	if last7d[0].Replies7d != 4 {
		t.Fatalf("unexpected 7d replies for root_b: got=%d want=4", last7d[0].Replies7d)
	}
	if last7d[0].VelocityScore <= last7d[1].VelocityScore {
		t.Fatalf("expected descending velocity score, got first=%f second=%f", last7d[0].VelocityScore, last7d[1].VelocityScore)
	}
}

func TestGetHotConversations_DiscountsLargeTwoAuthorThreads(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	broad := newThreadEventForHotConversations("hot_broad", "broad_author", now.Add(-6*time.Hour), nil, "broad root")
	pairSmall := newThreadEventForHotConversations("hot_pair_small", "pair_small_author", now.Add(-6*time.Hour), nil, "small pair root")
	pairLarge := newThreadEventForHotConversations("hot_pair_large", "pair_large_author", now.Add(-6*time.Hour), nil, "large pair root")
	solo := newThreadEventForHotConversations("hot_solo", "solo_author", now.Add(-6*time.Hour), nil, "solo root")

	events := []model.Event{broad, pairSmall, pairLarge, solo}
	reply := func(id, pubkey string, root model.Event, minutesAgo int, content string) model.Event {
		return newThreadEventForHotConversations(id, pubkey, now.Add(-time.Duration(minutesAgo)*time.Minute),
			[][]string{{"e", root.ID, "", "reply"}, {"e", root.ID, "", "root"}}, content)
	}
	events = append(events,
		reply("hot_broad_1", "broad_replier_1", broad, 50, "A distinct reply about the broader thread."),
		reply("hot_broad_2", "broad_replier_2", broad, 40, "Another person joining the broader thread."),
		reply("hot_pair_small_1", "pair_small_replier", pairSmall, 30, "Short exchange, first reply."),
		reply("hot_pair_small_2", "pair_small_replier", pairSmall, 20, "Short exchange, second reply."),
	)
	for i := 0; i < 48; i++ {
		events = append(events, reply(
			"hot_pair_large_"+string(rune('a'+(i%26)))+string(rune('a'+i/26)),
			"pair_large_replier",
			pairLarge,
			10,
			"Back and forth number "+string(rune('a'+i))+" stays between the same two people.",
		))
	}
	for i := 0; i < 30; i++ {
		events = append(events, reply(
			"hot_solo_"+string(rune('a'+i)),
			"solo_author",
			solo,
			15,
			"Solo reply "+string(rune('a'+i)),
		))
	}

	for _, event := range events {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := s.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.UpdateThreadProjection(ctx, event.ID); err != nil {
			t.Fatalf("project thread for %s: %v", event.ID, err)
		}
	}

	last24h, err := s.GetHotConversations(ctx, 24*time.Hour, "", 10, 0)
	if err != nil {
		t.Fatalf("GetHotConversations 24h: %v", err)
	}
	if len(last24h) != 3 {
		t.Fatalf("expected the solo thread to be excluded, got %#v", last24h)
	}
	if last24h[0].RootEventID != broad.ID || last24h[1].RootEventID != pairSmall.ID || last24h[2].RootEventID != pairLarge.ID {
		t.Fatalf("expected broad, then the short pair, then the long pair, got %#v", last24h)
	}
	if last24h[1].VelocityScore <= last24h[2].VelocityScore {
		t.Fatalf("expected the long two-author thread to rank below the short one: short=%f long=%f", last24h[1].VelocityScore, last24h[2].VelocityScore)
	}
	if last24h[2].Replies24h != 48 {
		t.Fatalf("display reply count should stay raw, got %d", last24h[2].Replies24h)
	}
	if last24h[2].VelocityScore >= 1 {
		t.Fatalf("expected the long two-author thread to sink below one full replier, got %f", last24h[2].VelocityScore)
	}
}

func TestGetHotConversations_SmallGroupsCannotOutrankBreadth(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	pile := newThreadEventForHotConversations("hot_pile", "pile_author", now.Add(-5*time.Hour), nil, "three people, a very long chain")
	broad := newThreadEventForHotConversations("hot_room", "room_author", now.Add(-5*time.Hour), nil, "six people having a conversation")
	reply := func(id, pubkey string, root model.Event, content string) model.Event {
		return newThreadEventForHotConversations(id, pubkey, now.Add(-30*time.Minute),
			[][]string{{"e", root.ID, "", "reply"}, {"e", root.ID, "", "root"}}, content)
	}
	events := []model.Event{pile, broad}
	for i := 0; i < 40; i++ {
		events = append(events, reply(
			"hot_pile_"+string(rune('a'+i/26))+string(rune('a'+i%26)),
			"pile_replier_"+string(rune('a'+i%2)),
			pile,
			"Nested reply "+string(rune('a'+i))+" in a thread that is still only three people.",
		))
	}
	broadReplies := []string{
		"One person adds a distinct point about the wider conversation.",
		"A second person answers with a different observation about the same note.",
		"Someone else brings a new detail the first two did not mention.",
		"A fourth voice explains why the thread is worth reading through.",
		"The fifth person closes with a separate conclusion of their own.",
	}
	for i, content := range broadReplies {
		events = append(events, reply(
			"hot_room_"+string(rune('a'+i)),
			"room_replier_"+string(rune('a'+i)),
			broad,
			content,
		))
	}
	for _, event := range events {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := s.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.UpdateThreadProjection(ctx, event.ID); err != nil {
			t.Fatalf("project thread for %s: %v", event.ID, err)
		}
	}

	last24h, err := s.GetHotConversations(ctx, 24*time.Hour, "", 10, 0)
	if err != nil {
		t.Fatalf("GetHotConversations: %v", err)
	}
	if len(last24h) != 2 {
		t.Fatalf("expected both threads, got %#v", last24h)
	}
	if last24h[0].RootEventID != broad.ID || last24h[1].RootEventID != pile.ID {
		t.Fatalf("expected the six-author thread ahead of the three-author pile, got %#v", last24h)
	}
	if last24h[1].Replies24h != 40 {
		t.Fatalf("display reply count stays raw, got %d", last24h[1].Replies24h)
	}
	wantPile := 2 * (24.0 / 40.0)
	if last24h[1].VelocityScore < wantPile-0.02 || last24h[1].VelocityScore > wantPile+0.02 {
		t.Fatalf("three-author velocity = %f, want about %f", last24h[1].VelocityScore, wantPile)
	}
	if last24h[0].VelocityScore <= last24h[1].VelocityScore {
		t.Fatalf("broad velocity %f should beat pile %f", last24h[0].VelocityScore, last24h[1].VelocityScore)
	}
}

func newThreadEventForHotConversations(id, pubkey string, ts time.Time, tags [][]string, content string) model.Event {
	createdAt := ts.Unix()
	raw, _ := json.Marshal(map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       1,
		"tags":       tags,
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
