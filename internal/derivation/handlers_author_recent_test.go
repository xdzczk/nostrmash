package derivation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectAuthorRecentEvent_OrderByCreatedAtDescIDDesc(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 17, 0, 0, 0, time.UTC)

	events := []model.Event{
		newEventForTest("mmmm", "author_ordering", 1002, 1, nil, "{}", baseTime.Add(1*time.Second)),
		newEventForTest("aaaa", "author_ordering", 1000, 1, nil, "{}", baseTime.Add(2*time.Second)),
		newEventForTest("zzzz", "author_ordering", 1000, 1, nil, "{}", baseTime.Add(3*time.Second)),
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, nil, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectAuthorRecentEvent(ctx, event.ID); err != nil {
			t.Fatalf("project author_recent_events for %s: %v", event.ID, err)
		}
	}

	recent, err := pgStore.GetAuthorRecentEvents(ctx, "author_ordering", 10)
	if err != nil {
		t.Fatalf("get author recent events: %v", err)
	}
	if len(recent) != 3 {
		t.Fatalf("unexpected event count: got=%d want=3", len(recent))
	}

	gotIDs := make([]string, 0, len(recent))
	for _, raw := range recent {
		var decoded struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode recent event: %v", err)
		}
		gotIDs = append(gotIDs, decoded.ID)
	}
	wantIDs := []string{"mmmm", "zzzz", "aaaa"}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("unexpected author ordering: got=%v want=%v", gotIDs, wantIDs)
		}
	}
}
