package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestProjectionRebuildRun_SupportsDMUnreadAndZapReceipts(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	dm := newEventForTest(
		"dm_for_rebuild",
		"sender_dm",
		1100,
		4,
		[][]string{{"p", "receiver_dm"}},
		`"encrypted"`,
		time.Unix(1100, 0).UTC(),
	)
	zap := newEventForTest(
		"zap_for_rebuild",
		"sender_zap",
		1101,
		9735,
		[][]string{{"p", "receiver_zap"}, {"e", "target_evt"}, {"amount", "5000"}},
		`""`,
		time.Unix(1101, 0).UTC(),
	)
	for _, event := range []model.Event{dm, zap} {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
	}

	runDM, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationDMUnreadCounts,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger dm rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, runDM.ID); err != nil {
		t.Fatalf("execute dm rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, runDM.ID)

	runZap, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationZapReceipts,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger zap rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, runZap.ID); err != nil {
		t.Fatalf("execute zap rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, runZap.ID)
}
