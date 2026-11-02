package derivation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestZapReceiptRebuildStoresInvoiceAmountAndOmitsZeroTopZap(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	goodDesc := `{"content":"Good Vibes","tags":[]}`
	zeroDesc := `{"content":"later","tags":[]}`
	events := []model.Event{
		newEventForTest("note_good", "author", now.Unix(), 1, nil, "hello", now),
		newEventForTest("note_zero", "author", now.Unix(), 1, nil, "quiet", now),
		newEventForTest("zap_good", "sender_good", now.Unix(), 9735, [][]string{
			{"p", "author"},
			{"e", "note_good"},
			{"bolt11", "lnbc210n1pwgood"},
			{"description", goodDesc},
		}, "", now),
		newEventForTest("zap_later_zero", "sender_later", now.Add(time.Second).Unix(), 9735, [][]string{
			{"p", "author"},
			{"e", "note_good"},
			{"bolt11", "lnbc1pwlater"},
			{"description", zeroDesc},
		}, "", now.Add(time.Second)),
		newEventForTest("zap_zero", "sender_zero", now.Unix(), 9735, [][]string{
			{"p", "author"},
			{"e", "note_zero"},
			{"bolt11", "lnbc1pwzero"},
			{"description", `{"content":"nothing","tags":[]}`},
		}, "", now),
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO zap_receipts (
			zap_receipt_id, created_at, event_id, sender_pubkey, receiver_pubkey, amount_sats, derivation_version
		) VALUES
			('zap_good', $1, 'note_good', 'sender_good', 'author', 0, 1),
			('zap_later_zero', $2, 'note_good', 'sender_later', 'author', 0, 1),
			('zap_zero', $1, 'note_zero', 'sender_zero', 'author', 0, 1)
	`, now.Unix(), now.Add(time.Second).Unix()); err != nil {
		t.Fatalf("seed zero zap receipts: %v", err)
	}

	runZap, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationZapReceipts,
		TargetVersion:  derivation.ZapReceiptsVersion,
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

	assertZapAmount(t, ctx, pool, "zap_good", 21)
	assertZapAmount(t, ctx, pool, "zap_later_zero", 0)
	assertZapAmount(t, ctx, pool, "zap_zero", 0)

	runNotes, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationNoteDiscoveryStats,
		TargetVersion:  derivation.NoteDiscoveryStatsVersion,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger note discovery rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, runNotes.ID); err != nil {
		t.Fatalf("execute note discovery rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, runNotes.ID)

	goodStats := readNoteDiscoveryStatsRow(t, ctx, pool, "note_good")
	if goodStats.ZapCount != 2 || goodStats.ZapMSats != 21_000 {
		t.Fatalf("note_good zap stats: count=%d msats=%d", goodStats.ZapCount, goodStats.ZapMSats)
	}
	zeroStats := readNoteDiscoveryStatsRow(t, ctx, pool, "note_zero")
	if zeroStats.ZapCount != 1 || zeroStats.ZapMSats != 0 {
		t.Fatalf("note_zero zap stats: count=%d msats=%d", zeroStats.ZapCount, zeroStats.ZapMSats)
	}

	ads, err := pgStore.TopZapAdsByEventIDs(ctx, []string{"note_good", "note_zero"})
	if err != nil {
		t.Fatalf("top zaps: %v", err)
	}
	ad, ok := ads["note_good"]
	if !ok {
		t.Fatal("expected top zap on note_good")
	}
	if ad.Sats != 21 || ad.SenderPubkey != "sender_good" || ad.Comment != "Good Vibes" {
		t.Fatalf("top zap = %+v", ad)
	}
	if _, ok := ads["note_zero"]; ok {
		t.Fatalf("zero-amount receipt was attached as top zap: %+v", ads["note_zero"])
	}
}

func TestZapAmountSQLSplitsOnBech32Separator(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	desc := func(amount string) string {
		t.Helper()
		body, err := json.Marshal(map[string]any{"tags": [][]string{{"amount", amount}}})
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	raw := func(tags [][]string) string {
		t.Helper()
		body, err := json.Marshal(map[string]any{"tags": tags})
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	cases := []struct {
		name string
		raw  string
		want int64
	}{
		{"nano amount containing digit 1", raw([][]string{{"bolt11", "lnbc210n1pw"}}), 21},
		{"later digit is not whole btc", raw([][]string{{"bolt11", "lnbc8210n1pw"}}), 821},
		{"description beats invoice", raw([][]string{{"bolt11", "lnbc210n1pw"}, {"description", desc("25000")}}), 25},
		{"receipt amount beats description", raw([][]string{{"amount", "21000"}, {"description", desc("999000")}, {"bolt11", "lnbc250n1pw"}}), 21},
		{"whole btc when the multiplier is absent", raw([][]string{{"bolt11", "lnbc11pw"}}), 100_000_000},
		{"missing amount", raw([][]string{{"bolt11", "lnbc1pw"}}), 0},
		{"micro decimal", raw([][]string{{"bolt11", "lnbc2.5u1pw"}}), 250},
		{"regtest prefix", raw([][]string{{"bolt11", "lnbcrt10u1pw"}}), 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got int64
			if err := pool.QueryRow(ctx, `SELECT migration_000111_zap_amount_sats($1::jsonb)`, tc.raw).Scan(&got); err != nil {
				t.Fatalf("query: %v", err)
			}
			if got != tc.want {
				t.Fatalf("amount = %d want %d", got, tc.want)
			}
		})
	}
}

func assertZapAmount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, receiptID string, wantSats int64) {
	t.Helper()
	var sats int64
	var version int
	if err := pool.QueryRow(ctx, `
		SELECT amount_sats, derivation_version
		FROM zap_receipts
		WHERE zap_receipt_id = $1
	`, receiptID).Scan(&sats, &version); err != nil {
		t.Fatalf("query zap receipt %s: %v", receiptID, err)
	}
	if sats != wantSats || version != derivation.ZapReceiptsVersion {
		t.Fatalf("zap receipt %s: amount_sats=%d version=%d want amount_sats=%d version=%d", receiptID, sats, version, wantSats, derivation.ZapReceiptsVersion)
	}
}
