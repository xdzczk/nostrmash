package derivation_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
	"github.com/xdzczk/nostrmash/migrations"
)

func TestProjectZapReceipts_SentPageUsesZapRequestAuthor(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	payer := strings.Repeat("ab", 32)
	wallet := strings.Repeat("cd", 32)
	receiver := strings.Repeat("ef", 32)
	noteID := strings.Repeat("12", 32)
	describedID := strings.Repeat("34", 32)
	taggedID := strings.Repeat("56", 32)

	note := newEventForTest(noteID, receiver, now.Unix(), 1, nil, "hello", now)
	described := newEventForTest(describedID, wallet, now.Add(time.Second).Unix(), 9735, [][]string{
		{"p", receiver},
		{"e", noteID},
		{"amount", "21000"},
		{"description", `{"pubkey":"` + strings.ToUpper(payer) + `","kind":9734,"content":"nice one","tags":[]}`},
	}, "", now.Add(time.Second))
	tagged := newEventForTest(taggedID, wallet, now.Add(2*time.Second).Unix(), 9735, [][]string{
		{"p", receiver},
		{"P", payer},
		{"e", noteID},
		{"amount", "5000"},
		{"description", `not-json`},
	}, "", now.Add(2*time.Second))

	for _, event := range []model.Event{note, described, tagged} {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive %s: %v", event.ID, err)
		}
	}

	assertZapSender(t, ctx, pool, describedID, payer)
	assertZapSender(t, ctx, pool, taggedID, payer)

	page, _, err := pgStore.GetAuthorSentZaps(ctx, payer, 10, nil)
	if err != nil {
		t.Fatalf("payer sent zaps: %v", err)
	}
	if got := zapRecordIDs(t, page); !reflect.DeepEqual(got, []string{taggedID, describedID}) {
		t.Fatalf("payer sent zaps = %v", got)
	}
	walletPage, _, err := pgStore.GetAuthorSentZaps(ctx, wallet, 10, nil)
	if err != nil {
		t.Fatalf("wallet sent zaps: %v", err)
	}
	if len(walletPage) != 0 {
		t.Fatalf("wallet sent zaps = %v, want none", zapRecordIDs(t, walletPage))
	}

	var payerSent, walletSent int64
	if err := pool.QueryRow(ctx, `SELECT content_zap_count FROM profile_public_stats WHERE pubkey = $1`, payer).Scan(&payerSent); err != nil {
		t.Fatalf("payer content_zap_count: %v", err)
	}
	if payerSent != 2 {
		t.Fatalf("payer content_zap_count = %d, want 2", payerSent)
	}
	if err := pool.QueryRow(ctx, `SELECT content_zap_count FROM profile_public_stats WHERE pubkey = $1`, wallet).Scan(&walletSent); err != nil {
		t.Fatalf("wallet content_zap_count: %v", err)
	}
	if walletSent != 0 {
		t.Fatalf("wallet content_zap_count = %d, want 0", walletSent)
	}
}

func TestZapReceiptSenderBackfillRewritesServicePubkey(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()
	payer := strings.Repeat("ab", 32)
	wallet := strings.Repeat("cd", 32)
	receiver := strings.Repeat("ef", 32)
	noteID := strings.Repeat("12", 32)
	describedID := strings.Repeat("34", 32)
	taggedID := strings.Repeat("56", 32)
	plainID := strings.Repeat("78", 32)

	events := []model.Event{
		newEventForTest(noteID, receiver, now.Unix(), 1, nil, "hello", now),
		newEventForTest(describedID, wallet, now.Unix(), 9735, [][]string{
			{"p", receiver},
			{"e", noteID},
			{"description", `{"pubkey":"` + strings.ToUpper(payer) + `","kind":9734,"content":"nice"}`},
		}, "", now),
		newEventForTest(taggedID, wallet, now.Unix(), 9735, [][]string{
			{"p", receiver},
			{"P", payer},
			{"description", `not-json`},
		}, "", now),
		newEventForTest(plainID, wallet, now.Unix(), 9735, [][]string{
			{"p", receiver},
			{"e", noteID},
			{"amount", "1000"},
		}, "", now),
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO zap_receipts (
			zap_receipt_id, created_at, event_id, sender_pubkey, receiver_pubkey, amount_sats, derivation_version
		) VALUES
			($1, $2, $3, $4, $5, 21, 1),
			($6, $2, $3, $4, $5, 5, 1),
			($7, $2, $3, $4, $5, 1, 1)
	`, describedID, now.Unix(), noteID, wallet, receiver, taggedID, plainID); err != nil {
		t.Fatalf("seed receipts: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO profile_public_stats (pubkey, derivation_version, content_zap_count)
		VALUES ($1, 1, 3), ($2, 1, 0)
	`, wallet, payer); err != nil {
		t.Fatalf("seed profile stats: %v", err)
	}

	sqlBytes, err := migrations.Files.ReadFile("000109_zap_receipt_sender.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("rerun sender backfill: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit backfill: %v", err)
	}

	assertZapSender(t, ctx, pool, describedID, payer)
	assertZapSender(t, ctx, pool, taggedID, payer)
	assertZapSender(t, ctx, pool, plainID, wallet)

	var payerSent, walletSent int64
	if err := pool.QueryRow(ctx, `SELECT content_zap_count FROM profile_public_stats WHERE pubkey = $1`, payer).Scan(&payerSent); err != nil {
		t.Fatalf("payer content_zap_count: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT content_zap_count FROM profile_public_stats WHERE pubkey = $1`, wallet).Scan(&walletSent); err != nil {
		t.Fatalf("wallet content_zap_count: %v", err)
	}
	if payerSent != 2 || walletSent != 1 {
		t.Fatalf("content_zap_count payer=%d wallet=%d, want 2 and 1", payerSent, walletSent)
	}

	page, _, err := pgStore.GetAuthorSentZaps(ctx, payer, 10, nil)
	if err != nil {
		t.Fatalf("payer sent zaps: %v", err)
	}
	if got := zapRecordIDs(t, page); !reflect.DeepEqual(got, []string{taggedID, describedID}) {
		t.Fatalf("payer sent zaps = %v", got)
	}
}

func assertZapSender(t *testing.T, ctx context.Context, pool *pgxpool.Pool, receiptID, want string) {
	t.Helper()
	var got string
	if err := pool.QueryRow(ctx, `SELECT sender_pubkey FROM zap_receipts WHERE zap_receipt_id = $1`, receiptID).Scan(&got); err != nil {
		t.Fatalf("sender for %s: %v", receiptID, err)
	}
	if got != want {
		t.Fatalf("sender for %s = %s, want %s", receiptID, got, want)
	}
}

func zapRecordIDs(t *testing.T, rows []json.RawMessage) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		var payload struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(row, &payload); err != nil {
			t.Fatalf("decode zap row: %v", err)
		}
		out = append(out, payload.EventID)
	}
	return out
}
