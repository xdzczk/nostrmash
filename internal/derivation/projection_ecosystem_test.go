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

func TestProjectEventAttributesAndProfileFields(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	base := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	author := strings.Repeat("ab", 32)

	note := newEventForTest("attr_note", author, base.Unix(), 1, [][]string{
		{"client", "Damus"},
		{"expiration", "1893456000"},
		{"content-warning", "spoilers"},
		{"proxy", "https://example.social/n/1", "activitypub"},
		{"k", "1"},
	}, "hello", base)
	profile := newEventForTest("attr_profile", author, base.Unix(), 0, nil, `{"name":"Ada","nip05":"ada@example.com","lud16":"ada@getalby.com","about":"hi","picture":"https://cdn.example/a.png","bot":false}`, base)
	if err := insertEvents(t, ctx, pgStore, note, profile); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectEventAttributes(ctx, note.ID); err != nil {
		t.Fatalf("attributes: %v", err)
	}
	var client, protocol string
	var warning bool
	if err := pool.QueryRow(ctx, `
		SELECT client, proxy_protocol, content_warning FROM event_attributes WHERE event_id = $1
	`, note.ID).Scan(&client, &protocol, &warning); err != nil {
		t.Fatal(err)
	}
	if client != "Damus" || protocol != "activitypub" || !warning {
		t.Fatalf("attributes client=%s protocol=%s warning=%v", client, protocol, warning)
	}
	var count int64
	if err := pool.QueryRow(ctx, `SELECT event_count FROM client_daily WHERE client = 'Damus'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("client daily count=%d err=%v", count, err)
	}

	if err := handlers.ProjectProfileFields(ctx, profile.ID); err != nil {
		t.Fatalf("profile fields: %v", err)
	}
	var domain, status string
	var score int
	if err := pool.QueryRow(ctx, `
		SELECT f.nip05_domain, f.completeness_score, v.status
		FROM profile_fields f
		JOIN nip05_verifications v ON v.pubkey = f.pubkey
		WHERE f.pubkey = $1
	`, author).Scan(&domain, &score, &status); err != nil {
		t.Fatal(err)
	}
	if domain != "example.com" || status != "pending" || score < 4 {
		t.Fatalf("identity domain=%s status=%s score=%d", domain, status, score)
	}
}

func TestRefreshEcosystemDailyRollup(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	if err := handlers.RefreshEcosystemDailyRollup(ctx); err != nil {
		t.Fatalf("rollup: %v", err)
	}
	var marked bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ecosystem_daily_rollup)`).Scan(&marked); err != nil || !marked {
		t.Fatalf("marker missing err=%v", err)
	}
}

func TestProjectNutzapAndZapEconomics(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	base := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
	sender := strings.Repeat("cd", 32)
	receiver := strings.Repeat("ef", 32)
	note := newEventForTest("zap_note", receiver, base.Unix(), 1, nil, "note", base)
	zap := newEventForTest("zap_rcpt", sender, base.Add(time.Second).Unix(), 9735, [][]string{
		{"p", receiver},
		{"e", "zap_note"},
		{"amount", "21000"},
	}, "", base.Add(time.Second))
	nutzap := newEventForTest("nut_rcpt", sender, base.Add(2*time.Second).Unix(), 9321, [][]string{
		{"p", receiver},
		{"e", "zap_note"},
		{"proof", `{"amount":21}`},
		{"mint", "https://mint.example", "sat"},
	}, "", base.Add(2*time.Second))
	if err := insertEvents(t, ctx, pgStore, note, zap, nutzap); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectZapReceipts(ctx, zap.ID); err != nil {
		t.Fatalf("zap: %v", err)
	}
	if err := handlers.ProjectNutzapReceipts(ctx, nutzap.ID); err != nil {
		t.Fatalf("nutzap: %v", err)
	}
	var sats, nutSats int64
	if err := pool.QueryRow(ctx, `SELECT sats_total, nutzap_sats FROM zap_daily`).Scan(&sats, &nutSats); err != nil {
		t.Fatal(err)
	}
	if sats != 21 || nutSats != 21 {
		t.Fatalf("daily sats=%d nut=%d", sats, nutSats)
	}
	var edgeSats int64
	if err := pool.QueryRow(ctx, `
		SELECT sats_total FROM zap_edges WHERE sender_pubkey = $1 AND receiver_pubkey = $2
	`, sender, receiver).Scan(&edgeSats); err != nil || edgeSats != 21 {
		t.Fatalf("edge sats=%d err=%v", edgeSats, err)
	}
}

func insertEvents(t *testing.T, ctx context.Context, pgStore *store.PostgresStore, events ...model.Event) error {
	t.Helper()
	for _, event := range events {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			return err
		}
	}
	return nil
}
