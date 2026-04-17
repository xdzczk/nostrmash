package derivation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

// drainPendingAuthorAnalyticsForTest synchronously drains the
// pending_author_analytics_recomputes queue produced by DeriveEventBundle.
//
// In production the heavy author-analytics rebuild runs in a background
// sweeper so per-event bundles stay fast. Tests still want to assert on
// the final projected rows synchronously, so this helper plays the
// sweeper's role inline. It loops until the queue is empty (with a
// generous safety bound) so tests can call it once after their event
// fan-out without having to know how many pubkeys were marked.
func drainPendingAuthorAnalyticsForTest(t *testing.T, ctx context.Context, handlers *derivation.Handlers) {
	t.Helper()
	for safety := 0; safety < 64; safety++ {
		processed, err := handlers.DrainPendingAuthorAnalyticsBatch(ctx, 64)
		if err != nil {
			t.Fatalf("drain pending author analytics: %v", err)
		}
		if processed == 0 {
			return
		}
	}
	t.Fatalf("drain pending author analytics did not converge after 64 batches")
}

func newEventForTest(
	id string,
	pubkey string,
	createdAt int64,
	kind int,
	tags [][]string,
	content string,
	firstSeenAt time.Time,
) model.Event {
	payload := map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       kind,
		"tags":       tags,
		"content":    content,
		"sig":        "sig_" + id,
	}
	raw, _ := json.Marshal(payload)
	return model.Event{
		ID:          id,
		Pubkey:      pubkey,
		CreatedAt:   createdAt,
		Kind:        kind,
		Sig:         "sig_" + id,
		Content:     content,
		RawJSON:     raw,
		FirstSeenAt: firstSeenAt,
		InsertedAt:  firstSeenAt,
	}
}

func extractTagsFromRaw(t *testing.T, raw json.RawMessage) [][]string {
	t.Helper()
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode tags from raw event: %v", err)
	}
	return payload.Tags
}

func assertRebuildRunSucceeded(t *testing.T, ctx context.Context, handlers *derivation.Handlers, runID int64) {
	t.Helper()
	run, err := handlers.GetProjectionRebuildRun(ctx, runID)
	if err != nil {
		t.Fatalf("get rebuild run %d: %v", runID, err)
	}
	if run.Status != derivation.RebuildStatusSucceeded {
		t.Fatalf("expected run %d status=%q, got %q", runID, derivation.RebuildStatusSucceeded, run.Status)
	}
	if run.StartedAt == nil || run.FinishedAt == nil {
		t.Fatalf("expected run %d started_at and finished_at to be set", runID)
	}
	if run.LastError != nil {
		t.Fatalf("expected run %d last_error to be nil, got %q", runID, *run.LastError)
	}
}

func assertReplyContributionVersions(t *testing.T, ctx context.Context, pool *pgxpool.Pool, expected map[string]int) {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT source_event_id, derivation_version
		FROM reply_count_contributions
	`)
	if err != nil {
		t.Fatalf("query reply contribution versions: %v", err)
	}
	defer rows.Close()

	got := make(map[string]int, len(expected))
	for rows.Next() {
		var sourceEventID string
		var version int
		if err := rows.Scan(&sourceEventID, &version); err != nil {
			t.Fatalf("scan reply contribution version row: %v", err)
		}
		got[sourceEventID] = version
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read reply contribution version rows: %v", err)
	}

	if len(got) != len(expected) {
		t.Fatalf("unexpected reply contribution row count: got=%d want=%d", len(got), len(expected))
	}
	for eventID, expectedVersion := range expected {
		gotVersion, ok := got[eventID]
		if !ok {
			t.Fatalf("missing expected contribution row for %s", eventID)
		}
		if gotVersion != expectedVersion {
			t.Fatalf("unexpected derivation_version for %s: got=%d want=%d", eventID, gotVersion, expectedVersion)
		}
	}
}

func assertActiveAndTargetVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, derivationName string, expectedActive, expectedTarget int) {
	t.Helper()
	var active int
	var target int
	if err := pool.QueryRow(ctx, `
		SELECT active_version, target_version
		FROM derivation_active_versions
		WHERE derivation_name = $1
	`, derivationName).Scan(&active, &target); err != nil {
		t.Fatalf("query derivation active/target versions for %s: %v", derivationName, err)
	}
	if active != expectedActive || target != expectedTarget {
		t.Fatalf(
			"unexpected derivation versions for %s: active=%d target=%d want_active=%d want_target=%d",
			derivationName,
			active,
			target,
			expectedActive,
			expectedTarget,
		)
	}
}

func readEventRefRows(ctx context.Context, pool *pgxpool.Pool, eventID string) ([]refRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT referenced_event_id, relation, tag_index
		FROM event_references
		WHERE source_event_id = $1
		ORDER BY tag_index ASC
	`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]refRow, 0)
	for rows.Next() {
		var row refRow
		if err := rows.Scan(&row.referenced, &row.relation, &row.tagIndex); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func readPubkeyRefRows(ctx context.Context, pool *pgxpool.Pool, eventID string) ([]refRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT referenced_pubkey, relation, tag_index
		FROM pubkey_references
		WHERE source_event_id = $1
		ORDER BY tag_index ASC
	`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]refRow, 0)
	for rows.Next() {
		var row refRow
		if err := rows.Scan(&row.referenced, &row.relation, &row.tagIndex); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

type refRow struct {
	referenced string
	relation   string
	tagIndex   int
}

func assertRefRowsEqual(t *testing.T, got, want []refRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("unexpected row count: got=%d want=%d rows=%#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected row at index %d: got=%#v want=%#v", i, got[i], want[i])
		}
	}
}

func setupSchemaPool(t *testing.T, ctx context.Context, dbURL string) *pgxpool.Pool {
	t.Helper()
	return dbtest.SetupSchemaPool(t, ctx, dbURL, "derivations")
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	return dbtest.DatabaseURL(t, "derivation")
}
