package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestInsertCanonicalEventIdempotentOnIDAndPreservesEarliestFirstSeen(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	store := NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 10, 0, 0, 0, time.UTC)

	eventID := "event_1"
	firstRaw := json.RawMessage(`{"id":"event_1","kind":1,"content":"hello"}`)
	firstEvent := model.Event{
		ID:          eventID,
		Pubkey:      "pubkey_a",
		CreatedAt:   12345,
		Kind:        1,
		Sig:         "sig_a",
		Content:     "hello",
		RawJSON:     firstRaw,
		FirstSeenAt: baseTime.Add(10 * time.Minute),
		InsertedAt:  baseTime.Add(10 * time.Minute),
	}

	tags := [][]string{
		{"e", "root", "wss://relay.hint"},
		{"p", "author"},
		{"client", "nostrmash"},
	}

	if err := store.InsertCanonicalEvent(ctx, firstEvent, tags, "wss://relay.one", baseTime.Add(10*time.Minute)); err != nil {
		t.Fatalf("first insert canonical event: %v", err)
	}

	// Same event ID should be idempotent for canonical payload fields,
	// but earliest first_seen should still move backward.
	secondEvent := model.Event{
		ID:          eventID,
		Pubkey:      "pubkey_b_should_not_overwrite",
		CreatedAt:   99999,
		Kind:        2,
		Sig:         "sig_b_should_not_overwrite",
		Content:     "changed content should not overwrite",
		RawJSON:     json.RawMessage(`{"id":"event_1","content":"changed"}`),
		FirstSeenAt: baseTime.Add(-5 * time.Minute),
		InsertedAt:  baseTime.Add(15 * time.Minute),
	}
	if err := store.InsertCanonicalEvent(ctx, secondEvent, tags, "wss://relay.one", baseTime.Add(-5*time.Minute)); err != nil {
		t.Fatalf("second insert canonical event: %v", err)
	}

	var (
		pubkey      string
		createdAt   int64
		kind        int
		sig         string
		content     string
		rawJSON     []byte
		firstSeenAt time.Time
	)
	err := pool.QueryRow(ctx, `
		SELECT pubkey, created_at, kind, sig, content, raw_json::text, first_seen_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &createdAt, &kind, &sig, &content, &rawJSON, &firstSeenAt)
	if err != nil {
		t.Fatalf("query canonical event: %v", err)
	}

	if pubkey != firstEvent.Pubkey || createdAt != firstEvent.CreatedAt || kind != firstEvent.Kind || sig != firstEvent.Sig || content != firstEvent.Content {
		t.Fatalf("canonical fields were overwritten on duplicate event id")
	}
	if !jsonEqual(rawJSON, firstRaw) {
		t.Fatalf("raw_json was overwritten on duplicate event id; got %s want %s", string(rawJSON), string(firstRaw))
	}
	expectedFirstSeen := baseTime.Add(-5 * time.Minute)
	if !firstSeenAt.Equal(expectedFirstSeen) {
		t.Fatalf("first_seen_at mismatch: got %s want %s", firstSeenAt, expectedFirstSeen)
	}

	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE id = $1`, eventID).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("expected 1 events row, got %d", eventCount)
	}

	var tagCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM event_tags WHERE event_id = $1`, eventID).Scan(&tagCount); err != nil {
		t.Fatalf("count event_tags: %v", err)
	}
	if tagCount != 4 {
		t.Fatalf("expected 4 tag rows, got %d", tagCount)
	}

	var relayCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM event_relays WHERE event_id = $1`, eventID).Scan(&relayCount); err != nil {
		t.Fatalf("count event_relays: %v", err)
	}
	if relayCount != 1 {
		t.Fatalf("expected 1 relay row, got %d", relayCount)
	}
}

func TestInsertCanonicalEventAccumulatesProvenanceAcrossRelays(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	store := NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 11, 0, 0, 0, time.UTC)
	eventID := "event_2"
	event := model.Event{
		ID:          eventID,
		Pubkey:      "pubkey_x",
		CreatedAt:   22222,
		Kind:        1,
		Sig:         "sig_x",
		Content:     "hi",
		RawJSON:     json.RawMessage(`{"id":"event_2","kind":1}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	tags := [][]string{{"e", "root"}}

	if err := store.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("insert relay one: %v", err)
	}
	if err := store.InsertCanonicalEvent(ctx, event, tags, "wss://relay.two", baseTime.Add(3*time.Minute)); err != nil {
		t.Fatalf("insert relay two: %v", err)
	}
	// Duplicate provenance key should be idempotent and preserve earliest seen_at.
	if err := store.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", baseTime.Add(-1*time.Minute)); err != nil {
		t.Fatalf("reinsert relay one earlier: %v", err)
	}

	var relayCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM event_relays WHERE event_id = $1`, eventID).Scan(&relayCount); err != nil {
		t.Fatalf("count event_relays: %v", err)
	}
	if relayCount != 2 {
		t.Fatalf("expected 2 relay rows, got %d", relayCount)
	}

	var relayOneSeenAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT seen_at FROM event_relays WHERE event_id = $1 AND relay_url = $2
	`, eventID, "wss://relay.one").Scan(&relayOneSeenAt); err != nil {
		t.Fatalf("query relay one seen_at: %v", err)
	}
	expectedRelayOneSeenAt := baseTime.Add(-1 * time.Minute)
	if !relayOneSeenAt.Equal(expectedRelayOneSeenAt) {
		t.Fatalf("relay one seen_at mismatch: got %s want %s", relayOneSeenAt, expectedRelayOneSeenAt)
	}
}

func TestInsertInvalidEventWritesIsolatedQuarantineRecord(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	store := NewPostgresStore(pool)
	invalid := model.InvalidEvent{
		SourceRelay:  "wss://relay.bad",
		ErrorCode:    "signature_invalid",
		ErrorMessage: "signature does not verify",
		RawPayload:   json.RawMessage(`{"id":"bad","sig":"oops"}`),
		SeenAt:       time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC),
	}
	if err := store.InsertInvalidEvent(ctx, invalid); err != nil {
		t.Fatalf("insert invalid event: %v", err)
	}

	var (
		sourceRelay  string
		errorCode    string
		errorMessage string
		rawPayload   []byte
	)
	err := pool.QueryRow(ctx, `
		SELECT source_relay, error_code, error_message, raw_payload::text
		FROM invalid_events
		LIMIT 1
	`).Scan(&sourceRelay, &errorCode, &errorMessage, &rawPayload)
	if err != nil {
		t.Fatalf("query invalid event: %v", err)
	}
	if sourceRelay != invalid.SourceRelay || errorCode != invalid.ErrorCode || errorMessage != invalid.ErrorMessage {
		t.Fatalf("unexpected invalid event fields")
	}
	if !jsonEqual(rawPayload, invalid.RawPayload) {
		t.Fatalf("raw payload mismatch: got %s want %s", string(rawPayload), string(invalid.RawPayload))
	}
}

func TestGetEventRawByIDAndBatchQueries(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 13, 0, 0, 0, time.UTC)
	eventA := model.Event{
		ID:          "event_query_a",
		Pubkey:      "pub_a",
		CreatedAt:   111,
		Kind:        1,
		Sig:         "sig_a",
		Content:     "a",
		RawJSON:     json.RawMessage(`{"id":"event_query_a","kind":1}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	eventB := model.Event{
		ID:          "event_query_b",
		Pubkey:      "pub_b",
		CreatedAt:   222,
		Kind:        1,
		Sig:         "sig_b",
		Content:     "b",
		RawJSON:     json.RawMessage(`{"id":"event_query_b","kind":1}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	if err := s.InsertCanonicalEvent(ctx, eventA, nil, "wss://relay.one", baseTime); err != nil {
		t.Fatalf("insert event a: %v", err)
	}
	if err := s.InsertCanonicalEvent(ctx, eventB, nil, "wss://relay.two", baseTime); err != nil {
		t.Fatalf("insert event b: %v", err)
	}

	raw, err := s.GetEventRawByID(ctx, "event_query_a")
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}
	if !jsonEqual(raw, eventA.RawJSON) {
		t.Fatalf("event raw mismatch: got %s want %s", string(raw), string(eventA.RawJSON))
	}

	batch, err := s.GetEventRawsByIDs(ctx, []string{"event_query_a", "event_query_b", "missing"})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 found events, got %d", len(batch))
	}
	if !jsonEqual(batch["event_query_b"], eventB.RawJSON) {
		t.Fatalf("event b raw mismatch: got %s want %s", string(batch["event_query_b"]), string(eventB.RawJSON))
	}
}

func TestGetEventRawByID_NotFound(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pool)
	_, err := s.GetEventRawByID(ctx, "missing_event")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGetEventSeenOn(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 14, 0, 0, 0, time.UTC)
	event := model.Event{
		ID:          "event_seen_on",
		Pubkey:      "pub_seen_on",
		CreatedAt:   333,
		Kind:        1,
		Sig:         "sig_seen_on",
		Content:     "seen-on",
		RawJSON:     json.RawMessage(`{"id":"event_seen_on","kind":1}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	if err := s.InsertCanonicalEvent(ctx, event, nil, "wss://relay.b", baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("insert relay b: %v", err)
	}
	if err := s.InsertCanonicalEvent(ctx, event, nil, "wss://relay.a", baseTime.Add(1*time.Minute)); err != nil {
		t.Fatalf("insert relay a: %v", err)
	}

	relays, err := s.GetEventSeenOn(ctx, event.ID)
	if err != nil {
		t.Fatalf("get seen-on: %v", err)
	}
	if len(relays) != 2 {
		t.Fatalf("expected 2 seen-on rows, got %d", len(relays))
	}
	if relays[0].RelayURL != "wss://relay.a" || relays[1].RelayURL != "wss://relay.b" {
		t.Fatalf("expected seen-on sorted by seen_at asc, got %#v", relays)
	}

	_, err = s.GetEventSeenOn(ctx, "missing_event")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing event, got %v", err)
	}
}

func TestInsertCanonicalEventEnqueuesDerivationJobsOnce(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pool)
	event := model.Event{
		ID:          "event_enqueue_1",
		Pubkey:      "pub_enqueue",
		CreatedAt:   123,
		Kind:        1,
		Sig:         "sig_enqueue",
		Content:     "enqueue",
		RawJSON:     json.RawMessage(`{"id":"event_enqueue_1","kind":1,"tags":[]}`),
		FirstSeenAt: time.Date(2026, 4, 4, 15, 0, 0, 0, time.UTC),
		InsertedAt:  time.Date(2026, 4, 4, 15, 0, 0, 0, time.UTC),
	}
	if err := s.InsertCanonicalEvent(ctx, event, nil, "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	// Duplicate should not enqueue duplicate derivation jobs.
	if err := s.InsertCanonicalEvent(ctx, event, nil, "wss://relay.two", event.FirstSeenAt.Add(1*time.Second)); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT job_type, idempotency_key
		FROM jobs
		ORDER BY job_type ASC
	`)
	if err != nil {
		t.Fatalf("query enqueued jobs: %v", err)
	}
	defer rows.Close()

	type row struct {
		jobType string
		key     string
	}
	got := make([]row, 0)
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.jobType, &r.key); err != nil {
			t.Fatalf("scan jobs row: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read jobs rows: %v", err)
	}

	if len(got) != 14 {
		t.Fatalf("expected 14 derivation jobs, got %d", len(got))
	}
	expected := map[string]string{
		"derive_event_relationships":   "derive_event_relationships:event_enqueue_1",
		"project_contact_lists_latest": "project_contact_lists_latest:event_enqueue_1",
		"project_deletion_events":      "project_deletion_events:event_enqueue_1",
		"project_author_recent_event":  "project_author_recent_event:event_enqueue_1",
		"project_profiles_latest":      "project_profiles_latest:event_enqueue_1",
		"project_reaction_events":      "project_reaction_events:event_enqueue_1",
		"project_reaction_counts":      "project_reaction_counts:event_enqueue_1",
		"project_relay_lists_latest":   "project_relay_lists_latest:event_enqueue_1",
		"project_reply_counts":         "project_reply_counts:event_enqueue_1",
		"project_repost_events":        "project_repost_events:event_enqueue_1",
		"project_repost_counts":        "project_repost_counts:event_enqueue_1",
		"repair_unresolved_references": "repair_unresolved_references:event_enqueue_1",
		"update_replaceable_state":     "update_replaceable_state:event_enqueue_1",
		"update_thread_projection":     "update_thread_projection:event_enqueue_1",
	}
	for _, row := range got {
		key, ok := expected[row.jobType]
		if !ok {
			t.Fatalf("unexpected job type %q", row.jobType)
		}
		if row.key != key {
			t.Fatalf("unexpected idempotency key for %q: got %q want %q", row.jobType, row.key, key)
		}
	}
}

func TestGetEventAncestors_OrdersRootToParent(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	baseTime := time.Date(2026, 4, 4, 16, 0, 0, 0, time.UTC)

	root := model.Event{
		ID:          "thread_root",
		Pubkey:      "author_a",
		CreatedAt:   1000,
		Kind:        1,
		Sig:         "sig_root",
		Content:     "root",
		RawJSON:     json.RawMessage(`{"id":"thread_root","kind":1,"tags":[]}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	parentTags := [][]string{{"e", "thread_root", "", "reply"}, {"e", "thread_root", "", "root"}}
	parent := model.Event{
		ID:          "thread_parent",
		Pubkey:      "author_b",
		CreatedAt:   1001,
		Kind:        1,
		Sig:         "sig_parent",
		Content:     "parent",
		RawJSON:     json.RawMessage(`{"id":"thread_parent","kind":1,"tags":[["e","thread_root","","reply"],["e","thread_root","","root"]]}`),
		FirstSeenAt: baseTime.Add(1 * time.Second),
		InsertedAt:  baseTime.Add(1 * time.Second),
	}
	childTags := [][]string{{"e", "thread_root", "", "root"}, {"e", "thread_parent", "", "reply"}}
	child := model.Event{
		ID:          "thread_child",
		Pubkey:      "author_c",
		CreatedAt:   1002,
		Kind:        1,
		Sig:         "sig_child",
		Content:     "child",
		RawJSON:     json.RawMessage(`{"id":"thread_child","kind":1,"tags":[["e","thread_root","","root"],["e","thread_parent","","reply"]]}`),
		FirstSeenAt: baseTime.Add(2 * time.Second),
		InsertedAt:  baseTime.Add(2 * time.Second),
	}

	for _, event := range []struct {
		event model.Event
		tags  [][]string
	}{
		{root, nil},
		{parent, parentTags},
		{child, childTags},
	} {
		if err := s.InsertCanonicalEvent(ctx, event.event, event.tags, "wss://relay.one", event.event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.event.ID, err)
		}
	}
	if err := handlers.UpdateThreadProjection(ctx, parent.ID); err != nil {
		t.Fatalf("project parent thread edge: %v", err)
	}
	if err := handlers.UpdateThreadProjection(ctx, child.ID); err != nil {
		t.Fatalf("project child thread edge: %v", err)
	}

	ancestors, missing, err := s.GetEventAncestors(ctx, child.ID, 10)
	if err != nil {
		t.Fatalf("get event ancestors: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing ancestors, got %#v", missing)
	}
	if len(ancestors) != 2 {
		t.Fatalf("expected 2 ancestors, got %d", len(ancestors))
	}
	ids := decodeEventIDs(t, ancestors)
	want := []string{"thread_root", "thread_parent"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("unexpected ancestor ordering: got=%v want=%v", ids, want)
	}
}

func TestGetEventReplies_CursorStableAcrossCreatedAtTies(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	baseTime := time.Date(2026, 4, 4, 17, 0, 0, 0, time.UTC)

	parent := model.Event{
		ID:          "reply_parent",
		Pubkey:      "author_parent",
		CreatedAt:   2000,
		Kind:        1,
		Sig:         "sig_parent",
		Content:     "parent",
		RawJSON:     json.RawMessage(`{"id":"reply_parent","kind":1,"tags":[]}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	if err := s.InsertCanonicalEvent(ctx, parent, nil, "wss://relay.one", parent.FirstSeenAt); err != nil {
		t.Fatalf("insert parent event: %v", err)
	}

	replies := []model.Event{
		{
			ID:          "reply_a",
			Pubkey:      "author_a",
			CreatedAt:   2001,
			Kind:        1,
			Sig:         "sig_a",
			Content:     "a",
			RawJSON:     json.RawMessage(`{"id":"reply_a","kind":1,"tags":[["e","reply_parent","","reply"]]}`),
			FirstSeenAt: baseTime.Add(1 * time.Second),
			InsertedAt:  baseTime.Add(1 * time.Second),
		},
		{
			ID:          "reply_b",
			Pubkey:      "author_b",
			CreatedAt:   2001,
			Kind:        1,
			Sig:         "sig_b",
			Content:     "b",
			RawJSON:     json.RawMessage(`{"id":"reply_b","kind":1,"tags":[["e","reply_parent","","reply"]]}`),
			FirstSeenAt: baseTime.Add(2 * time.Second),
			InsertedAt:  baseTime.Add(2 * time.Second),
		},
		{
			ID:          "reply_c",
			Pubkey:      "author_c",
			CreatedAt:   2002,
			Kind:        1,
			Sig:         "sig_c",
			Content:     "c",
			RawJSON:     json.RawMessage(`{"id":"reply_c","kind":1,"tags":[["e","reply_parent","","reply"]]}`),
			FirstSeenAt: baseTime.Add(3 * time.Second),
			InsertedAt:  baseTime.Add(3 * time.Second),
		},
	}
	for _, reply := range replies {
		tags := [][]string{{"e", "reply_parent", "", "reply"}}
		if err := s.InsertCanonicalEvent(ctx, reply, tags, "wss://relay.one", reply.FirstSeenAt); err != nil {
			t.Fatalf("insert reply %s: %v", reply.ID, err)
		}
		if err := handlers.UpdateThreadProjection(ctx, reply.ID); err != nil {
			t.Fatalf("project thread edge for %s: %v", reply.ID, err)
		}
	}

	firstPage, next, err := s.GetEventReplies(ctx, parent.ID, 2, nil)
	if err != nil {
		t.Fatalf("get first replies page: %v", err)
	}
	if next == nil {
		t.Fatalf("expected next cursor for first page")
	}
	firstIDs := decodeEventIDs(t, firstPage)
	if !reflect.DeepEqual(firstIDs, []string{"reply_a", "reply_b"}) {
		t.Fatalf("unexpected first page ordering: got=%v", firstIDs)
	}

	secondPage, next2, err := s.GetEventReplies(ctx, parent.ID, 2, next)
	if err != nil {
		t.Fatalf("get second replies page: %v", err)
	}
	if next2 != nil {
		t.Fatalf("expected no next cursor on final page")
	}
	secondIDs := decodeEventIDs(t, secondPage)
	if !reflect.DeepEqual(secondIDs, []string{"reply_c"}) {
		t.Fatalf("unexpected second page ordering: got=%v", secondIDs)
	}
}

func jsonEqual(left []byte, right []byte) bool {
	var leftV any
	var rightV any
	if err := json.Unmarshal(left, &leftV); err != nil {
		return false
	}
	if err := json.Unmarshal(right, &rightV); err != nil {
		return false
	}
	return reflect.DeepEqual(leftV, rightV)
}

func decodeEventIDs(t *testing.T, raws []json.RawMessage) []string {
	t.Helper()
	ids := make([]string, 0, len(raws))
	for _, raw := range raws {
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatalf("decode event payload: %v", err)
		}
		ids = append(ids, payload.ID)
	}
	return ids
}
