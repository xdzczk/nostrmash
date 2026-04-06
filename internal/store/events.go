package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

// PostgresStore persists Layer 1 ingest records into Postgres.
type PostgresStore struct {
	pool         *pgxpool.Pool
	jobPublisher jobs.CanonicalEventPublisher
}

// CanonicalInsertResult exposes idempotent upsert outcomes for metrics.
type CanonicalInsertResult struct {
	EventInserted bool
}

var ErrNotFound = errors.New("not found")

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{
		pool:         pool,
		jobPublisher: jobs.NewQueuePublisher(5),
	}
}

// SetCanonicalEventJobPublisher overrides canonical-event downstream publication.
func (s *PostgresStore) SetCanonicalEventJobPublisher(publisher jobs.CanonicalEventPublisher) {
	if s == nil {
		return
	}
	s.jobPublisher = publisher
}

// InsertCanonicalEvent stores a canonical event, its expanded tags, and relay provenance.
// Event + tags + provenance are written in one transaction.
func (s *PostgresStore) InsertCanonicalEvent(
	ctx context.Context,
	event model.Event,
	tags [][]string,
	relayURL string,
	relaySeenAt time.Time,
) error {
	_, err := s.InsertCanonicalEventWithResult(ctx, event, tags, relayURL, relaySeenAt)
	return err
}

// InsertCanonicalEventWithResult stores canonical rows and returns whether this event id was new.
func (s *PostgresStore) InsertCanonicalEventWithResult(
	ctx context.Context,
	event model.Event,
	tags [][]string,
	relayURL string,
	relaySeenAt time.Time,
) (outcome CanonicalInsertResult, err error) {
	started := time.Now()
	ctx, span := traceutil.StartSpan(ctx, "store.insert_canonical_event")
	defer func() {
		span.End(err)
		metrics.ObserveDBOperation("insert_canonical_event", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return outcome, fmt.Errorf("store is not initialized")
	}
	if strings.TrimSpace(event.ID) == "" {
		return outcome, fmt.Errorf("event id is required")
	}
	if strings.TrimSpace(relayURL) == "" {
		return outcome, fmt.Errorf("relay url is required")
	}

	now := time.Now().UTC()
	firstSeenAt := event.FirstSeenAt
	if firstSeenAt.IsZero() {
		firstSeenAt = now
	}
	insertedAt := event.InsertedAt
	if insertedAt.IsZero() {
		insertedAt = now
	}
	if relaySeenAt.IsZero() {
		relaySeenAt = firstSeenAt
	}

	expandedTags := ExpandEventTags(event.ID, tags)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return outcome, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	err = tx.QueryRow(ctx, `
		INSERT INTO events (
			id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at, inserted_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE
		SET first_seen_at = LEAST(events.first_seen_at, EXCLUDED.first_seen_at)
		RETURNING (xmax = 0) AS inserted
	`,
		event.ID,
		event.Pubkey,
		event.CreatedAt,
		event.Kind,
		event.Sig,
		event.Content,
		json.RawMessage(event.RawJSON),
		firstSeenAt,
		insertedAt,
	).Scan(&outcome.EventInserted)
	if err != nil {
		return outcome, fmt.Errorf("upsert event: %w", err)
	}

	for _, tag := range expandedTags {
		_, err := tx.Exec(ctx, `
			INSERT INTO event_tags (
				event_id, tag_name, tag_index, value_index, value, raw_values
			)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (event_id, tag_index, value_index) DO NOTHING
		`,
			tag.EventID,
			tag.TagName,
			tag.TagIndex,
			tag.ValueIndex,
			tag.Value,
			json.RawMessage(tag.RawValues),
		)
		if err != nil {
			return outcome, fmt.Errorf("insert event tag: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO event_relays (event_id, relay_url, seen_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (event_id, relay_url) DO UPDATE
		SET seen_at = LEAST(event_relays.seen_at, EXCLUDED.seen_at)
	`,
		event.ID,
		relayURL,
		relaySeenAt,
	)
	if err != nil {
		return outcome, fmt.Errorf("upsert event relay: %w", err)
	}

	if outcome.EventInserted {
		if s.jobPublisher == nil {
			return outcome, fmt.Errorf("canonical event job publisher is not configured")
		}
		if err := s.jobPublisher.PublishCanonicalEventJobsTx(ctx, tx, event.ID); err != nil {
			return outcome, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return outcome, fmt.Errorf("commit tx: %w", err)
	}
	return outcome, nil
}

// GetEventRawByID returns the canonical Layer 1 event JSON by id.
func (s *PostgresStore) GetEventRawByID(ctx context.Context, id string) (rawJSON json.RawMessage, err error) {
	started := time.Now()
	ctx, span := traceutil.StartSpan(ctx, "store.get_event_raw_by_id")
	defer func() {
		span.End(err)
		metrics.ObserveDBOperation("get_event_raw_by_id", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	trimmedID := strings.TrimSpace(id)
	if trimmedID == "" {
		return nil, fmt.Errorf("event id is required")
	}

	var raw string
	err = s.pool.QueryRow(ctx, `
		SELECT raw_json::text
		FROM events
		WHERE id = $1
	`, trimmedID).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get event by id: %w", err)
	}
	return json.RawMessage(raw), nil
}

// GetEventRawsByIDs fetches canonical Layer 1 event JSON by ids.
func (s *PostgresStore) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(ids) == 0 {
		return map[string]json.RawMessage{}, nil
	}

	trimmedIDs := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		trimmedID := strings.TrimSpace(id)
		if trimmedID == "" {
			continue
		}
		if _, ok := seen[trimmedID]; ok {
			continue
		}
		seen[trimmedID] = struct{}{}
		trimmedIDs = append(trimmedIDs, trimmedID)
	}
	if len(trimmedIDs) == 0 {
		return map[string]json.RawMessage{}, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, raw_json::text
		FROM events
		WHERE id = ANY($1::text[])
	`, trimmedIDs)
	if err != nil {
		return nil, fmt.Errorf("get events by ids: %w", err)
	}
	defer rows.Close()

	out := make(map[string]json.RawMessage, len(trimmedIDs))
	for rows.Next() {
		var id string
		var raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		out[id] = json.RawMessage(raw)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event rows: %w", err)
	}
	return out, nil
}

// GetEventSeenOn returns relay provenance rows for an event id.
func (s *PostgresStore) GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	trimmedID := strings.TrimSpace(id)
	if trimmedID == "" {
		return nil, fmt.Errorf("event id is required")
	}

	rows, err := s.pool.Query(ctx, `
		SELECT relay_url, seen_at
		FROM event_relays
		WHERE event_id = $1
		ORDER BY seen_at ASC, relay_url ASC
	`, trimmedID)
	if err != nil {
		return nil, fmt.Errorf("get event seen-on: %w", err)
	}
	defer rows.Close()

	relays := make([]model.EventRelay, 0)
	for rows.Next() {
		var relay model.EventRelay
		relay.EventID = trimmedID
		if err := rows.Scan(&relay.RelayURL, &relay.SeenAt); err != nil {
			return nil, fmt.Errorf("scan event relay row: %w", err)
		}
		relays = append(relays, relay)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event relay rows: %w", err)
	}
	if len(relays) > 0 {
		return relays, nil
	}

	var exists bool
	err = s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)
	`, trimmedID).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("check event existence: %w", err)
	}
	if !exists {
		return nil, ErrNotFound
	}
	return relays, nil
}

type ProfileProjection struct {
	Pubkey            string
	MetadataEventID   string
	MetadataCreatedAt int64
	ProfileJSON       json.RawMessage
}

type EventCounts struct {
	EventID       string
	ReplyCount    int64
	ReactionCount int64
	RepostCount   int64
	Consistency   string
}

type EventWithProvenance struct {
	Event  json.RawMessage
	Relays []model.EventRelay
}

type EventOrderCursor struct {
	CreatedAt int64
	ID        string
}

// GetProfileByPubkey fetches the latest projected profile for one pubkey.
func (s *PostgresStore) GetProfileByPubkey(ctx context.Context, pubkey string) (out ProfileProjection, err error) {
	started := time.Now()
	ctx, span := traceutil.StartSpan(ctx, "store.get_profile_by_pubkey")
	defer func() {
		span.End(err)
		metrics.ObserveDBOperation("get_profile_by_pubkey", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return out, fmt.Errorf("pubkey is required")
	}

	var profileText string
	err = s.pool.QueryRow(ctx, `
		SELECT pubkey, metadata_event_id, metadata_created_at, profile_json::text
		FROM profiles_latest
		WHERE pubkey = $1
	`, pubkey).Scan(&out.Pubkey, &out.MetadataEventID, &out.MetadataCreatedAt, &profileText)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, ErrNotFound
		}
		return out, fmt.Errorf("get profile by pubkey: %w", err)
	}
	out.ProfileJSON = json.RawMessage(profileText)
	return out, nil
}

// GetProfilesByPubkeys fetches projected profiles for a unique set of pubkeys.
func (s *PostgresStore) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]ProfileProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(pubkeys) == 0 {
		return map[string]ProfileProjection{}, nil
	}

	normalized := make([]string, 0, len(pubkeys))
	seen := make(map[string]struct{}, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		normalized = append(normalized, pubkey)
	}
	if len(normalized) == 0 {
		return map[string]ProfileProjection{}, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, metadata_event_id, metadata_created_at, profile_json::text
		FROM profiles_latest
		WHERE pubkey = ANY($1::text[])
	`, normalized)
	if err != nil {
		return nil, fmt.Errorf("get profiles by pubkeys: %w", err)
	}
	defer rows.Close()

	out := make(map[string]ProfileProjection, len(normalized))
	for rows.Next() {
		var row ProfileProjection
		var profileText string
		if err := rows.Scan(&row.Pubkey, &row.MetadataEventID, &row.MetadataCreatedAt, &profileText); err != nil {
			return nil, fmt.Errorf("scan profile row: %w", err)
		}
		row.ProfileJSON = json.RawMessage(profileText)
		out[row.Pubkey] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read profile rows: %w", err)
	}
	return out, nil
}

// GetAuthorRecentEvents returns projected recent event payloads for one author.
func (s *PostgresStore) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM author_recent_events are
		INNER JOIN events e ON e.id = are.event_id
		WHERE are.author_pubkey = $1
		ORDER BY are.created_at DESC, are.event_id DESC
		LIMIT $2
	`, pubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get author recent events: %w", err)
	}
	defer rows.Close()

	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan author recent event row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author recent event rows: %w", err)
	}
	return out, nil
}

// GetAuthorReplies returns replies authored by one pubkey sorted by created_at desc, id desc.
func (s *PostgresStore) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM events e
		WHERE e.pubkey = $1
		  AND EXISTS (
		      SELECT 1
		      FROM event_references er
		      WHERE er.source_event_id = e.id
		        AND er.relation = 'reply'
		  )
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $2
	`, pubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get author replies: %w", err)
	}
	defer rows.Close()

	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan author reply row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author replies rows: %w", err)
	}
	return out, nil
}

// GetEventWithProvenance loads the canonical event payload and relay provenance.
func (s *PostgresStore) GetEventWithProvenance(ctx context.Context, id string) (EventWithProvenance, error) {
	out := EventWithProvenance{}
	raw, err := s.GetEventRawByID(ctx, id)
	if err != nil {
		return out, err
	}
	relays, err := s.GetEventSeenOn(ctx, id)
	if err != nil {
		return out, err
	}
	out.Event = raw
	out.Relays = relays
	return out, nil
}

// GetEventCounts returns eventually-consistent Layer 3 interaction counters.
func (s *PostgresStore) GetEventCounts(ctx context.Context, eventID string) (EventCounts, error) {
	out := EventCounts{Consistency: "eventual"}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return out, fmt.Errorf("event id is required")
	}
	out.EventID = eventID

	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT count FROM reply_counts WHERE event_id = $1), 0),
		       COALESCE((SELECT count FROM reaction_counts WHERE event_id = $1), 0),
		       COALESCE((SELECT count FROM repost_counts WHERE event_id = $1), 0)
	`, eventID).Scan(&out.ReplyCount, &out.ReactionCount, &out.RepostCount); err != nil {
		return out, fmt.Errorf("get event counts: %w", err)
	}
	return out, nil
}

// GetEventReplies returns one cursor-paginated page of direct replies ordered by created_at asc, id asc.
func (s *PostgresStore) GetEventReplies(
	ctx context.Context,
	eventID string,
	limit int,
	cursor *EventOrderCursor,
) (events []json.RawMessage, nextCursor *EventOrderCursor, err error) {
	started := time.Now()
	ctx, span := traceutil.StartSpan(ctx, "store.get_event_replies")
	defer func() {
		span.End(err)
		metrics.ObserveDBOperation("get_event_replies", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil, fmt.Errorf("event id is required")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	exists, err := s.eventExists(ctx, eventID)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, ErrNotFound
	}

	type replyRow struct {
		id        string
		createdAt int64
		raw       string
	}
	rowsOut := make([]replyRow, 0, limit+1)

	var rows pgx.Rows
	if cursor == nil {
		rows, err = s.pool.Query(ctx, `
			SELECT te.child_event_id, te.child_created_at, e.raw_json::text
			FROM thread_edges te
			INNER JOIN events e ON e.id = te.child_event_id
			WHERE te.parent_event_id = $1
			ORDER BY te.child_created_at ASC, te.child_event_id ASC
			LIMIT $2
		`, eventID, limit+1)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT te.child_event_id, te.child_created_at, e.raw_json::text
			FROM thread_edges te
			INNER JOIN events e ON e.id = te.child_event_id
			WHERE te.parent_event_id = $1
			  AND (te.child_created_at, te.child_event_id) > ($2, $3)
			ORDER BY te.child_created_at ASC, te.child_event_id ASC
			LIMIT $4
		`, eventID, cursor.CreatedAt, cursor.ID, limit+1)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("get event replies: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row replyRow
		if err := rows.Scan(&row.id, &row.createdAt, &row.raw); err != nil {
			return nil, nil, fmt.Errorf("scan event replies row: %w", err)
		}
		rowsOut = append(rowsOut, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read event replies rows: %w", err)
	}

	hasMore := len(rowsOut) > limit
	if hasMore {
		rowsOut = rowsOut[:limit]
	}
	events = make([]json.RawMessage, 0, len(rowsOut))
	for _, row := range rowsOut {
		events = append(events, json.RawMessage(row.raw))
	}

	nextCursor = nil
	if hasMore && len(rowsOut) > 0 {
		last := rowsOut[len(rowsOut)-1]
		nextCursor = &EventOrderCursor{
			CreatedAt: last.createdAt,
			ID:        last.id,
		}
	}
	return events, nextCursor, nil
}

// GetEventAncestors returns ancestors ordered root -> ... -> parent for one event.
func (s *PostgresStore) GetEventAncestors(
	ctx context.Context,
	eventID string,
	maxDepth int,
) (ancestors []json.RawMessage, missingIDs []string, err error) {
	started := time.Now()
	ctx, span := traceutil.StartSpan(ctx, "store.get_event_ancestors")
	defer func() {
		span.End(err)
		metrics.ObserveDBOperation("get_event_ancestors", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil, fmt.Errorf("event id is required")
	}
	if maxDepth <= 0 {
		maxDepth = 100
	}
	if maxDepth > 200 {
		maxDepth = 200
	}

	exists, err := s.eventExists(ctx, eventID)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, ErrNotFound
	}

	ancestorIDs := make([]string, 0, maxDepth)
	missingSet := map[string]struct{}{}
	current := eventID
	visited := map[string]struct{}{
		eventID: {},
	}

	for i := 0; i < maxDepth; i++ {
		var parentID string
		var parentMissing bool
		err := s.pool.QueryRow(ctx, `
			SELECT parent_event_id, parent_missing
			FROM thread_edges
			WHERE child_event_id = $1
		`, current).Scan(&parentID, &parentMissing)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				break
			}
			return nil, nil, fmt.Errorf("lookup thread edge for ancestors: %w", err)
		}
		parentID = strings.TrimSpace(parentID)
		if parentID == "" {
			break
		}
		if _, seen := visited[parentID]; seen {
			break
		}
		visited[parentID] = struct{}{}
		ancestorIDs = append(ancestorIDs, parentID)
		if parentMissing {
			missingSet[parentID] = struct{}{}
			break
		}
		current = parentID
	}

	foundByID, err := s.GetEventRawsByIDs(ctx, ancestorIDs)
	if err != nil {
		return nil, nil, err
	}

	ancestors = make([]json.RawMessage, 0, len(ancestorIDs))
	for i := len(ancestorIDs) - 1; i >= 0; i-- {
		ancestorID := ancestorIDs[i]
		raw, ok := foundByID[ancestorID]
		if !ok {
			missingSet[ancestorID] = struct{}{}
			continue
		}
		ancestors = append(ancestors, raw)
	}
	missingIDs = make([]string, 0, len(missingSet))
	for id := range missingSet {
		missingIDs = append(missingIDs, id)
	}
	slices.Sort(missingIDs)
	return ancestors, missingIDs, nil
}

// InsertInvalidEvent writes one invalid payload into quarantine storage.
// This intentionally uses an isolated write path from canonical ingest transactions.
func (s *PostgresStore) InsertInvalidEvent(ctx context.Context, invalid model.InvalidEvent) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	if strings.TrimSpace(invalid.ErrorCode) == "" {
		return fmt.Errorf("invalid event error_code is required")
	}
	if strings.TrimSpace(invalid.ErrorMessage) == "" {
		return fmt.Errorf("invalid event error_message is required")
	}

	seenAt := invalid.SeenAt
	if seenAt.IsZero() {
		seenAt = time.Now().UTC()
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO invalid_events (source_relay, error_code, error_message, raw_payload, seen_at)
		VALUES ($1, $2, $3, $4, $5)
	`,
		invalid.SourceRelay,
		invalid.ErrorCode,
		invalid.ErrorMessage,
		json.RawMessage(invalid.RawPayload),
		seenAt,
	)
	if err != nil {
		return fmt.Errorf("insert invalid event: %w", err)
	}
	return nil
}

func (s *PostgresStore) eventExists(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)
	`, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check event existence: %w", err)
	}
	return exists, nil
}

func dbResultFromErr(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, ErrNotFound) {
		return "not_found"
	}
	return "error"
}

func queueResultFromErr(err error) string {
	if err == nil {
		return "ok"
	}
	return "error"
}

// ListRelayHealth returns the latest persisted checkpoint rows per relay/mode/filter_group.
func (s *PostgresStore) ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT relay_url, mode, filter_group, "since", "until", cursor, eose_seen_at, status, updated_at
		FROM ingest_checkpoints
		ORDER BY updated_at DESC, relay_url ASC
	`)
	if err != nil {
		if strings.Contains(err.Error(), `column "since" does not exist`) {
			rows, err = s.pool.Query(ctx, `
				SELECT relay_url, mode, filter_group, since_ts, until_ts, cursor_val, eose_seen_at, status, updated_at
				FROM ingest_checkpoints
				ORDER BY updated_at DESC, relay_url ASC
			`)
		}
		if err != nil {
			return nil, fmt.Errorf("list relay health: %w", err)
		}
	}
	defer rows.Close()

	out := make([]model.IngestCheckpoint, 0)
	for rows.Next() {
		var row model.IngestCheckpoint
		if err := rows.Scan(
			&row.RelayURL,
			&row.Mode,
			&row.FilterGroup,
			&row.Since,
			&row.Until,
			&row.Cursor,
			&row.EOSESeenAt,
			&row.Status,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan relay health row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read relay health rows: %w", err)
	}
	return out, nil
}

// ExpandEventTags deterministically expands raw Nostr tags into event_tags rows.
func ExpandEventTags(eventID string, tags [][]string) []model.EventTag {
	totalValues := 0
	for _, tag := range tags {
		if len(tag) > 1 {
			totalValues += len(tag) - 1
		}
	}
	out := make([]model.EventTag, 0, totalValues)
	for tagIndex, tag := range tags {
		if len(tag) == 0 {
			continue
		}
		rawValues, err := json.Marshal(tag)
		if err != nil {
			// []string marshaling cannot fail; skip defensively if it does.
			continue
		}
		tagName := tag[0]
		for i := 1; i < len(tag); i++ {
			out = append(out, model.EventTag{
				EventID:    eventID,
				TagName:    tagName,
				TagIndex:   tagIndex,
				ValueIndex: i - 1,
				Value:      tag[i],
				RawValues:  rawValues,
			})
		}
	}
	return out
}
