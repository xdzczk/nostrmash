package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/eventtags"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/traceutil"
)

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

	expandedTags := ExpandEventTags(event.ID, event.Kind, tags)

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
		event.RawJSON,
		firstSeenAt,
		insertedAt,
	).Scan(&outcome.EventInserted)
	if err != nil {
		return outcome, fmt.Errorf("upsert event: %w", err)
	}

	if len(expandedTags) > 0 {
		// Insert all expanded tags in a single round-trip via unnest arrays
		// instead of one Exec per tag. Tag-heavy events (e.g. large kind-3
		// contact lists) previously drove N round-trips and dominated ingest
		// latency and WAL churn. The event id is constant across rows, so it is
		// passed as a scalar rather than a repeated array column.
		tagNames := make([]string, len(expandedTags))
		tagIndexes := make([]int32, len(expandedTags))
		valueIndexes := make([]int32, len(expandedTags))
		values := make([]string, len(expandedTags))
		for i, tag := range expandedTags {
			tagNames[i] = tag.TagName
			tagIndexes[i] = int32(tag.TagIndex)
			valueIndexes[i] = int32(tag.ValueIndex)
			values[i] = tag.Value
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO event_tags (
				event_id, tag_name, tag_index, value_index, value
			)
			SELECT $1, t.tag_name, t.tag_index, t.value_index, t.value
			FROM unnest($2::text[], $3::int[], $4::int[], $5::text[])
				AS t(tag_name, tag_index, value_index, value)
			ON CONFLICT (event_id, tag_index, value_index) DO NOTHING
		`,
			event.ID,
			tagNames,
			tagIndexes,
			valueIndexes,
			values,
		)
		if err != nil {
			return outcome, fmt.Errorf("insert event tags: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO event_relays (event_id, relay_url, seen_at, pubkey)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id, relay_url) DO UPDATE
		SET seen_at = LEAST(event_relays.seen_at, EXCLUDED.seen_at),
		    pubkey = EXCLUDED.pubkey
	`,
		event.ID,
		relayURL,
		relaySeenAt,
		event.Pubkey,
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

// ExpandEventTags deterministically expands raw Nostr tags into event_tags
// rows, applying the internal/eventtags persistence policy (allowlist +
// kind scope). Filtered tags remain available via events.raw_json.
func ExpandEventTags(eventID string, kind int, tags [][]string) []model.EventTag {
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
		tagName := tag[0]
		if !eventtags.ShouldPersist(kind, tagName) {
			continue
		}
		for i := 1; i < len(tag); i++ {
			out = append(out, model.EventTag{
				EventID:    eventID,
				TagName:    tagName,
				TagIndex:   tagIndex,
				ValueIndex: i - 1,
				Value:      tag[i],
			})
		}
	}
	return out
}
