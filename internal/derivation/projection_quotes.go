package derivation

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

const quoteProjectionDescription = "Project kind-1 quote posts from q tags onto the quoted event or address"

// quoteBackfillSQL inserts one row per distinct kind-1 quote target. $1 is
// the derivation version and must be cast: under DISTINCT, Postgres types an
// untyped parameter as text and the integer column then rejects the insert.
// q tags are not stored in event_tags, so the source is events.raw_json.
// Repeated q tags on one note collapse: ON CONFLICT cannot update the same
// row twice in one insert.
const quoteBackfillSQL = `
INSERT INTO quote_contributions (
	source_event_id, target_event_id, target_address, derivation_version
)
SELECT DISTINCT parsed.source_event_id, parsed.target_event_id, parsed.target_address, $1::integer
FROM (
	SELECT
		e.id AS source_event_id,
		CASE
			WHEN q.val ~ '^[0-9a-fA-F]{64}$' THEN lower(q.val)
			ELSE ''
		END AS target_event_id,
		CASE
			WHEN q.val ~ '^[0-9]+:[0-9a-fA-F]{64}:'
				THEN split_part(q.val, ':', 1) || ':' || lower(split_part(q.val, ':', 2)) || ':' ||
					substr(q.val, char_length(split_part(q.val, ':', 1)) + char_length(split_part(q.val, ':', 2)) + 3)
			ELSE ''
		END AS target_address
	FROM events e
	CROSS JOIN LATERAL (
		SELECT tag->>1 AS val
		FROM jsonb_array_elements(
			CASE
				WHEN jsonb_typeof(e.raw_json->'tags') = 'array' THEN e.raw_json->'tags'
				ELSE '[]'::jsonb
			END
		) AS tag
		WHERE jsonb_typeof(tag) = 'array'
		  AND jsonb_array_length(tag) >= 2
		  AND tag->>0 = 'q'
	) q
	WHERE e.kind = 1
) parsed
WHERE (parsed.target_event_id <> '' OR parsed.target_address <> '')
  AND parsed.target_event_id IS DISTINCT FROM lower(parsed.source_event_id)
ON CONFLICT (source_event_id, target_event_id, target_address) DO UPDATE
SET derivation_version = EXCLUDED.derivation_version,
    projected_at = now()
`

type quoteTarget struct {
	eventID string
	address string
}

func (h *Handlers) ProjectQuoteCounts(ctx context.Context, eventID string) error {
	return h.projectQuoteCountsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectQuoteCountsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var kind int
	if err := h.pool.QueryRow(ctx, `SELECT kind FROM events WHERE id = $1`, eventID).Scan(&kind); err != nil {
		return fmt.Errorf("load event for quote counts: %w", err)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	var targets []quoteTarget
	if kind == 1 {
		targets = quoteTargetsFromTags(eventID, tags)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin quote count tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationQuoteCounts,
		QuoteCountsVersion,
		quoteProjectionDescription,
		versionOverride,
	)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM quote_contributions WHERE source_event_id = $1`, eventID); err != nil {
		return fmt.Errorf("delete quote contributions: %w", err)
	}
	for _, target := range targets {
		if _, err := tx.Exec(ctx, `
			INSERT INTO quote_contributions (
				source_event_id, target_event_id, target_address, derivation_version
			)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (source_event_id, target_event_id, target_address) DO UPDATE
			SET derivation_version = EXCLUDED.derivation_version,
			    projected_at = now()
		`, eventID, target.eventID, target.address, writeVersion); err != nil {
			return fmt.Errorf("insert quote contribution: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit quote count tx: %w", err)
	}
	return nil
}

func (h *Handlers) rebuildQuoteCountsWithVersion(ctx context.Context, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin quote rebuild tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationQuoteCounts,
		QuoteCountsVersion,
		quoteProjectionDescription,
		versionOverride,
	)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM quote_contributions`); err != nil {
		return fmt.Errorf("clear quote contributions: %w", err)
	}
	if _, err := tx.Exec(ctx, quoteBackfillSQL, writeVersion); err != nil {
		return fmt.Errorf("backfill quote contributions: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit quote rebuild tx: %w", err)
	}
	return nil
}

func quoteTargetsFromTags(sourceEventID string, tags [][]string) []quoteTarget {
	seen := map[string]struct{}{}
	out := make([]quoteTarget, 0)
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != "q" {
			continue
		}
		value := strings.TrimSpace(tag[1])
		if value == "" {
			continue
		}
		var target quoteTarget
		if isHexEventID(value) {
			id := strings.ToLower(value)
			if strings.EqualFold(id, sourceEventID) {
				continue
			}
			target.eventID = id
		} else if addr, ok := nostr.ParseAddressCoordinate(value); ok {
			target.address = addr.String()
		} else {
			continue
		}
		key := target.eventID + "\x00" + target.address
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, target)
	}
	return out
}

func isHexEventID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if !unicode.Is(unicode.ASCII_Hex_Digit, r) {
			return false
		}
	}
	return true
}
