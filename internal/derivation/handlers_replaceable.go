package derivation

import (
	"context"
	"fmt"
	"strings"
)

func (h *Handlers) UpdateReplaceableState(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	type replaceableEvent struct {
		Pubkey    string
		Kind      int
		CreatedAt int64
		DTag      string
	}
	var event replaceableEvent
	err := h.pool.QueryRow(ctx, `
		SELECT e.pubkey, e.kind, e.created_at,
		       COALESCE((
		           SELECT et.value
		           FROM event_tags et
		           WHERE et.event_id = e.id
		             AND et.tag_name = 'd'
		             AND et.tag_index >= 0
		             AND et.value_index = 0
		           ORDER BY et.tag_index ASC
		           LIMIT 1
		       ), '')
		FROM events e
		WHERE e.id = $1
	`, eventID).Scan(&event.Pubkey, &event.Kind, &event.CreatedAt, &event.DTag)
	if err != nil {
		return fmt.Errorf("load event for replaceable derivation: %w", err)
	}
	if !isReplaceableKind(event.Kind) {
		return nil
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var isNewAddress bool
	err = tx.QueryRow(ctx, `
		WITH prior AS (
			SELECT event_id
			FROM replaceable_state
			WHERE pubkey = $1 AND kind = $2 AND d_tag = $3
		),
		upsert AS (
			INSERT INTO replaceable_state (
				pubkey, kind, d_tag, event_id, created_at, derivation_version
			)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (pubkey, kind, d_tag) DO UPDATE
			SET event_id = EXCLUDED.event_id,
			    created_at = EXCLUDED.created_at,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
			WHERE EXCLUDED.created_at > replaceable_state.created_at
			   OR (
			       EXCLUDED.created_at = replaceable_state.created_at
			       AND EXCLUDED.event_id > replaceable_state.event_id
			   )
			RETURNING event_id
		)
		SELECT NOT EXISTS (SELECT 1 FROM prior) AND EXISTS (SELECT 1 FROM upsert)
	`,
		event.Pubkey,
		event.Kind,
		event.DTag,
		eventID,
		event.CreatedAt,
		ReplaceableStateVersion,
	).Scan(&isNewAddress)
	if err != nil {
		return fmt.Errorf("upsert replaceable state: %w", err)
	}
	// A new kind-30023 address is one article. Replacing the same d tag
	// keeps the count. Deleting the current copy decrements it.
	if isNewAddress && (event.Kind == 34235 || event.Kind == 34236) && h.incrementalProfilePublicStats {
		writeVersion, err := resolveDerivationWriteVersion(
			ctx,
			tx,
			DerivationProfilePublicStats,
			ProfilePublicStatsVersion,
			"Project public profile counters and recent activity",
			nil,
		)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_public_stats (
				pubkey, follower_count, following_count, note_count, reply_count,
				media_count, derivation_version
			)
			VALUES ($1, 0, 0, 0, 0, 1, $2)
			ON CONFLICT (pubkey) DO UPDATE
			SET media_count = profile_public_stats.media_count + 1,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, event.Pubkey, writeVersion); err != nil {
			return fmt.Errorf("increment addressable media count: %w", err)
		}
	}
	if isNewAddress && event.Kind == 30023 && h.incrementalProfilePublicStats {
		writeVersion, err := resolveDerivationWriteVersion(
			ctx,
			tx,
			DerivationProfilePublicStats,
			ProfilePublicStatsVersion,
			"Project public profile counters and recent activity",
			nil,
		)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_public_stats (
				pubkey, follower_count, following_count, note_count, reply_count,
				long_form_note_count, derivation_version
			)
			VALUES ($1, 0, 0, 0, 0, 1, $2)
			ON CONFLICT (pubkey) DO UPDATE
			SET long_form_note_count = profile_public_stats.long_form_note_count + 1,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, event.Pubkey, writeVersion); err != nil {
			return fmt.Errorf("increment long-form note count: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit replaceable derivation tx: %w", err)
	}
	return nil
}

func isReplaceableKind(kind int) bool {
	if kind == 0 || kind == 3 {
		return true
	}
	if kind >= 10000 && kind < 20000 {
		return true
	}
	return kind >= 30000 && kind < 40000
}
