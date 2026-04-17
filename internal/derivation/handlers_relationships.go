package derivation

import (
	"context"
	"fmt"
	"strings"
)

func (h *Handlers) DeriveEventRelationships(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	rawTags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	eventRefs := deriveEventReferences(eventID, rawTags)
	pubkeyRefs := derivePubkeyReferences(eventID, rawTags)

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `DELETE FROM event_references WHERE source_event_id = $1`, eventID); err != nil {
		return fmt.Errorf("delete prior event references: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM pubkey_references WHERE source_event_id = $1`, eventID); err != nil {
		return fmt.Errorf("delete prior pubkey references: %w", err)
	}

	for _, ref := range eventRefs {
		_, err := tx.Exec(ctx, `
			INSERT INTO event_references (
				source_event_id, referenced_event_id, relation, tag_index, relay_hint, marker, derivation_version
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (source_event_id, tag_index, referenced_event_id, relation) DO UPDATE
			SET relay_hint = EXCLUDED.relay_hint,
			    marker = EXCLUDED.marker,
			    derivation_version = EXCLUDED.derivation_version,
			    derived_at = now()
		`,
			ref.SourceEventID,
			ref.Referenced,
			ref.Relation,
			ref.TagIndex,
			nullIfBlank(ref.RelayHint),
			nullIfBlank(ref.Marker),
			EventRelationshipsVersion,
		)
		if err != nil {
			return fmt.Errorf("insert event reference: %w", err)
		}
	}

	for _, ref := range pubkeyRefs {
		_, err := tx.Exec(ctx, `
			INSERT INTO pubkey_references (
				source_event_id, referenced_pubkey, relation, tag_index, relay_hint, marker, derivation_version
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (source_event_id, tag_index, referenced_pubkey, relation) DO UPDATE
			SET relay_hint = EXCLUDED.relay_hint,
			    marker = EXCLUDED.marker,
			    derivation_version = EXCLUDED.derivation_version,
			    derived_at = now()
		`,
			ref.SourceEventID,
			ref.Referenced,
			ref.Relation,
			ref.TagIndex,
			nullIfBlank(ref.RelayHint),
			nullIfBlank(ref.Marker),
			EventRelationshipsVersion,
		)
		if err != nil {
			return fmt.Errorf("insert pubkey reference: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit derive relationships tx: %w", err)
	}
	return nil
}
