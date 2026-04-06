package derivation

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) ProjectReactionEvents(ctx context.Context, eventID string) error {
	return h.projectReactionEventsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectReactionEventsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectInteractionEvent(
		ctx,
		eventID,
		7,
		DerivationReactionEvents,
		ReactionEventsVersion,
		"Project reaction_events records from kind=7 references",
		versionOverride,
		func(tx pgx.Tx, source interactionSource, targetEventID string, writeVersion int) error {
			_, err := tx.Exec(ctx, `
				INSERT INTO reaction_events (
					event_id, target_event_id, reactor_pubkey, content, created_at, derivation_version
				)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (event_id) DO UPDATE
				SET target_event_id = EXCLUDED.target_event_id,
				    reactor_pubkey = EXCLUDED.reactor_pubkey,
				    content = EXCLUDED.content,
				    created_at = EXCLUDED.created_at,
				    derivation_version = EXCLUDED.derivation_version,
				    projected_at = now()
			`, source.EventID, targetEventID, source.Pubkey, source.Content, source.CreatedAt, writeVersion)
			if err != nil {
				return fmt.Errorf("upsert reaction event: %w", err)
			}
			return nil
		},
	)
}

func (h *Handlers) ProjectRepostEvents(ctx context.Context, eventID string) error {
	return h.projectRepostEventsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectRepostEventsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectInteractionEvent(
		ctx,
		eventID,
		6,
		DerivationRepostEvents,
		RepostEventsVersion,
		"Project repost_events records from kind=6 references",
		versionOverride,
		func(tx pgx.Tx, source interactionSource, targetEventID string, writeVersion int) error {
			quote := nullIfBlank(source.Content)
			_, err := tx.Exec(ctx, `
				INSERT INTO repost_events (
					event_id, target_event_id, reposter_pubkey, quote, created_at, derivation_version
				)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (event_id) DO UPDATE
				SET target_event_id = EXCLUDED.target_event_id,
				    reposter_pubkey = EXCLUDED.reposter_pubkey,
				    quote = EXCLUDED.quote,
				    created_at = EXCLUDED.created_at,
				    derivation_version = EXCLUDED.derivation_version,
				    projected_at = now()
			`, source.EventID, targetEventID, source.Pubkey, quote, source.CreatedAt, writeVersion)
			if err != nil {
				return fmt.Errorf("upsert repost event: %w", err)
			}
			return nil
		},
	)
}

func (h *Handlers) ProjectDeletionEvents(ctx context.Context, eventID string) error {
	return h.projectDeletionEventsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectDeletionEventsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectInteractionEvent(
		ctx,
		eventID,
		5,
		DerivationDeletionEvents,
		DeletionEventsVersion,
		"Project deletion_events records from kind=5 references",
		versionOverride,
		func(tx pgx.Tx, source interactionSource, targetEventID string, writeVersion int) error {
			_, err := tx.Exec(ctx, `
				INSERT INTO deletion_events (
					event_id, deleter_pubkey, target_event_id, created_at, derivation_version
				)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (event_id) DO UPDATE
				SET deleter_pubkey = EXCLUDED.deleter_pubkey,
				    target_event_id = EXCLUDED.target_event_id,
				    created_at = EXCLUDED.created_at,
				    derivation_version = EXCLUDED.derivation_version,
				    projected_at = now()
			`, source.EventID, source.Pubkey, targetEventID, source.CreatedAt, writeVersion)
			if err != nil {
				return fmt.Errorf("upsert deletion event: %w", err)
			}
			if err := h.reconcileDMUnreadForDeletedTarget(ctx, tx, targetEventID); err != nil {
				return err
			}
			return nil
		},
	)
}

type interactionSource struct {
	EventID   string
	Pubkey    string
	Kind      int
	CreatedAt int64
	Content   string
	Tags      [][]string
}

type interactionUpserter func(tx pgx.Tx, source interactionSource, targetEventID string, writeVersion int) error

func (h *Handlers) projectInteractionEvent(
	ctx context.Context,
	eventID string,
	requiredKind int,
	derivationName string,
	derivationVersion int,
	derivationDescription string,
	versionOverride *int,
	upsert interactionUpserter,
) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	if upsert == nil {
		return fmt.Errorf("upsert handler is required")
	}

	var source interactionSource
	source.EventID = eventID
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind, created_at, content
		FROM events
		WHERE id = $1
	`, eventID).Scan(&source.Pubkey, &source.Kind, &source.CreatedAt, &source.Content); err != nil {
		return fmt.Errorf("load interaction source event: %w", err)
	}
	rawTags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	source.Tags = rawTags
	references := deriveEventReferences(eventID, rawTags)
	targetEventID := ""
	for _, ref := range references {
		targetEventID = strings.TrimSpace(ref.Referenced)
		if targetEventID != "" {
			break
		}
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		derivationName,
		derivationVersion,
		derivationDescription,
		versionOverride,
	)
	if err != nil {
		return err
	}

	tableName := strings.TrimSuffix(derivationName, "_latest")
	if source.Kind != requiredKind || targetEventID == "" {
		_, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE event_id = $1`, tableName), eventID)
		if err != nil {
			return fmt.Errorf("delete %s row: %w", tableName, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}
		return nil
	}

	if err := upsert(tx, source, targetEventID, writeVersion); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}
