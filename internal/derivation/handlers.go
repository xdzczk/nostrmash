package derivation

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handlers struct {
	pool *pgxpool.Pool
}

type EventJobPayload struct {
	EventID string `json:"event_id"`
}

func NewHandlers(pool *pgxpool.Pool) *Handlers {
	return &Handlers{pool: pool}
}

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

	if err := upsertDerivationVersion(ctx, tx, DerivationEventRelationships, EventRelationshipsVersion, "Derive e/p references with v1 root/reply/mention semantics"); err != nil {
		return err
	}

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

	if err := upsertDerivationVersion(ctx, tx, DerivationReplaceableState, ReplaceableStateVersion, "Track deterministic latest-wins replaceable event state"); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
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
	`,
		event.Pubkey,
		event.Kind,
		event.DTag,
		eventID,
		event.CreatedAt,
		ReplaceableStateVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert replaceable state: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit replaceable derivation tx: %w", err)
	}
	return nil
}

func (h *Handlers) ProjectProfilesLatest(ctx context.Context, eventID string) error {
	return h.projectProfilesLatestWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectProfilesLatestWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var kind int
	err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &kind)
	if err != nil {
		return fmt.Errorf("load event for profile projection: %w", err)
	}
	if kind != 0 {
		return nil
	}

	type metadataWinner struct {
		EventID   string
		CreatedAt int64
		Content   string
	}
	var winner metadataWinner
	err = h.pool.QueryRow(ctx, `
		SELECT id, created_at, content
		FROM events
		WHERE pubkey = $1
		  AND kind = 0
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, pubkey).Scan(&winner.EventID, &winner.CreatedAt, &winner.Content)
	if err != nil {
		return fmt.Errorf("load latest metadata event: %w", err)
	}

	profileJSON := json.RawMessage(`{}`)
	if strings.TrimSpace(winner.Content) != "" {
		var profile any
		if err := json.Unmarshal([]byte(winner.Content), &profile); err == nil {
			encoded, marshalErr := json.Marshal(profile)
			if marshalErr == nil {
				profileJSON = encoded
			}
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
		DerivationProfilesLatest,
		ProfilesLatestVersion,
		"Project latest effective replaceable metadata (kind 0) per pubkey",
		versionOverride,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO profiles_latest (
			pubkey, metadata_event_id, metadata_created_at, profile_json, derivation_version
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (pubkey) DO UPDATE
		SET metadata_event_id = EXCLUDED.metadata_event_id,
		    metadata_created_at = EXCLUDED.metadata_created_at,
		    profile_json = EXCLUDED.profile_json,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`,
		pubkey,
		winner.EventID,
		winner.CreatedAt,
		profileJSON,
		writeVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert profiles_latest: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit profile projection tx: %w", err)
	}
	return nil
}

func (h *Handlers) ProjectAuthorRecentEvent(ctx context.Context, eventID string) error {
	return h.projectAuthorRecentEventWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectAuthorRecentEventWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var createdAt int64
	err := h.pool.QueryRow(ctx, `
		SELECT pubkey, created_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &createdAt)
	if err != nil {
		return fmt.Errorf("load event for author projection: %w", err)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorRecentEvents,
		AuthorRecentEventsVersion,
		"Project author recent events ordered by created_at desc, id desc",
		versionOverride,
	)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO author_recent_events (
			author_pubkey, event_id, created_at, derivation_version
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (author_pubkey, event_id) DO UPDATE
		SET created_at = EXCLUDED.created_at,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`,
		pubkey,
		eventID,
		createdAt,
		writeVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert author_recent_events: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit author projection tx: %w", err)
	}
	return nil
}

func (h *Handlers) UpdateThreadProjection(ctx context.Context, eventID string) error {
	return h.updateThreadProjectionWithVersion(ctx, eventID, nil)
}

func (h *Handlers) updateThreadProjectionWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var kind int
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT kind, created_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind, &createdAt); err != nil {
		return fmt.Errorf("load event for thread projection: %w", err)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationThreadProjection,
		ThreadProjectionVersion,
		"Project reply parent/root edges with unresolved reference tracking",
		versionOverride,
	)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM unresolved_thread_references WHERE source_event_id = $1`, eventID); err != nil {
		return fmt.Errorf("delete unresolved thread references: %w", err)
	}

	if kind != 1 {
		if _, err := tx.Exec(ctx, `DELETE FROM thread_edges WHERE child_event_id = $1`, eventID); err != nil {
			return fmt.Errorf("delete non-thread edge: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit non-thread projection tx: %w", err)
		}
		return nil
	}

	rawTags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	refs := deriveEventReferences(eventID, rawTags)
	parentEventID := firstReferenceByRelation(refs, "reply")
	rootEventID := firstReferenceByRelation(refs, "root")
	if parentEventID == "" {
		parentEventID = rootEventID
	}
	if rootEventID == "" {
		rootEventID = parentEventID
	}

	if parentEventID == "" {
		if _, err := tx.Exec(ctx, `DELETE FROM thread_edges WHERE child_event_id = $1`, eventID); err != nil {
			return fmt.Errorf("delete thread edge with no parent: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit thread projection tx with no parent: %w", err)
		}
		return nil
	}

	parentExists, err := eventExistsTx(ctx, tx, parentEventID)
	if err != nil {
		return err
	}
	parentMissing := !parentExists
	rootMissing := false
	if rootEventID != "" {
		if rootEventID == parentEventID {
			rootMissing = parentMissing
		} else {
			rootExists, existsErr := eventExistsTx(ctx, tx, rootEventID)
			if existsErr != nil {
				return existsErr
			}
			rootMissing = !rootExists
		}
	}

	if parentMissing {
		if _, err := tx.Exec(ctx, `
			INSERT INTO unresolved_thread_references (
				source_event_id, missing_event_id, relation, derivation_version
			)
			VALUES ($1, $2, 'reply', $3)
			ON CONFLICT (source_event_id, missing_event_id, relation) DO UPDATE
			SET derivation_version = EXCLUDED.derivation_version,
			    detected_at = now()
		`, eventID, parentEventID, writeVersion); err != nil {
			return fmt.Errorf("upsert unresolved reply reference: %w", err)
		}
	}
	if rootEventID != "" && rootMissing {
		if _, err := tx.Exec(ctx, `
			INSERT INTO unresolved_thread_references (
				source_event_id, missing_event_id, relation, derivation_version
			)
			VALUES ($1, $2, 'root', $3)
			ON CONFLICT (source_event_id, missing_event_id, relation) DO UPDATE
			SET derivation_version = EXCLUDED.derivation_version,
			    detected_at = now()
		`, eventID, rootEventID, writeVersion); err != nil {
			return fmt.Errorf("upsert unresolved root reference: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO thread_edges (
			child_event_id, child_created_at, parent_event_id, root_event_id, parent_missing, root_missing, derivation_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (child_event_id) DO UPDATE
		SET child_created_at = EXCLUDED.child_created_at,
		    parent_event_id = EXCLUDED.parent_event_id,
		    root_event_id = EXCLUDED.root_event_id,
		    parent_missing = EXCLUDED.parent_missing,
		    root_missing = EXCLUDED.root_missing,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`,
		eventID,
		createdAt,
		parentEventID,
		nullIfBlank(rootEventID),
		parentMissing,
		rootMissing,
		writeVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert thread edge: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit thread projection tx: %w", err)
	}
	return nil
}

func (h *Handlers) RepairUnresolvedReferences(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := upsertDerivationVersion(ctx, tx, DerivationThreadProjection, ThreadProjectionVersion, "Repair unresolved thread references when missing events arrive"); err != nil {
		return err
	}

	rows, err := tx.Query(ctx, `
		SELECT DISTINCT source_event_id
		FROM unresolved_thread_references
		WHERE missing_event_id = $1
		ORDER BY source_event_id ASC
	`, eventID)
	if err != nil {
		return fmt.Errorf("query unresolved references for repair: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var sourceEventID string
		if err := rows.Scan(&sourceEventID); err != nil {
			return fmt.Errorf("scan unresolved reference source event: %w", err)
		}
		if err := enqueueDerivationJobTx(ctx, tx, JobTypeUpdateThreadProjection, sourceEventID, "repair:"+eventID); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read unresolved references for repair: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit repair unresolved references tx: %w", err)
	}
	return nil
}

func (h *Handlers) ProjectReplyCounts(ctx context.Context, eventID string) error {
	return h.projectReplyCountsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectReplyCountsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectCounts(
		ctx,
		eventID,
		DerivationReplyCounts,
		ReplyCountsVersion,
		"Project eventually-consistent reply counts from relation=reply references",
		"reply_count_contributions",
		"reply_counts",
		func(kind int, refs []derivedReference) []string {
			if kind != 1 {
				return nil
			}
			ids := make([]string, 0, len(refs))
			for _, ref := range refs {
				if ref.Relation != "reply" {
					continue
				}
				ids = append(ids, ref.Referenced)
			}
			return ids
		},
		versionOverride,
	)
}

func (h *Handlers) ProjectReactionCounts(ctx context.Context, eventID string) error {
	return h.projectReactionCountsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectReactionCountsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectCounts(
		ctx,
		eventID,
		DerivationReactionCounts,
		ReactionCountsVersion,
		"Project eventually-consistent reaction counts from kind=7 e references",
		"reaction_count_contributions",
		"reaction_counts",
		func(kind int, refs []derivedReference) []string {
			if kind != 7 {
				return nil
			}
			ids := make([]string, 0, len(refs))
			for _, ref := range refs {
				ids = append(ids, ref.Referenced)
			}
			return ids
		},
		versionOverride,
	)
}

func (h *Handlers) ProjectRepostCounts(ctx context.Context, eventID string) error {
	return h.projectRepostCountsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectRepostCountsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectCounts(
		ctx,
		eventID,
		DerivationRepostCounts,
		RepostCountsVersion,
		"Project eventually-consistent repost counts from kind=6 e references",
		"repost_count_contributions",
		"repost_counts",
		func(kind int, refs []derivedReference) []string {
			if kind != 6 {
				return nil
			}
			ids := make([]string, 0, len(refs))
			for _, ref := range refs {
				ids = append(ids, ref.Referenced)
			}
			return ids
		},
		versionOverride,
	)
}

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
			return nil
		},
	)
}

func (h *Handlers) ProjectContactListsLatest(ctx context.Context, eventID string) error {
	return h.projectContactListsLatestWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectContactListsLatestWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectReplaceableListLatest(
		ctx,
		eventID,
		3,
		DerivationContactListsLatest,
		ContactListsLatestVersion,
		"Project contact_lists_latest from kind=3 replaceables",
		versionOverride,
		func(tags [][]string) json.RawMessage {
			contacts := make([]string, 0)
			for _, tag := range tags {
				if len(tag) < 2 || tag[0] != "p" {
					continue
				}
				value := strings.TrimSpace(tag[1])
				if value == "" {
					continue
				}
				contacts = append(contacts, value)
			}
			contacts = normalizeUniqueIDs(contacts)
			if len(contacts) == 0 {
				return json.RawMessage(`[]`)
			}
			encoded, err := json.Marshal(contacts)
			if err != nil {
				return json.RawMessage(`[]`)
			}
			return encoded
		},
		func(tx pgx.Tx, pubkey, winnerID string, winnerCreatedAt int64, payload json.RawMessage, writeVersion int) error {
			_, err := tx.Exec(ctx, `
				INSERT INTO contact_lists_latest (
					pubkey, event_id, created_at, contacts_json, derivation_version
				)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (pubkey) DO UPDATE
				SET event_id = EXCLUDED.event_id,
				    created_at = EXCLUDED.created_at,
				    contacts_json = EXCLUDED.contacts_json,
				    derivation_version = EXCLUDED.derivation_version,
				    updated_at = now()
			`, pubkey, winnerID, winnerCreatedAt, payload, writeVersion)
			if err != nil {
				return fmt.Errorf("upsert contact_lists_latest: %w", err)
			}
			return nil
		},
	)
}

func (h *Handlers) ProjectRelayListsLatest(ctx context.Context, eventID string) error {
	return h.projectRelayListsLatestWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectRelayListsLatestWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectReplaceableListLatest(
		ctx,
		eventID,
		10002,
		DerivationRelayListsLatest,
		RelayListsLatestVersion,
		"Project relay_lists_latest from kind=10002 replaceables",
		versionOverride,
		func(tags [][]string) json.RawMessage {
			relays := make([]string, 0)
			for _, tag := range tags {
				if len(tag) < 2 || tag[0] != "r" {
					continue
				}
				value := strings.TrimSpace(tag[1])
				if value == "" {
					continue
				}
				relays = append(relays, value)
			}
			relays = normalizeUniqueIDs(relays)
			if len(relays) == 0 {
				return json.RawMessage(`[]`)
			}
			encoded, err := json.Marshal(relays)
			if err != nil {
				return json.RawMessage(`[]`)
			}
			return encoded
		},
		func(tx pgx.Tx, pubkey, winnerID string, winnerCreatedAt int64, payload json.RawMessage, writeVersion int) error {
			_, err := tx.Exec(ctx, `
				INSERT INTO relay_lists_latest (
					pubkey, event_id, created_at, relays_json, derivation_version
				)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (pubkey) DO UPDATE
				SET event_id = EXCLUDED.event_id,
				    created_at = EXCLUDED.created_at,
				    relays_json = EXCLUDED.relays_json,
				    derivation_version = EXCLUDED.derivation_version,
				    updated_at = now()
			`, pubkey, winnerID, winnerCreatedAt, payload, writeVersion)
			if err != nil {
				return fmt.Errorf("upsert relay_lists_latest: %w", err)
			}
			return nil
		},
	)
}

func (h *Handlers) projectCounts(
	ctx context.Context,
	eventID string,
	derivationName string,
	derivationVersion int,
	derivationDescription string,
	contributionTable string,
	countsTable string,
	projector func(kind int, refs []derivedReference) []string,
	versionOverride *int,
) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	if projector == nil {
		return fmt.Errorf("projector is required")
	}

	var kind int
	err := h.pool.QueryRow(ctx, `
		SELECT kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind)
	if err != nil {
		return fmt.Errorf("load event kind for %s: %w", derivationName, err)
	}

	rawTags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	references := deriveEventReferences(eventID, rawTags)
	referencedIDs := normalizeUniqueIDs(projector(kind, references))

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

	existing, err := readExistingContributions(ctx, tx, contributionTable, eventID)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE source_event_id = $1`, contributionTable), eventID); err != nil {
		return fmt.Errorf("delete prior contributions in %s: %w", contributionTable, err)
	}

	for _, targetEventID := range referencedIDs {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (
				source_event_id, target_event_id, derivation_version
			)
			VALUES ($1, $2, $3)
			ON CONFLICT (source_event_id, target_event_id) DO UPDATE
			SET derivation_version = EXCLUDED.derivation_version,
			    projected_at = now()
		`, contributionTable), eventID, targetEventID, writeVersion)
		if err != nil {
			return fmt.Errorf("insert contribution into %s: %w", contributionTable, err)
		}
	}

	affectedTargets := make(map[string]struct{}, len(existing)+len(referencedIDs))
	for _, targetEventID := range existing {
		affectedTargets[targetEventID] = struct{}{}
	}
	for _, targetEventID := range referencedIDs {
		affectedTargets[targetEventID] = struct{}{}
	}

	for targetEventID := range affectedTargets {
		var count int64
		if err := tx.QueryRow(ctx, fmt.Sprintf(`
			SELECT COUNT(*)
			FROM %s
			WHERE target_event_id = $1
		`, contributionTable), targetEventID).Scan(&count); err != nil {
			return fmt.Errorf("read aggregate from %s: %w", contributionTable, err)
		}
		if count == 0 {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE event_id = $1`, countsTable), targetEventID); err != nil {
				return fmt.Errorf("delete zero row from %s: %w", countsTable, err)
			}
			continue
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (
				event_id, count, derivation_version
			)
			VALUES ($1, $2, $3)
			ON CONFLICT (event_id) DO UPDATE
			SET count = EXCLUDED.count,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, countsTable), targetEventID, count, writeVersion)
		if err != nil {
			return fmt.Errorf("upsert row in %s: %w", countsTable, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s tx: %w", derivationName, err)
	}
	return nil
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

type replaceableListPayloadBuilder func(tags [][]string) json.RawMessage
type replaceableListUpserter func(tx pgx.Tx, pubkey, winnerID string, winnerCreatedAt int64, payload json.RawMessage, writeVersion int) error

func (h *Handlers) projectReplaceableListLatest(
	ctx context.Context,
	eventID string,
	requiredKind int,
	derivationName string,
	derivationVersion int,
	derivationDescription string,
	versionOverride *int,
	buildPayload replaceableListPayloadBuilder,
	upsert replaceableListUpserter,
) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	if buildPayload == nil || upsert == nil {
		return fmt.Errorf("projection handlers are not configured")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var kind int
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &kind); err != nil {
		return fmt.Errorf("load event for %s: %w", derivationName, err)
	}
	if kind != requiredKind {
		return nil
	}

	var winnerID string
	var winnerCreatedAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT id, created_at
		FROM events
		WHERE pubkey = $1
		  AND kind = $2
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, pubkey, requiredKind).Scan(&winnerID, &winnerCreatedAt); err != nil {
		return fmt.Errorf("load winner event for %s: %w", derivationName, err)
	}

	tags, err := h.loadEventTags(ctx, winnerID)
	if err != nil {
		return err
	}
	payload := buildPayload(tags)

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
	if err := upsert(tx, pubkey, winnerID, winnerCreatedAt, payload, writeVersion); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func readExistingContributions(ctx context.Context, tx pgx.Tx, table string, sourceEventID string) ([]string, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT target_event_id
		FROM %s
		WHERE source_event_id = $1
	`, table), sourceEventID)
	if err != nil {
		return nil, fmt.Errorf("load existing contributions from %s: %w", table, err)
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var targetEventID string
		if err := rows.Scan(&targetEventID); err != nil {
			return nil, fmt.Errorf("scan contribution from %s: %w", table, err)
		}
		out = append(out, targetEventID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read contributions from %s: %w", table, err)
	}
	return out, nil
}

func normalizeUniqueIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	slices.Sort(out)
	return out
}

func firstReferenceByRelation(refs []derivedReference, relation string) string {
	for _, ref := range refs {
		if ref.Relation == relation {
			return ref.Referenced
		}
	}
	return ""
}

func eventExistsTx(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)
	`, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check referenced event existence: %w", err)
	}
	return exists, nil
}

func enqueueDerivationJobTx(ctx context.Context, tx pgx.Tx, jobType, eventID, idempotencySuffix string) error {
	payload, err := json.Marshal(EventJobPayload{EventID: eventID})
	if err != nil {
		return fmt.Errorf("encode %s payload for event %s: %w", jobType, eventID, err)
	}
	idempotencyKey := fmt.Sprintf("%s:%s", jobType, eventID)
	if trimmed := strings.TrimSpace(idempotencySuffix); trimmed != "" {
		idempotencyKey = fmt.Sprintf("%s:%s", idempotencyKey, trimmed)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (job_type, payload, idempotency_key, max_attempts, run_after)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
	`,
		jobType,
		json.RawMessage(payload),
		idempotencyKey,
		5,
	)
	if err != nil {
		return fmt.Errorf("enqueue %s for event %s: %w", jobType, eventID, err)
	}
	return nil
}

type derivedReference struct {
	SourceEventID string
	Referenced    string
	Relation      string
	TagIndex      int
	RelayHint     string
	Marker        string
}

func (h *Handlers) loadEventTags(ctx context.Context, eventID string) ([][]string, error) {
	var rawEvent string
	if err := h.pool.QueryRow(ctx, `SELECT raw_json::text FROM events WHERE id = $1`, eventID).Scan(&rawEvent); err != nil {
		return nil, fmt.Errorf("load raw event for derivation: %w", err)
	}
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(rawEvent), &payload); err != nil {
		return nil, fmt.Errorf("decode event tags: %w", err)
	}
	return payload.Tags, nil
}

func deriveEventReferences(sourceEventID string, tags [][]string) []derivedReference {
	return deriveReferencesByTagName(sourceEventID, tags, "e")
}

func derivePubkeyReferences(sourceEventID string, tags [][]string) []derivedReference {
	return deriveReferencesByTagName(sourceEventID, tags, "p")
}

func deriveReferencesByTagName(sourceEventID string, tags [][]string, tagName string) []derivedReference {
	refs := make([]derivedReference, 0)
	unmarkedIdx := make([]int, 0)

	for i, tag := range tags {
		if len(tag) < 2 || tag[0] != tagName {
			continue
		}
		referenced := strings.TrimSpace(tag[1])
		if referenced == "" {
			continue
		}

		relayHint := ""
		if len(tag) > 2 {
			relayHint = strings.TrimSpace(tag[2])
		}
		marker := ""
		if len(tag) > 3 {
			marker = strings.TrimSpace(tag[3])
		}
		relation, marked := ParseRelationMarker(marker)
		if marked {
			refs = append(refs, derivedReference{
				SourceEventID: sourceEventID,
				Referenced:    referenced,
				Relation:      relation,
				TagIndex:      i,
				RelayHint:     relayHint,
				Marker:        relation,
			})
			continue
		}

		refs = append(refs, derivedReference{
			SourceEventID: sourceEventID,
			Referenced:    referenced,
			TagIndex:      i,
			RelayHint:     relayHint,
		})
		unmarkedIdx = append(unmarkedIdx, len(refs)-1)
	}

	assignLegacyRelations(refs, unmarkedIdx)
	filtered := refs[:0]
	for _, ref := range refs {
		if ref.Relation == "" {
			continue
		}
		filtered = append(filtered, ref)
	}
	return filtered
}

func assignLegacyRelations(refs []derivedReference, unmarkedIdx []int) {
	if len(unmarkedIdx) == 0 {
		return
	}

	for _, idx := range unmarkedIdx {
		refs[idx].Relation = "mention"
	}

	rootSet := false
	replySet := false
	for _, ref := range refs {
		switch ref.Relation {
		case "root":
			rootSet = true
		case "reply":
			replySet = true
		}
	}

	if !rootSet {
		first := unmarkedIdx[0]
		refs[first].Relation = "root"
	}
	if !replySet && len(unmarkedIdx) > 1 {
		last := unmarkedIdx[len(unmarkedIdx)-1]
		refs[last].Relation = "reply"
	}
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

func upsertDerivationVersion(
	ctx context.Context,
	tx pgx.Tx,
	name string,
	version int,
	description string,
) error {
	codeVersion := strings.TrimSpace(os.Getenv("APP_VERSION"))
	if codeVersion == "" {
		codeVersion = "dev"
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO derivation_versions (projection_name, version, code_version, description, activated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (projection_name, version) DO UPDATE
		SET code_version = EXCLUDED.code_version,
		    description = EXCLUDED.description
	`,
		name,
		version,
		codeVersion,
		description,
	)
	if err != nil {
		return fmt.Errorf("upsert derivation version %q: %w", name, err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO derivation_active_versions (
			derivation_name, active_version, target_version, description
		)
		VALUES ($1, $2, $2, $3)
		ON CONFLICT (derivation_name) DO UPDATE
		SET target_version = EXCLUDED.target_version,
		    description = EXCLUDED.description,
		    updated_at = now()
	`,
		name,
		version,
		description,
	)
	if err != nil {
		return fmt.Errorf("upsert derivation active version %q: %w", name, err)
	}
	return nil
}

func resolveDerivationWriteVersion(
	ctx context.Context,
	tx pgx.Tx,
	name string,
	targetVersion int,
	description string,
	versionOverride *int,
) (int, error) {
	if err := upsertDerivationVersion(ctx, tx, name, targetVersion, description); err != nil {
		return 0, err
	}
	if versionOverride != nil {
		return *versionOverride, nil
	}
	var activeVersion int
	if err := tx.QueryRow(ctx, `
		SELECT active_version
		FROM derivation_active_versions
		WHERE derivation_name = $1
	`, name).Scan(&activeVersion); err != nil {
		return 0, fmt.Errorf("load active derivation version %q: %w", name, err)
	}
	return activeVersion, nil
}

func nullIfBlank(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}
