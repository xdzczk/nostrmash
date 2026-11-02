package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// bookmarkTarget is one public save. eventID is a kind-1 or kind-30023 id
// when the tag is an e tag, or when an a tag already resolves. address is
// set for kind-30023 a tags (30023:pubkey:d).
type bookmarkTarget struct {
	eventID string
	address string
	pubkey  string
	dTag    string
}

// ProjectBookmarkSignals keeps the saved-note rank current. A bookmark list
// rewrites that person's edges. A note or article that shows up later picks
// up saves that were already pointing at it.
func (h *Handlers) ProjectBookmarkSignals(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	var kind int
	err := h.pool.QueryRow(ctx, `SELECT kind FROM events WHERE id = $1`, eventID).Scan(&kind)
	if err != nil {
		return fmt.Errorf("load event kind for bookmark signals: %w", err)
	}
	switch kind {
	case 10003, 30003:
		return h.projectBookmarkListsWithVersion(ctx, eventID, nil)
	case 1, 30023:
		return h.refreshBookmarkStatsForEvent(ctx, eventID)
	default:
		return nil
	}
}

func (h *Handlers) rebuildBookmarkListsWithVersion(ctx context.Context, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventIDs, err := h.queryEventIDs(ctx, `
		SELECT event_id
		FROM replaceable_state
		WHERE kind IN (10003, 30003)
		ORDER BY created_at ASC, event_id ASC
	`)
	if err != nil {
		return err
	}
	return h.rebuildProjectEventIDs(ctx, DerivationBookmarkLists, eventIDs, h.projectBookmarkListsWithVersion, versionOverride)
}

func (h *Handlers) projectBookmarkListsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var kind int
	var createdAt int64
	var dTag string
	err := h.pool.QueryRow(ctx, `
		SELECT e.pubkey, e.kind, e.created_at,
		       COALESCE((
		           SELECT et.value
		           FROM event_tags et
		           WHERE et.event_id = e.id
		             AND et.tag_name = 'd'
		             AND et.value_index = 0
		           ORDER BY et.tag_index ASC
		           LIMIT 1
		       ), '')
		FROM events e
		WHERE e.id = $1
	`, eventID).Scan(&pubkey, &kind, &createdAt, &dTag)
	if err != nil {
		return fmt.Errorf("load bookmark list event: %w", err)
	}
	if kind != 10003 && kind != 30003 {
		return nil
	}
	// Keep the same d tag replaceable_state stored. A normal kind-10003
	// list has none, so the key is empty. Kind 30003 is the d tag.

	var winnerID string
	err = h.pool.QueryRow(ctx, `
		SELECT event_id
		FROM replaceable_state
		WHERE pubkey = $1 AND kind = $2 AND d_tag = $3
	`, pubkey, kind, dTag).Scan(&winnerID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load bookmark list winner: %w", err)
	}
	if winnerID != eventID {
		return nil
	}

	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	targets := bookmarkTargetsFromTags(tags)

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin bookmark list tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockPubkeyForWriteTx(ctx, tx, pubkey, pubkeyLockNamespaceBookmarkLists); err != nil {
		return err
	}
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationBookmarkLists,
		BookmarkListsVersion,
		"Project public bookmark lists into distinct saved-note counts",
		versionOverride,
	)
	if err != nil {
		return err
	}

	if err := resolveBookmarkAddressesTx(ctx, tx, targets); err != nil {
		return err
	}
	previous, err := loadBookmarkEdgeTargetsTx(ctx, tx, pubkey, kind, dTag)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM bookmark_edges
		WHERE bookmarker_pubkey = $1
		  AND source_kind = $2
		  AND source_d_tag = $3
	`, pubkey, kind, dTag); err != nil {
		return fmt.Errorf("delete bookmark edges: %w", err)
	}
	if err := insertBookmarkEdgesTx(ctx, tx, pubkey, kind, dTag, eventID, createdAt, writeVersion, targets); err != nil {
		return err
	}

	affected := make([]string, 0, len(previous)+len(targets))
	affected = append(affected, previous...)
	for _, target := range targets {
		if target.eventID != "" {
			affected = append(affected, target.eventID)
		}
	}
	if err := recomputeBookmarkStatsTx(ctx, tx, affected, writeVersion); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit bookmark list tx: %w", err)
	}
	return nil
}

func (h *Handlers) refreshBookmarkStatsForEvent(ctx context.Context, eventID string) error {
	var pubkey string
	var kind int
	err := h.pool.QueryRow(ctx, `SELECT pubkey, kind FROM events WHERE id = $1`, eventID).Scan(&pubkey, &kind)
	if err != nil {
		return fmt.Errorf("load event for bookmark stats: %w", err)
	}
	if kind != 1 && kind != 30023 {
		return nil
	}
	address := ""
	if kind == 30023 {
		tags, err := h.loadEventTags(ctx, eventID)
		if err != nil {
			return err
		}
		if dTag := strings.TrimSpace(firstTagValue(tags, "d")); dTag != "" {
			address = "30023:" + strings.ToLower(pubkey) + ":" + dTag
		}
	}
	var exists bool
	if err := h.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM bookmark_edges
			WHERE target_event_id = $1
			   OR ($2 <> '' AND target_address = $2)
		)
	`, eventID, address).Scan(&exists); err != nil {
		return fmt.Errorf("check bookmark edges for event: %w", err)
	}
	if !exists {
		return nil
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin bookmark stats tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationBookmarkLists,
		BookmarkListsVersion,
		"Project public bookmark lists into distinct saved-note counts",
		nil,
	)
	if err != nil {
		return err
	}
	if err := recomputeBookmarkStatsTx(ctx, tx, []string{eventID}, writeVersion); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit bookmark stats tx: %w", err)
	}
	return nil
}

func bookmarkTargetsFromTags(tags [][]string) []bookmarkTarget {
	seen := make(map[string]struct{})
	out := make([]bookmarkTarget, 0)
	for _, tag := range tags {
		if len(tag) < 2 {
			continue
		}
		switch tag[0] {
		case "e":
			value := strings.ToLower(strings.TrimSpace(tag[1]))
			if !isHex64(value) {
				continue
			}
			key := value + "\n"
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, bookmarkTarget{eventID: value})
		case "a":
			address, pubkey, dTag, ok := parseBookmarkAddress(tag[1])
			if !ok {
				continue
			}
			key := "\n" + address
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, bookmarkTarget{address: address, pubkey: pubkey, dTag: dTag})
		}
	}
	return out
}

func parseBookmarkAddress(raw string) (address, pubkey, dTag string, ok bool) {
	raw = strings.TrimSpace(raw)
	const prefix = "30023:"
	if len(raw) <= len(prefix) || !strings.EqualFold(raw[:len(prefix)], prefix) {
		return "", "", "", false
	}
	rest := raw[len(prefix):]
	pubkey, dTag, found := strings.Cut(rest, ":")
	if !found || strings.TrimSpace(dTag) == "" || !isHex64(pubkey) {
		return "", "", "", false
	}
	pubkey = strings.ToLower(pubkey)
	dTag = strings.TrimSpace(dTag)
	return prefix + pubkey + ":" + dTag, pubkey, dTag, true
}

func isHex64(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f', c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

func resolveBookmarkAddressesTx(ctx context.Context, tx pgx.Tx, targets []bookmarkTarget) error {
	pubkeys := make([]string, 0)
	dTags := make([]string, 0)
	for _, target := range targets {
		if target.address == "" {
			continue
		}
		pubkeys = append(pubkeys, target.pubkey)
		dTags = append(dTags, target.dTag)
	}
	if len(pubkeys) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `
		SELECT rs.pubkey, rs.d_tag, rs.event_id
		FROM replaceable_state rs
		JOIN unnest($1::text[], $2::text[]) AS u(pubkey, d_tag)
		  ON rs.kind = 30023
		 AND rs.pubkey = u.pubkey
		 AND rs.d_tag = u.d_tag
	`, pubkeys, dTags)
	if err != nil {
		return fmt.Errorf("resolve bookmark addresses: %w", err)
	}
	defer rows.Close()
	resolved := make(map[string]string)
	for rows.Next() {
		var rowPubkey, rowDTag, eventID string
		if err := rows.Scan(&rowPubkey, &rowDTag, &eventID); err != nil {
			return fmt.Errorf("scan resolved bookmark address: %w", err)
		}
		resolved["30023:"+strings.ToLower(rowPubkey)+":"+rowDTag] = eventID
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read resolved bookmark addresses: %w", err)
	}
	for i := range targets {
		if targets[i].address == "" {
			continue
		}
		if eventID := resolved[targets[i].address]; eventID != "" {
			targets[i].eventID = eventID
		}
	}
	return nil
}

func loadBookmarkEdgeTargetsTx(ctx context.Context, tx pgx.Tx, pubkey string, kind int, dTag string) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT target_event_id, target_address
		FROM bookmark_edges
		WHERE bookmarker_pubkey = $1
		  AND source_kind = $2
		  AND source_d_tag = $3
	`, pubkey, kind, dTag)
	if err != nil {
		return nil, fmt.Errorf("load prior bookmark edges: %w", err)
	}
	defer rows.Close()
	ids := make([]string, 0)
	addresses := make([]bookmarkTarget, 0)
	for rows.Next() {
		var eventID, address string
		if err := rows.Scan(&eventID, &address); err != nil {
			return nil, fmt.Errorf("scan prior bookmark edge: %w", err)
		}
		if eventID != "" {
			ids = append(ids, eventID)
		}
		if address != "" {
			if parsed, pubkey, dTag, ok := parseBookmarkAddress(address); ok {
				addresses = append(addresses, bookmarkTarget{address: parsed, pubkey: pubkey, dTag: dTag})
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read prior bookmark edges: %w", err)
	}
	resolved, err := resolveAddressEventIDsTx(ctx, tx, addresses)
	if err != nil {
		return nil, err
	}
	return append(ids, resolved...), nil
}

func resolveAddressEventIDsTx(ctx context.Context, tx pgx.Tx, targets []bookmarkTarget) ([]string, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	if err := resolveBookmarkAddressesTx(ctx, tx, targets); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(targets))
	for _, target := range targets {
		if target.eventID != "" {
			ids = append(ids, target.eventID)
		}
	}
	return ids, nil
}

func insertBookmarkEdgesTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	kind int,
	dTag string,
	listEventID string,
	createdAt int64,
	version int,
	targets []bookmarkTarget,
) error {
	if len(targets) == 0 {
		return nil
	}
	eventIDs := make([]string, len(targets))
	addresses := make([]string, len(targets))
	for i, target := range targets {
		eventIDs[i] = target.eventID
		addresses[i] = target.address
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO bookmark_edges (
			bookmarker_pubkey,
			target_event_id,
			target_address,
			source_kind,
			source_d_tag,
			list_event_id,
			list_created_at,
			derivation_version
		)
		SELECT $1, u.event_id, u.address, $2, $3, $4, $5, $6
		FROM unnest($7::text[], $8::text[]) AS u(event_id, address)
		WHERE u.event_id <> '' OR u.address <> ''
		ON CONFLICT (bookmarker_pubkey, target_event_id, target_address, source_kind, source_d_tag)
		DO UPDATE
		SET list_event_id = EXCLUDED.list_event_id,
		    list_created_at = EXCLUDED.list_created_at,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, kind, dTag, listEventID, createdAt, version, eventIDs, addresses); err != nil {
		return fmt.Errorf("insert bookmark edges: %w", err)
	}
	return nil
}

func recomputeBookmarkStatsTx(ctx context.Context, tx pgx.Tx, eventIDs []string, version int) error {
	eventIDs = normalizeUniqueIDs(eventIDs)
	if len(eventIDs) == 0 {
		return nil
	}
	for _, eventID := range eventIDs {
		if err := lockPubkeyForWriteTx(ctx, tx, eventID, pubkeyLockNamespaceBookmarkStats); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO note_bookmark_stats (
			event_id, author_pubkey, kind, bookmark_count, derivation_version, projected_at
		)
		SELECT e.id,
		       e.pubkey,
		       e.kind,
		       (
		           SELECT COUNT(DISTINCT be.bookmarker_pubkey)
		           FROM bookmark_edges be
		           WHERE be.bookmarker_pubkey <> e.pubkey
		             AND (
		               be.target_event_id = e.id
		               OR (
		                 e.kind = 30023
		                 AND be.target_address = (
		                   '30023:' || lower(e.pubkey) || ':' || COALESCE((
		                     SELECT et.value
		                     FROM event_tags et
		                     WHERE et.event_id = e.id
		                       AND et.tag_name = 'd'
		                       AND et.value_index = 0
		                     ORDER BY et.tag_index ASC
		                     LIMIT 1
		                   ), '')
		                 )
		               )
		             )
		       ),
		       $2,
		       now()
		FROM events e
		WHERE e.id = ANY($1::text[])
		  AND e.kind IN (1, 30023)
		ON CONFLICT (event_id) DO UPDATE
		SET author_pubkey = EXCLUDED.author_pubkey,
		    kind = EXCLUDED.kind,
		    bookmark_count = EXCLUDED.bookmark_count,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`, eventIDs, version); err != nil {
		return fmt.Errorf("upsert bookmark stats: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM note_bookmark_stats
		WHERE event_id = ANY($1::text[])
		  AND bookmark_count <= 0
	`, eventIDs); err != nil {
		return fmt.Errorf("delete empty bookmark stats: %w", err)
	}
	return nil
}
