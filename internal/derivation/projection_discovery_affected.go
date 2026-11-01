package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

func (h *Handlers) affectedProfileDiscoveryPubkeysTx(
	ctx context.Context,
	tx pgx.Tx,
	sourceEventID string,
	kind int,
	references []derivedReference,
	tags [][]string,
) ([]string, error) {
	pubkeys := make([]string, 0, 8)

	var sourcePubkey string
	if err := tx.QueryRow(ctx, `
		SELECT pubkey
		FROM events
		WHERE id = $1
	`, sourceEventID).Scan(&sourcePubkey); err != nil {
		return nil, fmt.Errorf("load source event pubkey: %w", err)
	}
	pubkeys = append(pubkeys, sourcePubkey)

	switch kind {
	case 1:
		for _, targetID := range replyAffectTargets(references) {
			targetPubkey, err := h.eventPubkeyTx(ctx, tx, targetID)
			if err != nil {
				return nil, err
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		rows, err := tx.Query(ctx, `
			SELECT e.pubkey
			FROM reply_count_contributions c
			JOIN events e ON e.id = c.target_event_id
			WHERE c.source_event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing profile discovery reply targets: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetPubkey string
			if err := rows.Scan(&targetPubkey); err != nil {
				return nil, fmt.Errorf("scan existing profile discovery reply target: %w", err)
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing profile discovery reply targets: %w", err)
		}
	case 6:
		for _, ref := range references {
			targetPubkey, err := h.eventPubkeyTx(ctx, tx, ref.Referenced)
			if err != nil {
				return nil, err
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		rows, err := tx.Query(ctx, `
			SELECT e.pubkey
			FROM repost_events r
			JOIN events e ON e.id = r.target_event_id
			WHERE r.event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing profile discovery repost targets: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetPubkey string
			if err := rows.Scan(&targetPubkey); err != nil {
				return nil, fmt.Errorf("scan existing profile discovery repost target: %w", err)
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing profile discovery repost targets: %w", err)
		}
	case 7:
		for _, ref := range references {
			targetPubkey, err := h.eventPubkeyTx(ctx, tx, ref.Referenced)
			if err != nil {
				return nil, err
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		rows, err := tx.Query(ctx, `
			SELECT e.pubkey
			FROM reaction_events r
			JOIN events e ON e.id = r.target_event_id
			WHERE r.event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing profile discovery reaction targets: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetPubkey string
			if err := rows.Scan(&targetPubkey); err != nil {
				return nil, fmt.Errorf("scan existing profile discovery reaction target: %w", err)
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing profile discovery reaction targets: %w", err)
		}
	case 9735:
		pubkeys = append(pubkeys, firstTagValue(tags, "p"))
		var priorReceiverPubkey *string
		if err := tx.QueryRow(ctx, `
			SELECT receiver_pubkey
			FROM zap_receipts
			WHERE zap_receipt_id = $1
		`, sourceEventID).Scan(&priorReceiverPubkey); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("load prior profile discovery zap receiver: %w", err)
		}
		if priorReceiverPubkey != nil {
			pubkeys = append(pubkeys, *priorReceiverPubkey)
		}
	case 3:
		for _, tag := range tags {
			if len(tag) < 2 || tag[0] != "p" {
				continue
			}
			pubkeys = append(pubkeys, tag[1])
		}
		rows, err := tx.Query(ctx, `
			SELECT followed_pubkey
			FROM follower_edges
			WHERE source_event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing profile discovery follow targets: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var followedPubkey string
			if err := rows.Scan(&followedPubkey); err != nil {
				return nil, fmt.Errorf("scan existing profile discovery follow target: %w", err)
			}
			pubkeys = append(pubkeys, followedPubkey)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing profile discovery follow targets: %w", err)
		}
	}
	return normalizeUniqueIDs(pubkeys), nil
}

func (h *Handlers) affectedNoteDiscoveryIDsTx(
	ctx context.Context,
	tx pgx.Tx,
	sourceEventID string,
	kind int,
	references []derivedReference,
	tags [][]string,
) ([]string, error) {
	ids := make([]string, 0, 8)
	if isNoteDiscoveryProjectableKind(kind) {
		ids = append(ids, sourceEventID)
	}
	switch kind {
	case 1:
		ids = append(ids, replyAffectTargets(references)...)
		contributionTargets, err := queryStringColumnTx(ctx, tx, `
			SELECT target_event_id
			FROM reply_count_contributions
			WHERE source_event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing reply targets: %w", err)
		}
		ids = append(ids, contributionTargets...)
		// Thread-wide discover reply totals live on the root summary; refresh
		// whatever root/parent the edge currently (or previously) pointed at.
		var rootEventID *string
		var parentEventID string
		err = tx.QueryRow(ctx, `
			SELECT root_event_id, parent_event_id
			FROM thread_edges
			WHERE child_event_id = $1
		`, sourceEventID).Scan(&rootEventID, &parentEventID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("load thread edge targets for note discovery: %w", err)
		}
		if err == nil {
			if rootEventID != nil {
				ids = append(ids, *rootEventID)
			}
			ids = append(ids, parentEventID)
		}
	case 6:
		for _, ref := range references {
			ids = append(ids, ref.Referenced)
		}
		rows, err := tx.Query(ctx, `
			SELECT target_event_id
			FROM repost_count_contributions
			WHERE source_event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing repost targets: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetEventID string
			if err := rows.Scan(&targetEventID); err != nil {
				return nil, fmt.Errorf("scan existing repost target: %w", err)
			}
			ids = append(ids, targetEventID)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing repost targets: %w", err)
		}
	case 7:
		for _, ref := range references {
			ids = append(ids, ref.Referenced)
		}
		rows, err := tx.Query(ctx, `
			SELECT target_event_id
			FROM reaction_count_contributions
			WHERE source_event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing reaction targets: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetEventID string
			if err := rows.Scan(&targetEventID); err != nil {
				return nil, fmt.Errorf("scan existing reaction target: %w", err)
			}
			ids = append(ids, targetEventID)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing reaction targets: %w", err)
		}
	case 9735:
		ids = append(ids, firstTagValue(tags, "e"))
		currentAddressID, err := replaceableEventIDByAddressTx(ctx, tx, firstAddress(tags, "a"))
		if err != nil {
			return nil, err
		}
		ids = append(ids, currentAddressID)
		var priorTargetEventID *string
		var priorAddress string
		if err := tx.QueryRow(ctx, `
			SELECT event_id, COALESCE(target_address, '')
			FROM zap_receipts
			WHERE zap_receipt_id = $1
		`, sourceEventID).Scan(&priorTargetEventID, &priorAddress); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("load prior zap target: %w", err)
		}
		if priorTargetEventID != nil {
			ids = append(ids, *priorTargetEventID)
		}
		priorAddressID, err := replaceableEventIDByAddressTx(ctx, tx, priorAddress)
		if err != nil {
			return nil, err
		}
		ids = append(ids, priorAddressID)
	}
	return normalizeUniqueIDs(ids), nil
}

func replaceableEventIDByAddressTx(ctx context.Context, tx pgx.Tx, address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", nil
	}
	addr, ok := nostr.ParseAddressCoordinate(address)
	if !ok {
		return "", nil
	}
	var eventID string
	err := tx.QueryRow(ctx, `
		SELECT event_id
		FROM replaceable_state
		WHERE kind = $1 AND lower(pubkey) = $2 AND d_tag = $3
	`, addr.Kind, addr.Pubkey, addr.DTag).Scan(&eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("resolve replaceable address %s: %w", addr.String(), err)
	}
	return eventID, nil
}
