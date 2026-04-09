package derivation

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
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
		for _, ref := range references {
			if ref.Relation != "reply" {
				continue
			}
			targetPubkey, err := h.eventPubkeyTx(ctx, tx, ref.Referenced)
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
		for _, ref := range references {
			if ref.Relation == "reply" {
				ids = append(ids, ref.Referenced)
			}
		}
		rows, err := tx.Query(ctx, `
			SELECT target_event_id
			FROM reply_count_contributions
			WHERE source_event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing reply targets: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetEventID string
			if err := rows.Scan(&targetEventID); err != nil {
				return nil, fmt.Errorf("scan existing reply target: %w", err)
			}
			ids = append(ids, targetEventID)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing reply targets: %w", err)
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
		var priorTargetEventID *string
		if err := tx.QueryRow(ctx, `
			SELECT event_id
			FROM zap_receipts
			WHERE zap_receipt_id = $1
		`, sourceEventID).Scan(&priorTargetEventID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("load prior zap target: %w", err)
		}
		if priorTargetEventID != nil {
			ids = append(ids, *priorTargetEventID)
		}
	}
	return normalizeUniqueIDs(ids), nil
}
