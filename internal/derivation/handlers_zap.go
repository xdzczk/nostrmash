package derivation

import (
	"context"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

func (h *Handlers) ProjectZapReceipts(ctx context.Context, eventID string) error {
	return h.projectZapReceiptsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectZapReceiptsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var kind int
	var receiptPubkey string
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT kind, pubkey, created_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind, &receiptPubkey, &createdAt); err != nil {
		return fmt.Errorf("load event for zap projection: %w", err)
	}
	if kind != 9735 {
		return nil
	}

	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	// The event pubkey is the lightning service. The sent-zap page filters
	// on the person who paid.
	senderPubkey := zapReceiptSender(receiptPubkey, tags)
	receiver := firstTagValue(tags, "p")
	targetEventID := firstTagValue(tags, "e")
	targetAddress := ""
	if addr, ok := nostr.ParseAddressCoordinate(firstTagValue(tags, "a")); ok {
		targetAddress = addr.String()
	}
	amountSats := zapReceiptAmountSats(tags)

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationZapReceipts,
		ZapReceiptsVersion,
		"Project zap receipts by sender, receiver, target event, and sats",
		versionOverride,
	)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO zap_receipts (
			zap_receipt_id, created_at, event_id, sender_pubkey, receiver_pubkey, amount_sats, target_address, derivation_version, projected_at
		)
		VALUES ($1, $2, nullif($3, ''), $4, nullif($5, ''), $6, $7, $8, now())
		ON CONFLICT (zap_receipt_id) DO UPDATE
		SET created_at = EXCLUDED.created_at,
		    event_id = EXCLUDED.event_id,
		    sender_pubkey = EXCLUDED.sender_pubkey,
		    receiver_pubkey = EXCLUDED.receiver_pubkey,
		    amount_sats = EXCLUDED.amount_sats,
		    target_address = EXCLUDED.target_address,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`, eventID, createdAt, targetEventID, senderPubkey, receiver, amountSats, targetAddress, writeVersion); err != nil {
		return fmt.Errorf("upsert zap receipt: %w", err)
	}
	if err := h.applyZapEconomicsTx(ctx, tx, eventID, senderPubkey, receiver, createdAt, amountSats); err != nil {
		return err
	}
	if targetEventID != "" && amountSats > 0 {
		claimed, err := claimStatDeltaTx(ctx, tx, eventID, "zap_goal_progress")
		if err != nil {
			return err
		}
		if claimed {
			if _, err := tx.Exec(ctx, `
				UPDATE zap_goals SET raised_sats = raised_sats + $2 WHERE event_id = $1
			`, targetEventID, amountSats); err != nil {
				return fmt.Errorf("add zap goal progress: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE note_discovery_stats
			SET first_zap_at = LEAST(COALESCE(first_zap_at, $2), $2)
			WHERE event_id = $1 AND (first_zap_at IS NULL OR first_zap_at > $2)
		`, targetEventID, createdAt); err != nil {
			return fmt.Errorf("record first zap: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit zap projection tx: %w", err)
	}
	return nil
}

// rebuildZapReceiptsWithVersion reprojects kind-9735 receipts only so a full
// rebuild rewrites amount_sats without scanning every event kind.
func (h *Handlers) rebuildZapReceiptsWithVersion(ctx context.Context, versionOverride *int) error {
	return h.rebuildProjectKinds(ctx, DerivationZapReceipts, []int{9735}, h.projectZapReceiptsWithVersion, versionOverride)
}
