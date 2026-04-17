package derivation

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) ProjectProfilePublicStats(ctx context.Context, eventID string) error {
	return h.projectProfilePublicStatsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectProfilePublicStatsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey); err != nil {
		return fmt.Errorf("load event for profile public stats projection: %w", err)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := h.projectProfilePublicStatsForPubkeysTx(ctx, tx, []string{pubkey}, versionOverride); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit profile public stats projection tx: %w", err)
	}
	return nil
}

func (h *Handlers) projectProfilePublicStatsForPubkeysTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkeys []string,
	versionOverride *int,
) error {
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationProfilePublicStats,
		ProfilePublicStatsVersion,
		"Project public profile counters and recent activity",
		versionOverride,
	)
	if err != nil {
		return err
	}

	// normalizeUniqueIDs returns the pubkeys in sorted order, so concurrent
	// transactions that touch overlapping pubkey sets always acquire the
	// per-pubkey advisory locks in the same order and cannot deadlock.
	for _, pubkey := range normalizeUniqueIDs(pubkeys) {
		if err := lockPubkeyForWriteTx(ctx, tx, pubkey, pubkeyLockNamespaceProfilePublicStats); err != nil {
			return err
		}
		var followerCount int64
		var followingCount int64
		var noteCount int64
		var replyCount int64
		var recentActivityAt *int64
		if err := tx.QueryRow(ctx, `
			SELECT
				COALESCE((
					SELECT COUNT(*)
					FROM follower_edges
					WHERE followed_pubkey = $1
				), 0) AS follower_count,
				COALESCE((
					SELECT COUNT(*)
					FROM follower_edges
					WHERE follower_pubkey = $1
				), 0) AS following_count,
				COALESCE((
					SELECT COUNT(*)
					FROM events e
					WHERE e.pubkey = $1
					  AND e.kind = 1
					  AND NOT EXISTS (
					      SELECT 1
					      FROM event_references er
					      WHERE er.source_event_id = e.id
					        AND er.relation = 'reply'
					  )
				), 0) AS note_count,
				COALESCE((
					SELECT COUNT(*)
					FROM events e
					WHERE e.pubkey = $1
					  AND e.kind = 1
					  AND EXISTS (
					      SELECT 1
					      FROM event_references er
					      WHERE er.source_event_id = e.id
					        AND er.relation = 'reply'
					  )
				), 0) AS reply_count,
				(
					SELECT MAX(created_at)
					FROM events
					WHERE pubkey = $1
				) AS recent_activity_at
		`, pubkey).Scan(
			&followerCount,
			&followingCount,
			&noteCount,
			&replyCount,
			&recentActivityAt,
		); err != nil {
			return fmt.Errorf("compute profile public stats for %s: %w", pubkey, err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_public_stats (
				pubkey,
				follower_count,
				following_count,
				note_count,
				reply_count,
				recent_activity_at,
				derivation_version
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (pubkey) DO UPDATE
			SET follower_count = EXCLUDED.follower_count,
			    following_count = EXCLUDED.following_count,
			    note_count = EXCLUDED.note_count,
			    reply_count = EXCLUDED.reply_count,
			    recent_activity_at = EXCLUDED.recent_activity_at,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, pubkey, followerCount, followingCount, noteCount, replyCount, recentActivityAt, writeVersion); err != nil {
			return fmt.Errorf("upsert profile public stats for %s: %w", pubkey, err)
		}
	}
	return nil
}
