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
		counts, err := scanProfilePublicStats(tx.QueryRow(ctx, profilePublicStatsComputeSQL, pubkey))
		if err != nil {
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
				long_form_note_count,
				media_count,
				zap_received_count,
				sats_zapped,
				content_zap_count,
				time_joined,
				derivation_version
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (pubkey) DO UPDATE
			SET follower_count = EXCLUDED.follower_count,
			    following_count = EXCLUDED.following_count,
			    note_count = EXCLUDED.note_count,
			    reply_count = EXCLUDED.reply_count,
			    recent_activity_at = EXCLUDED.recent_activity_at,
			    long_form_note_count = EXCLUDED.long_form_note_count,
			    media_count = EXCLUDED.media_count,
			    zap_received_count = EXCLUDED.zap_received_count,
			    sats_zapped = EXCLUDED.sats_zapped,
			    content_zap_count = EXCLUDED.content_zap_count,
			    time_joined = EXCLUDED.time_joined,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, pubkey, counts.FollowerCount, counts.FollowingCount, counts.NoteCount, counts.ReplyCount, counts.RecentActivityAt, counts.LongFormNoteCount, counts.MediaCount, counts.ZapReceivedCount, counts.SatsZapped, counts.ContentZapCount, counts.TimeJoined, writeVersion); err != nil {
			return fmt.Errorf("upsert profile public stats for %s: %w", pubkey, err)
		}
	}
	return nil
}

// profilePublicStatsCounts is the absolute profile header computed from
// stored events. The incremental deltas and this query must agree;
// reconciliation compares them field by field.
type profilePublicStatsCounts struct {
	FollowerCount     int64
	FollowingCount    int64
	NoteCount         int64
	ReplyCount        int64
	RecentActivityAt  *int64
	LongFormNoteCount int64
	MediaCount        int64
	ZapReceivedCount  int64
	SatsZapped        int64
	ContentZapCount   int64
	TimeJoined        *int64
}

// profilePublicStatsComputeSQL is the full recompute of one pubkey.
// media_count matches migration 000102: kind 20, or kind 1 with an image,
// video, or imeta tag, or a media URL in the content.
const profilePublicStatsComputeSQL = `
SELECT
	COALESCE((
		SELECT COUNT(*)
		FROM follower_edges
		WHERE followed_pubkey = $1
	), 0),
	COALESCE((
		SELECT COUNT(*)
		FROM follower_edges
		WHERE follower_pubkey = $1
	), 0),
	COALESCE((
		SELECT COUNT(*)
		FROM events e
		WHERE e.pubkey = $1
		  AND e.kind = 1
		  AND NOT EXISTS (
		      SELECT 1
		      FROM thread_edges te
		      WHERE te.child_event_id = e.id
		  )
	), 0),
	COALESCE((
		SELECT COUNT(*)
		FROM events e
		WHERE e.pubkey = $1
		  AND e.kind = 1
		  AND EXISTS (
		      SELECT 1
		      FROM thread_edges te
		      WHERE te.child_event_id = e.id
		  )
	), 0),
	(
		SELECT MAX(created_at)
		FROM events
		WHERE pubkey = $1
	),
	COALESCE((
		SELECT COUNT(*)
		FROM replaceable_state
		WHERE pubkey = $1 AND kind = 30023
	), 0),
	COALESCE((
		SELECT COUNT(*)
		FROM events e
		WHERE e.pubkey = $1
		  AND (
		      e.kind = 20
		      OR (
		          e.kind = 1
		          AND (
		              EXISTS (
		                  SELECT 1
		                  FROM event_tags t
		                  WHERE t.event_id = e.id
		                    AND LOWER(t.tag_name) IN ('image', 'thumb', 'video', 'imeta')
		              )
		              OR EXISTS (
		                  SELECT 1
		                  FROM regexp_matches(LOWER(COALESCE(e.content, '')), '(https?://[^[:space:]]+)', 'g') AS m(url)
		                  WHERE url[1] ~ '\.(png|jpe?g|gif|webp|mp4|mov|webm|m4v)([?#].*)?$'
		              )
		          )
		      )
		  )
	), 0),
	COALESCE((
		SELECT COUNT(*)
		FROM zap_receipts
		WHERE receiver_pubkey = $1
	), 0),
	COALESCE((
		SELECT SUM(amount_sats)
		FROM zap_receipts
		WHERE receiver_pubkey = $1
	), 0),
	COALESCE((
		SELECT COUNT(*)
		FROM zap_receipts
		WHERE sender_pubkey = $1
	), 0),
	(
		SELECT MIN(created_at)
		FROM events
		WHERE pubkey = $1
	)
`

func scanProfilePublicStats(row pgx.Row) (profilePublicStatsCounts, error) {
	var out profilePublicStatsCounts
	err := row.Scan(
		&out.FollowerCount,
		&out.FollowingCount,
		&out.NoteCount,
		&out.ReplyCount,
		&out.RecentActivityAt,
		&out.LongFormNoteCount,
		&out.MediaCount,
		&out.ZapReceivedCount,
		&out.SatsZapped,
		&out.ContentZapCount,
		&out.TimeJoined,
	)
	return out, err
}
