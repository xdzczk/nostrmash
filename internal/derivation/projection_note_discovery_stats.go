package derivation

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) ProjectNoteDiscoveryStats(ctx context.Context, eventID string) error {
	return h.projectNoteDiscoveryStatsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectNoteDiscoveryStatsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var kind int
	if err := h.pool.QueryRow(ctx, `
		SELECT kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind); err != nil {
		return fmt.Errorf("load event for note discovery projection: %w", err)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	references := deriveEventReferences(eventID, tags)

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationNoteDiscoveryStats,
		NoteDiscoveryStatsVersion,
		"Project per-note discovery counters and rolling scores",
		versionOverride,
	)
	if err != nil {
		return err
	}

	affectedNoteIDs, err := h.affectedNoteDiscoveryIDsTx(ctx, tx, eventID, kind, references, tags)
	if err != nil {
		return err
	}
	nowUnix := time.Now().UTC().Unix()
	for _, noteID := range affectedNoteIDs {
		if err := h.refreshNoteDiscoveryStatsTx(ctx, tx, noteID, writeVersion, nowUnix); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit note discovery projection tx: %w", err)
	}
	return nil
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

func (h *Handlers) refreshNoteDiscoveryStatsTx(
	ctx context.Context,
	tx pgx.Tx,
	noteID string,
	writeVersion int,
	nowUnix int64,
) error {
	var authorPubkey string
	var noteCreatedAt int64
	var noteKind int
	var noteContent string
	if err := tx.QueryRow(ctx, `
		SELECT pubkey, created_at, kind, COALESCE(content, '')
		FROM events
		WHERE id = $1
	`, noteID).Scan(&authorPubkey, &noteCreatedAt, &noteKind, &noteContent); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if _, delErr := tx.Exec(ctx, `DELETE FROM note_discovery_stats WHERE event_id = $1`, noteID); delErr != nil {
				return fmt.Errorf("delete stale note discovery row for missing event: %w", delErr)
			}
			return nil
		}
		return fmt.Errorf("load note event metadata: %w", err)
	}
	if !isNoteDiscoveryProjectableKind(noteKind) {
		if _, err := tx.Exec(ctx, `DELETE FROM note_discovery_stats WHERE event_id = $1`, noteID); err != nil {
			return fmt.Errorf("delete non-note discovery row: %w", err)
		}
		return nil
	}

	replyCount, err := queryInt64Tx(ctx, tx, `SELECT COALESCE((SELECT count FROM reply_counts WHERE event_id = $1), 0)`, noteID)
	if err != nil {
		return fmt.Errorf("load total reply_count: %w", err)
	}
	repostCount, err := queryInt64Tx(ctx, tx, `SELECT COALESCE((SELECT count FROM repost_counts WHERE event_id = $1), 0)`, noteID)
	if err != nil {
		return fmt.Errorf("load total repost_count: %w", err)
	}
	reactionCount, err := queryInt64Tx(ctx, tx, `SELECT COALESCE((SELECT count FROM reaction_counts WHERE event_id = $1), 0)`, noteID)
	if err != nil {
		return fmt.Errorf("load total reaction_count: %w", err)
	}
	zapCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM zap_receipts
		WHERE event_id = $1
	`, noteID)
	if err != nil {
		return fmt.Errorf("load total zap_count: %w", err)
	}
	zapMSats, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(SUM(amount_sats * 1000), 0)
		FROM zap_receipts
		WHERE event_id = $1
	`, noteID)
	if err != nil {
		return fmt.Errorf("load total zap_msats: %w", err)
	}

	reply24h, repost24h, reaction24h, zapCount24h, zapMSats24h, err := loadWindowedInteractionCounts(ctx, tx, noteID, nowUnix, 24*time.Hour)
	if err != nil {
		return err
	}
	reply7d, repost7d, reaction7d, zapCount7d, zapMSats7d, err := loadWindowedInteractionCounts(ctx, tx, noteID, nowUnix, 7*24*time.Hour)
	if err != nil {
		return err
	}
	hasImage, hasVideo, hasLink, hasArticle, attachmentCount, err := loadNoteMediaFlagsTx(ctx, tx, noteID)
	if err != nil {
		return err
	}
	score24h := computeTrendingScore(24*time.Hour, nowUnix, noteCreatedAt, reply24h, repost24h, reaction24h, zapCount24h, zapMSats24h)
	score7d := computeTrendingScore(7*24*time.Hour, nowUnix, noteCreatedAt, reply7d, repost7d, reaction7d, zapCount7d, zapMSats7d)
	primaryLanguage, languageConfidence := detectPrimaryLanguage(noteContent)

	if _, err := tx.Exec(ctx, `
		INSERT INTO note_discovery_stats (
			event_id,
			author_pubkey,
			created_at,
			reply_count,
			repost_count,
			reaction_count,
			zap_count,
			zap_msats,
			has_image,
			has_video,
			has_link,
			has_article,
			attachment_count,
			primary_language,
			language_confidence,
			score_24h,
			score_7d,
			last_scored_at,
			derivation_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now(), $18)
		ON CONFLICT (event_id) DO UPDATE
		SET author_pubkey = EXCLUDED.author_pubkey,
		    created_at = EXCLUDED.created_at,
		    reply_count = EXCLUDED.reply_count,
		    repost_count = EXCLUDED.repost_count,
		    reaction_count = EXCLUDED.reaction_count,
		    zap_count = EXCLUDED.zap_count,
		    zap_msats = EXCLUDED.zap_msats,
		    has_image = EXCLUDED.has_image,
		    has_video = EXCLUDED.has_video,
		    has_link = EXCLUDED.has_link,
		    has_article = EXCLUDED.has_article,
		    attachment_count = EXCLUDED.attachment_count,
		    primary_language = EXCLUDED.primary_language,
		    language_confidence = EXCLUDED.language_confidence,
		    score_24h = EXCLUDED.score_24h,
		    score_7d = EXCLUDED.score_7d,
		    last_scored_at = now(),
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`, noteID, authorPubkey, noteCreatedAt, replyCount, repostCount, reactionCount, zapCount, zapMSats, hasImage, hasVideo, hasLink, hasArticle, attachmentCount, primaryLanguage, languageConfidence, score24h, score7d, writeVersion); err != nil {
		return fmt.Errorf("upsert note discovery stats: %w", err)
	}
	return nil
}

func loadWindowedInteractionCounts(
	ctx context.Context,
	tx pgx.Tx,
	noteID string,
	nowUnix int64,
	window time.Duration,
) (int64, int64, int64, int64, int64, error) {
	cutoff := nowUnix - int64(window/time.Second)
	replyCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM reply_count_contributions c
		JOIN events e ON e.id = c.source_event_id
		WHERE c.target_event_id = $1
		  AND e.created_at >= $2
	`, noteID, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("load windowed reply_count: %w", err)
	}
	repostCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM repost_events
		WHERE target_event_id = $1
		  AND created_at >= $2
	`, noteID, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("load windowed repost_count: %w", err)
	}
	reactionCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM reaction_events
		WHERE target_event_id = $1
		  AND created_at >= $2
	`, noteID, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("load windowed reaction_count: %w", err)
	}
	zapCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM zap_receipts
		WHERE event_id = $1
		  AND created_at >= $2
	`, noteID, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("load windowed zap_count: %w", err)
	}
	zapMSats, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(SUM(amount_sats * 1000), 0)
		FROM zap_receipts
		WHERE event_id = $1
		  AND created_at >= $2
	`, noteID, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("load windowed zap_msats: %w", err)
	}
	return replyCount, repostCount, reactionCount, zapCount, zapMSats, nil
}

func queryInt64Tx(ctx context.Context, tx pgx.Tx, sql string, args ...any) (int64, error) {
	var value int64
	if err := tx.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func loadNoteMediaFlagsTx(ctx context.Context, tx pgx.Tx, noteID string) (bool, bool, bool, bool, int, error) {
	var hasImage bool
	var hasVideo bool
	var hasLink bool
	var hasArticle bool
	var attachmentCount int
	if err := tx.QueryRow(ctx, `
		WITH note AS (
			SELECT kind, LOWER(COALESCE(content, '')) AS content
			FROM events
			WHERE id = $1
		),
		content_links AS (
			SELECT DISTINCT m[1] AS url
			FROM note n,
			LATERAL regexp_matches(n.content, '(https?://[^[:space:]]+)', 'g') AS m
		),
		tag_rows AS (
			SELECT LOWER(COALESCE(tag_name, '')) AS tag_name, LOWER(COALESCE(value, '')) AS value
			FROM event_tags
			WHERE event_id = $1
		),
		signal AS (
			SELECT
				EXISTS (
					SELECT 1 FROM tag_rows t
					WHERE t.tag_name IN ('image', 'thumb')
				) OR EXISTS (
					SELECT 1 FROM content_links l
					WHERE l.url ~ '\.(png|jpe?g|gif|webp)([?#].*)?$'
				) AS has_image,
				EXISTS (
					SELECT 1 FROM tag_rows t
					WHERE t.tag_name = 'video'
				) OR EXISTS (
					SELECT 1 FROM content_links l
					WHERE l.url ~ '\.(mp4|mov|webm|m4v)([?#].*)?$'
				) AS has_video,
				EXISTS (
					SELECT 1 FROM tag_rows t
					WHERE t.tag_name = 'r' AND t.value <> ''
				) OR EXISTS (
					SELECT 1 FROM content_links
				) AS has_link,
				EXISTS (
					SELECT 1 FROM note n
					WHERE n.kind = 30023 OR char_length(n.content) >= 1200
				) AS has_article,
				(
					COALESCE((
						SELECT COUNT(*)
						FROM tag_rows t
						WHERE t.tag_name IN ('image', 'thumb', 'video', 'imeta')
					), 0) +
					COALESCE((
						SELECT COUNT(*)
						FROM content_links l
						WHERE l.url ~ '\.(png|jpe?g|gif|webp|mp4|mov|webm|m4v)([?#].*)?$'
					), 0)
				)::int AS attachment_count
		)
		SELECT has_image, has_video, has_link, has_article, attachment_count
		FROM signal
	`).Scan(&hasImage, &hasVideo, &hasLink, &hasArticle, &attachmentCount); err != nil {
		return false, false, false, false, 0, fmt.Errorf("load note media flags: %w", err)
	}
	return hasImage, hasVideo, hasLink, hasArticle, attachmentCount, nil
}

func computeTrendingScore(
	window time.Duration,
	nowUnix int64,
	noteCreatedAt int64,
	replyCount int64,
	repostCount int64,
	reactionCount int64,
	zapCount int64,
	zapMSats int64,
) float64 {
	windowSeconds := int64(window / time.Second)
	if windowSeconds <= 0 {
		return 0
	}
	ageSeconds := nowUnix - noteCreatedAt
	if ageSeconds < 0 {
		ageSeconds = 0
	}
	if ageSeconds > windowSeconds {
		return 0
	}
	base := float64(replyCount)*3.0 +
		float64(repostCount)*2.0 +
		float64(reactionCount)*1.0 +
		float64(zapCount)*2.0 +
		(float64(zapMSats) / 100000.0)
	decay := 1.0 / (1.0 + (float64(ageSeconds) / float64(windowSeconds)))
	score := base * decay
	if score <= 0 {
		return 0
	}
	return math.Round(score*1000.0) / 1000.0
}

func isNoteDiscoveryProjectableKind(kind int) bool {
	return kind == 1 || kind == 30023
}
