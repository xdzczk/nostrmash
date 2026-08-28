package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) ProjectNoteDiscoveryStats(ctx context.Context, eventID string) error {
	return h.projectNoteDiscoveryStatsWithVersion(ctx, eventID, nil)
}

// rebuildNoteDiscoveryStatsWithVersion reprojects note-like events only so a
// full rebuild can recreate truncated rows without scanning every event kind.
func (h *Handlers) rebuildNoteDiscoveryStatsWithVersion(ctx context.Context, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventIDs, err := h.queryEventIDsByKinds(ctx, 1, 30023)
	if err != nil {
		return err
	}
	return h.rebuildProjectEventIDs(
		ctx,
		DerivationNoteDiscoveryStats,
		eventIDs,
		h.projectNoteDiscoveryStatsWithVersion,
		versionOverride,
	)
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

	// Prefer thread-wide descendant totals for discovery reply_count so Discover
	// matches other clients' conversation sizes. Fall back to direct-parent
	// reply_counts when no thread summary exists (e.g. non-root notes).
	replyCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(
			(SELECT reply_count FROM thread_summaries WHERE root_event_id = $1),
			(SELECT count FROM reply_counts WHERE event_id = $1),
			0
		)
	`, noteID)
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

	weighting := h.engagementWeighting
	engagement24h, err := loadWindowedEngagementWeights(ctx, tx, noteID, authorPubkey, nowUnix, 24*time.Hour, weighting)
	if err != nil {
		return err
	}
	engagement7d, err := loadWindowedEngagementWeights(ctx, tx, noteID, authorPubkey, nowUnix, 7*24*time.Hour, weighting)
	if err != nil {
		return err
	}
	// Thread roots score by conversation size: replace the direct-reply weight
	// with unique thread-wide repliers (same dedup / self-exclusion / trust
	// weighting) so Discover matches other clients' conversation sizes without
	// inheriting the raw thread_summaries counters a single account can inflate.
	if isRoot, threadErr := hasThreadSummary(ctx, tx, noteID); threadErr != nil {
		return threadErr
	} else if isRoot {
		if engagement24h.ReplyWeight, err = loadWindowedThreadReplyWeight(ctx, tx, noteID, authorPubkey, nowUnix, 24*time.Hour, weighting); err != nil {
			return err
		}
		if engagement7d.ReplyWeight, err = loadWindowedThreadReplyWeight(ctx, tx, noteID, authorPubkey, nowUnix, 7*24*time.Hour, weighting); err != nil {
			return err
		}
	}
	hasImage, hasVideo, hasLink, hasArticle, attachmentCount, err := loadNoteMediaFlagsTx(ctx, tx, noteID)
	if err != nil {
		return err
	}
	score24h := computeTrendingScore(24*time.Hour, nowUnix, noteCreatedAt,
		engagement24h.ReplyWeight, engagement24h.RepostWeight, engagement24h.ReactionWeight, engagement24h.ZapWeight, engagement24h.ZapMSats)
	score7d := computeTrendingScore(7*24*time.Hour, nowUnix, noteCreatedAt,
		engagement7d.ReplyWeight, engagement7d.RepostWeight, engagement7d.ReactionWeight, engagement7d.ZapWeight, engagement7d.ZapMSats)
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
	`, noteID).Scan(&hasImage, &hasVideo, &hasLink, &hasArticle, &attachmentCount); err != nil {
		return false, false, false, false, 0, fmt.Errorf("load note media flags: %w", err)
	}
	return hasImage, hasVideo, hasLink, hasArticle, attachmentCount, nil
}

func isNoteDiscoveryProjectableKind(kind int) bool {
	return kind == 1 || kind == 30023
}
