package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Ledger projection names for applied_stat_deltas. Each event may claim at
// most one row per projection, which gates the whole multi-pubkey delta set
// for that projection so retries are exactly-once.
const (
	statDeltaProfilePublicStats   = "profile_public_stats"
	statDeltaAuthorActivityDaily  = "author_activity_daily"
	statDeltaAuthorHashtagDaily   = "author_hashtag_daily"
	statDeltaAuthorMediaDaily     = "author_media_daily"
	statDeltaAuthorHourlyActivity = "author_hourly_activity"
	statDeltaFollowerCounts       = "profile_public_stats_followers"
)

// ApplyIncrementalAuthorStats applies O(1) counter deltas for the
// projections that used to be full-history recomputes. It is a no-op when
// both incremental profile-public-stats and author-activity-daily flags are
// disabled.
//
// Must run after the cheap projections that establish the facts this path
// reads (hashtags, note_discovery_stats media flags, reaction/repost/zap
// rows, contact lists). Idempotent via applied_stat_deltas.
func (h *Handlers) ApplyIncrementalAuthorStats(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	if !h.incrementalProfilePublicStats && !h.incrementalAuthorActivityDaily {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var kind int
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind, created_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &kind, &createdAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load event for incremental stats: %w", err)
	}

	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	references := deriveEventReferences(eventID, tags)
	isReply := false
	replyTargetEventID := ""
	for _, ref := range references {
		if ref.Relation == "reply" {
			isReply = true
			replyTargetEventID = strings.TrimSpace(ref.Referenced)
			break
		}
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin incremental stats tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if h.incrementalProfilePublicStats {
		if err := h.applyIncrementalProfilePublicStatsTx(ctx, tx, eventID, pubkey, kind, createdAt, isReply); err != nil {
			return err
		}
	}
	if h.incrementalAuthorActivityDaily {
		if err := h.applyIncrementalAuthorActivityDailyTx(ctx, tx, eventID, pubkey, kind, createdAt, isReply, replyTargetEventID, tags); err != nil {
			return err
		}
		if err := h.applyIncrementalAuthorHashtagDailyTx(ctx, tx, eventID, pubkey, kind, createdAt); err != nil {
			return err
		}
		if err := h.applyIncrementalAuthorMediaDailyTx(ctx, tx, eventID, pubkey, kind, createdAt); err != nil {
			return err
		}
		if err := h.applyIncrementalAuthorHourlyActivityTx(ctx, tx, eventID, pubkey, kind, createdAt, isReply, replyTargetEventID, tags); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit incremental stats tx: %w", err)
	}
	return nil
}

func claimStatDeltaTx(ctx context.Context, tx pgx.Tx, eventID, projection string) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO applied_stat_deltas (event_id, projection)
		VALUES ($1, $2)
		ON CONFLICT (event_id, projection) DO NOTHING
	`, eventID, projection)
	if err != nil {
		return false, fmt.Errorf("claim stat delta %s/%s: %w", eventID, projection, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (h *Handlers) applyIncrementalProfilePublicStatsTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
	isReply bool,
) error {
	if kind != 1 {
		return nil
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaProfilePublicStats)
	if err != nil || !claimed {
		return err
	}
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationProfilePublicStats,
		ProfilePublicStatsVersion,
		"Project public profile counters and recent activity",
		nil,
	)
	if err != nil {
		return err
	}
	noteDelta := int64(0)
	replyDelta := int64(0)
	if isReply {
		replyDelta = 1
	} else {
		noteDelta = 1
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
		VALUES ($1, 0, 0, $2, $3, $4, $5)
		ON CONFLICT (pubkey) DO UPDATE
		SET note_count = profile_public_stats.note_count + EXCLUDED.note_count,
		    reply_count = profile_public_stats.reply_count + EXCLUDED.reply_count,
		    recent_activity_at = GREATEST(
				COALESCE(profile_public_stats.recent_activity_at, 0),
				COALESCE(EXCLUDED.recent_activity_at, 0)
			),
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, noteDelta, replyDelta, createdAt, writeVersion); err != nil {
		return fmt.Errorf("apply profile public stats delta: %w", err)
	}
	return nil
}

func (h *Handlers) applyFollowerCountDeltasTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, authorPubkey string,
	previousFollowed, contacts []string,
	versionOverride *int,
) error {
	if !h.incrementalProfilePublicStats {
		return nil
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaFollowerCounts)
	if err != nil || !claimed {
		return err
	}
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

	prev := make(map[string]struct{}, len(previousFollowed))
	for _, p := range previousFollowed {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		prev[p] = struct{}{}
	}
	next := make(map[string]struct{}, len(contacts))
	for _, p := range contacts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		next[p] = struct{}{}
	}

	type delta struct {
		pubkey         string
		followerDelta  int64
		followingDelta int64
	}
	deltas := map[string]*delta{}
	bump := func(pubkey string, followerDelta, followingDelta int64) {
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			return
		}
		d, ok := deltas[pubkey]
		if !ok {
			d = &delta{pubkey: pubkey}
			deltas[pubkey] = d
		}
		d.followerDelta += followerDelta
		d.followingDelta += followingDelta
	}

	for followed := range next {
		if _, ok := prev[followed]; ok {
			continue
		}
		bump(authorPubkey, 0, 1)
		bump(followed, 1, 0)
	}
	for followed := range prev {
		if _, ok := next[followed]; ok {
			continue
		}
		bump(authorPubkey, 0, -1)
		bump(followed, -1, 0)
	}

	for _, d := range deltas {
		if d.followerDelta == 0 && d.followingDelta == 0 {
			continue
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
			VALUES ($1, GREATEST($2, 0), GREATEST($3, 0), 0, 0, NULL, $4)
			ON CONFLICT (pubkey) DO UPDATE
			SET follower_count = GREATEST(profile_public_stats.follower_count + $2, 0),
			    following_count = GREATEST(profile_public_stats.following_count + $3, 0),
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, d.pubkey, d.followerDelta, d.followingDelta, writeVersion); err != nil {
			return fmt.Errorf("apply follower count delta for %s: %w", d.pubkey, err)
		}
	}
	return nil
}

type activityDailyDelta struct {
	pubkey             string
	activityDate       time.Time
	postCount          int64
	noteCount          int64
	replyCount         int64
	engagementReceived int64
	engagementGiven    int64
}

func (h *Handlers) applyIncrementalAuthorActivityDailyTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
	isReply bool,
	replyTargetEventID string,
	tags [][]string,
) error {
	activityDate := time.Unix(createdAt, 0).UTC().Truncate(24 * time.Hour)
	deltas := make([]activityDailyDelta, 0, 2)

	switch kind {
	case 1:
		d := activityDailyDelta{
			pubkey:       pubkey,
			activityDate: activityDate,
			postCount:    1,
		}
		if isReply {
			d.replyCount = 1
			d.engagementGiven = 1
		} else {
			d.noteCount = 1
		}
		deltas = append(deltas, d)
		if isReply && replyTargetEventID != "" {
			targetPubkey, ok, err := lookupEventPubkeyTx(ctx, tx, replyTargetEventID)
			if err != nil {
				return err
			}
			if ok && targetPubkey != "" && targetPubkey != pubkey {
				deltas = append(deltas, activityDailyDelta{
					pubkey:             targetPubkey,
					activityDate:       activityDate,
					engagementReceived: 1,
				})
			} else if ok && targetPubkey == pubkey {
				// Self-reply: cancel the given increment to match full-rebuild
				// semantics (e.pubkey <> $1 / target.pubkey <> $1 filters).
				deltas[0].engagementGiven = 0
			}
		}
	case 6, 7:
		targetEventID := firstReferencedEventID(tags)
		if targetEventID == "" {
			return nil
		}
		targetPubkey, ok, err := lookupEventPubkeyTx(ctx, tx, targetEventID)
		if err != nil {
			return err
		}
		if !ok || targetPubkey == "" || targetPubkey == pubkey {
			return nil
		}
		deltas = append(deltas,
			activityDailyDelta{pubkey: pubkey, activityDate: activityDate, engagementGiven: 1},
			activityDailyDelta{pubkey: targetPubkey, activityDate: activityDate, engagementReceived: 1},
		)
	case 9735:
		receiver := firstTagValue(tags, "p")
		if receiver == "" || receiver == pubkey {
			return nil
		}
		deltas = append(deltas,
			activityDailyDelta{pubkey: pubkey, activityDate: activityDate, engagementGiven: 1},
			activityDailyDelta{pubkey: receiver, activityDate: activityDate, engagementReceived: 1},
		)
	default:
		return nil
	}
	if len(deltas) == 0 {
		return nil
	}

	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorActivityDaily)
	if err != nil || !claimed {
		return err
	}
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorActivityDaily,
		AuthorActivityDailyVersion,
		"Project per-author daily post cadence and engagement aggregates",
		nil,
	)
	if err != nil {
		return err
	}
	for _, d := range deltas {
		if err := upsertAuthorActivityDailyDeltaTx(ctx, tx, d, writeVersion); err != nil {
			return err
		}
	}
	return nil
}

func upsertAuthorActivityDailyDeltaTx(ctx context.Context, tx pgx.Tx, d activityDailyDelta, writeVersion int) error {
	if d.pubkey == "" {
		return nil
	}
	if d.postCount == 0 && d.noteCount == 0 && d.replyCount == 0 && d.engagementReceived == 0 && d.engagementGiven == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO author_activity_daily (
			pubkey,
			activity_date,
			post_count,
			note_count,
			reply_count,
			engagement_received,
			engagement_given,
			derivation_version
		)
		VALUES ($1, $2::date, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (pubkey, activity_date) DO UPDATE
		SET post_count = author_activity_daily.post_count + EXCLUDED.post_count,
		    note_count = author_activity_daily.note_count + EXCLUDED.note_count,
		    reply_count = author_activity_daily.reply_count + EXCLUDED.reply_count,
		    engagement_received = author_activity_daily.engagement_received + EXCLUDED.engagement_received,
		    engagement_given = author_activity_daily.engagement_given + EXCLUDED.engagement_given,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, d.pubkey, d.activityDate, d.postCount, d.noteCount, d.replyCount, d.engagementReceived, d.engagementGiven, writeVersion)
	if err != nil {
		return fmt.Errorf("upsert author_activity_daily delta for %s: %w", d.pubkey, err)
	}
	return nil
}

func (h *Handlers) applyIncrementalAuthorHashtagDailyTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
) error {
	if kind != 1 {
		return nil
	}
	rows, err := tx.Query(ctx, `
		SELECT hashtag
		FROM event_hashtags
		WHERE event_id = $1
	`, eventID)
	if err != nil {
		return fmt.Errorf("load event hashtags for incremental daily: %w", err)
	}
	hashtags := make([]string, 0)
	for rows.Next() {
		var hashtag string
		if err := rows.Scan(&hashtag); err != nil {
			rows.Close()
			return fmt.Errorf("scan hashtag for incremental daily: %w", err)
		}
		hashtags = append(hashtags, hashtag)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(hashtags) == 0 {
		return nil
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorHashtagDaily)
	if err != nil || !claimed {
		return err
	}
	activityDate := time.Unix(createdAt, 0).UTC().Truncate(24 * time.Hour)
	for _, hashtag := range hashtags {
		if _, err := tx.Exec(ctx, `
			INSERT INTO author_hashtag_daily (
				pubkey, activity_date, hashtag, usage_count, derivation_version
			)
			VALUES ($1, $2::date, $3, 1, $4)
			ON CONFLICT (pubkey, activity_date, hashtag) DO UPDATE
			SET usage_count = author_hashtag_daily.usage_count + 1,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, pubkey, activityDate, hashtag, AuthorTopicStatsVersion); err != nil {
			return fmt.Errorf("upsert author_hashtag_daily: %w", err)
		}
	}
	return nil
}

func (h *Handlers) applyIncrementalAuthorMediaDailyTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
) error {
	if kind != 1 {
		return nil
	}
	var hasImage, hasVideo, hasLink, hasArticle bool
	var attachmentCount int
	err := tx.QueryRow(ctx, `
		SELECT has_image, has_video, has_link, has_article, attachment_count
		FROM note_discovery_stats
		WHERE event_id = $1
	`, eventID).Scan(&hasImage, &hasVideo, &hasLink, &hasArticle, &attachmentCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load note_discovery_stats for media daily: %w", err)
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorMediaDaily)
	if err != nil || !claimed {
		return err
	}
	textOnly := !hasImage && !hasVideo && !hasLink && !hasArticle
	activityDate := time.Unix(createdAt, 0).UTC().Truncate(24 * time.Hour)
	_, err = tx.Exec(ctx, `
		INSERT INTO author_media_daily (
			pubkey,
			activity_date,
			total_posts,
			with_image_count,
			with_video_count,
			with_link_count,
			with_article_count,
			text_only_count,
			total_attachment_count,
			derivation_version
		)
		VALUES (
			$1, $2::date, 1,
			CASE WHEN $3 THEN 1 ELSE 0 END,
			CASE WHEN $4 THEN 1 ELSE 0 END,
			CASE WHEN $5 THEN 1 ELSE 0 END,
			CASE WHEN $6 THEN 1 ELSE 0 END,
			CASE WHEN $7 THEN 1 ELSE 0 END,
			$8,
			$9
		)
		ON CONFLICT (pubkey, activity_date) DO UPDATE
		SET total_posts = author_media_daily.total_posts + 1,
		    with_image_count = author_media_daily.with_image_count + EXCLUDED.with_image_count,
		    with_video_count = author_media_daily.with_video_count + EXCLUDED.with_video_count,
		    with_link_count = author_media_daily.with_link_count + EXCLUDED.with_link_count,
		    with_article_count = author_media_daily.with_article_count + EXCLUDED.with_article_count,
		    text_only_count = author_media_daily.text_only_count + EXCLUDED.text_only_count,
		    total_attachment_count = author_media_daily.total_attachment_count + EXCLUDED.total_attachment_count,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, activityDate, hasImage, hasVideo, hasLink, hasArticle, textOnly, attachmentCount, AuthorMediaMixStatsVersion)
	if err != nil {
		return fmt.Errorf("upsert author_media_daily: %w", err)
	}
	return nil
}

func (h *Handlers) applyIncrementalAuthorHourlyActivityTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
	isReply bool,
	replyTargetEventID string,
	tags [][]string,
) error {
	engagedAt := time.Unix(createdAt, 0).UTC()
	activityDate := engagedAt.Truncate(24 * time.Hour)
	dow := int16(engagedAt.Weekday()) // Sunday=0 matches EXTRACT(DOW ...)
	hour := int16(engagedAt.Hour())

	type hourlyDelta struct {
		pubkey             string
		postCount          int64
		noteCount          int64
		replyCount         int64
		engagementReceived int64
		replyReceived      int64
		reactionReceived   int64
		repostReceived     int64
		zapReceived        int64
	}
	deltas := make([]hourlyDelta, 0, 2)

	switch kind {
	case 1:
		d := hourlyDelta{pubkey: pubkey, postCount: 1}
		if isReply {
			d.replyCount = 1
		} else {
			d.noteCount = 1
		}
		deltas = append(deltas, d)
		if isReply && replyTargetEventID != "" {
			targetPubkey, ok, err := lookupEventPubkeyTx(ctx, tx, replyTargetEventID)
			if err != nil {
				return err
			}
			if ok && targetPubkey != "" && targetPubkey != pubkey {
				deltas = append(deltas, hourlyDelta{
					pubkey:             targetPubkey,
					engagementReceived: 1,
					replyReceived:      1,
				})
			}
		}
	case 6, 7:
		targetEventID := firstReferencedEventID(tags)
		if targetEventID == "" {
			return nil
		}
		targetPubkey, ok, err := lookupEventPubkeyTx(ctx, tx, targetEventID)
		if err != nil {
			return err
		}
		if !ok || targetPubkey == "" || targetPubkey == pubkey {
			return nil
		}
		d := hourlyDelta{pubkey: targetPubkey, engagementReceived: 1}
		if kind == 7 {
			d.reactionReceived = 1
		} else {
			d.repostReceived = 1
		}
		deltas = append(deltas, d)
	case 9735:
		receiver := firstTagValue(tags, "p")
		if receiver == "" || receiver == pubkey {
			return nil
		}
		deltas = append(deltas, hourlyDelta{
			pubkey:             receiver,
			engagementReceived: 1,
			zapReceived:        1,
		})
	default:
		return nil
	}
	if len(deltas) == 0 {
		return nil
	}

	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorHourlyActivity)
	if err != nil || !claimed {
		return err
	}
	for _, d := range deltas {
		if _, err := tx.Exec(ctx, `
			INSERT INTO author_hourly_activity (
				pubkey,
				activity_date,
				day_of_week,
				hour_of_day,
				post_count,
				note_count,
				reply_count,
				engagement_received,
				reply_received,
				reaction_received,
				repost_received,
				zap_received,
				derivation_version
			)
			VALUES ($1, $2::date, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (pubkey, activity_date, day_of_week, hour_of_day) DO UPDATE
			SET post_count = author_hourly_activity.post_count + EXCLUDED.post_count,
			    note_count = author_hourly_activity.note_count + EXCLUDED.note_count,
			    reply_count = author_hourly_activity.reply_count + EXCLUDED.reply_count,
			    engagement_received = author_hourly_activity.engagement_received + EXCLUDED.engagement_received,
			    reply_received = author_hourly_activity.reply_received + EXCLUDED.reply_received,
			    reaction_received = author_hourly_activity.reaction_received + EXCLUDED.reaction_received,
			    repost_received = author_hourly_activity.repost_received + EXCLUDED.repost_received,
			    zap_received = author_hourly_activity.zap_received + EXCLUDED.zap_received,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, d.pubkey, activityDate, dow, hour,
			d.postCount, d.noteCount, d.replyCount,
			d.engagementReceived, d.replyReceived, d.reactionReceived, d.repostReceived, d.zapReceived,
			AuthorActivityWindowsVersion); err != nil {
			return fmt.Errorf("upsert author_hourly_activity for %s: %w", d.pubkey, err)
		}
	}
	return nil
}

func lookupEventPubkeyTx(ctx context.Context, tx pgx.Tx, eventID string) (string, bool, error) {
	var pubkey string
	err := tx.QueryRow(ctx, `SELECT pubkey FROM events WHERE id = $1`, eventID).Scan(&pubkey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("lookup event pubkey %s: %w", eventID, err)
	}
	return pubkey, true, nil
}

func firstReferencedEventID(tags [][]string) string {
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != "e" {
			continue
		}
		value := strings.TrimSpace(tag[1])
		if value != "" {
			return value
		}
	}
	return ""
}
