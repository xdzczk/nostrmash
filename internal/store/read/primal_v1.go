package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// GetFeedEvents returns raw events for a Primal-style page.
// An empty authors slice reads across all authors. includeReplies false drops
// events that already have a thread-edge child row. hasImage keeps only notes
// flagged in note_discovery_stats.
func (s *Read) GetFeedEvents(
	ctx context.Context,
	authors []string,
	kinds []int,
	since int64,
	until int64,
	limit int,
	offset int,
	includeReplies bool,
	onlyReplies bool,
	hasImage bool,
) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(kinds) == 0 {
		kinds = []int{1}
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	if offset > 10000 {
		offset = 10000
	}
	if until <= 0 {
		until = 1 << 62
	}
	if authors == nil {
		authors = []string{}
	}
	var rows pgx.Rows
	var err error
	if len(authors) == 0 {
		rows, err = s.pool.Query(ctx, `
			SELECT e.raw_json
			FROM events e
			WHERE e.kind = ANY($1::int[])
			  AND e.created_at >= $2
			  AND e.created_at <= $3
			  AND (
			    ($4::bool AND EXISTS (SELECT 1 FROM thread_edges t WHERE t.child_event_id = e.id))
			    OR (NOT $4::bool AND $5::bool)
			    OR (
			      NOT $4::bool AND NOT $5::bool
			      AND NOT EXISTS (SELECT 1 FROM thread_edges t WHERE t.child_event_id = e.id)
			    )
			  )
			  AND (
			    NOT $6::bool
			    OR EXISTS (
			      SELECT 1 FROM note_discovery_stats n
			      WHERE n.event_id = e.id AND n.has_image
			    )
			  )
			ORDER BY e.created_at DESC, e.id DESC
			LIMIT $7 OFFSET $8
		`, kinds, since, until, onlyReplies, includeReplies, hasImage, limit, offset)
	} else {
		// One short index walk per author, then a merge. A single ANY() over a
		// large follow list plans a scan that times out the home feed.
		innerLimit := limit + offset
		if innerLimit < limit {
			innerLimit = limit
		}
		if innerLimit > 100 {
			innerLimit = 100
		}
		rows, err = s.pool.Query(ctx, `
			SELECT page.raw_json
			FROM unnest($1::text[]) AS author(pubkey)
			JOIN LATERAL (
				SELECT e.raw_json, e.created_at, e.id
				FROM events e
				WHERE e.pubkey = author.pubkey
				  AND e.kind = ANY($2::int[])
				  AND e.created_at >= $3
				  AND e.created_at <= $4
				  AND (
				    ($5::bool AND EXISTS (SELECT 1 FROM thread_edges t WHERE t.child_event_id = e.id))
				    OR (NOT $5::bool AND $6::bool)
				    OR (
				      NOT $5::bool AND NOT $6::bool
				      AND NOT EXISTS (SELECT 1 FROM thread_edges t WHERE t.child_event_id = e.id)
				    )
				  )
				  AND (
				    NOT $7::bool
				    OR EXISTS (
				      SELECT 1 FROM note_discovery_stats n
				      WHERE n.event_id = e.id AND n.has_image
				    )
				  )
				ORDER BY e.created_at DESC, e.id DESC
				LIMIT $10
			) page ON true
			ORDER BY page.created_at DESC, page.id DESC
			LIMIT $8 OFFSET $9
		`, authors, kinds, since, until, onlyReplies, includeReplies, hasImage, limit, offset, innerLimit)
	}
	if err != nil {
		return nil, fmt.Errorf("get feed events: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan feed event: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read feed events: %w", err)
	}
	return out, nil
}

// GetFollowerCountsByPubkeys returns follower totals for the given pubkeys.
func (s *Read) GetFollowerCountsByPubkeys(ctx context.Context, pubkeys []string) ([]readmodel.ProfileFollowerCount, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(pubkeys) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, follower_count
		FROM profile_public_stats
		WHERE pubkey = ANY($1::text[])
	`, pubkeys)
	if err != nil {
		return nil, fmt.Errorf("get follower counts: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.ProfileFollowerCount, 0, len(pubkeys))
	for rows.Next() {
		var row readmodel.ProfileFollowerCount
		if err := rows.Scan(&row.Pubkey, &row.FollowerCount); err != nil {
			return nil, fmt.Errorf("scan follower count: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// GetZapTotalsByReceiver sums zap receipts received by pubkey.
func (s *Read) GetZapTotalsByReceiver(ctx context.Context, pubkey string) (readmodel.ZapTotals, error) {
	if s == nil || s.pool == nil {
		return readmodel.ZapTotals{}, fmt.Errorf("store is not initialized")
	}
	var out readmodel.ZapTotals
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(SUM(amount_sats), 0)
		FROM zap_receipts
		WHERE receiver_pubkey = $1
	`, strings.TrimSpace(pubkey)).Scan(&out.Count, &out.Sats)
	if err != nil {
		return readmodel.ZapTotals{}, fmt.Errorf("get zap totals: %w", err)
	}
	return out, nil
}

// GetOldestEventCreatedAt returns the earliest stored created_at for pubkey.
func (s *Read) GetOldestEventCreatedAt(ctx context.Context, pubkey string) (int64, bool, error) {
	if s == nil || s.pool == nil {
		return 0, false, fmt.Errorf("store is not initialized")
	}
	var created int64
	err := s.pool.QueryRow(ctx, `
		SELECT created_at FROM events
		WHERE pubkey = $1
		ORDER BY created_at ASC, id ASC
		LIMIT 1
	`, strings.TrimSpace(pubkey)).Scan(&created)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("get oldest event: %w", err)
	}
	return created, true, nil
}

// CountEventsByKind counts stored events of one kind for pubkey.
func (s *Read) CountEventsByKind(ctx context.Context, pubkey string, kind int) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM events WHERE pubkey = $1 AND kind = $2
	`, strings.TrimSpace(pubkey), kind).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count events by kind: %w", err)
	}
	return n, nil
}

// GetEventRelayHints returns one relay URL per event id.
func (s *Read) GetEventRelayHints(ctx context.Context, ids []string) ([]readmodel.EventRelayHint, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (event_id) event_id, relay_url
		FROM event_relays
		WHERE event_id = ANY($1::text[])
		ORDER BY event_id, seen_at ASC
	`, ids)
	if err != nil {
		return nil, fmt.Errorf("get event relay hints: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.EventRelayHint, 0, len(ids))
	for rows.Next() {
		var row readmodel.EventRelayHint
		if err := rows.Scan(&row.EventID, &row.RelayURL); err != nil {
			return nil, fmt.Errorf("scan event relay hint: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// GetViewerEventActions reports the viewer's likes, replies, reposts, and zaps.
func (s *Read) GetViewerEventActions(ctx context.Context, viewer string, ids []string) ([]readmodel.ViewerEventAction, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	viewer = strings.TrimSpace(viewer)
	if viewer == "" || len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT target_event_id, BOOL_OR(liked), BOOL_OR(replied), BOOL_OR(reposted), BOOL_OR(zapped)
		FROM (
			SELECT target_event_id, true AS liked, false AS replied, false AS reposted, false AS zapped
			FROM reaction_events
			WHERE reactor_pubkey = $1 AND target_event_id = ANY($2::text[])
			UNION ALL
			SELECT target_event_id, false, true, false, false
			FROM reply_count_contributions
			WHERE source_pubkey = $1 AND target_event_id = ANY($2::text[])
			UNION ALL
			SELECT target_event_id, false, false, true, false
			FROM repost_events
			WHERE reposter_pubkey = $1 AND target_event_id = ANY($2::text[])
			UNION ALL
			SELECT event_id, false, false, false, true
			FROM zap_receipts
			WHERE sender_pubkey = $1 AND event_id = ANY($2::text[])
		) actions
		GROUP BY target_event_id
	`, viewer, ids)
	if err != nil {
		return nil, fmt.Errorf("get viewer event actions: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.ViewerEventAction, 0)
	for rows.Next() {
		var row readmodel.ViewerEventAction
		if err := rows.Scan(&row.EventID, &row.Liked, &row.Replied, &row.Reposted, &row.Zapped); err != nil {
			return nil, fmt.Errorf("scan viewer event action: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// GetTopZapsByEvents returns up to limitPerEvent zaps per target event, highest sats first.
func (s *Read) GetTopZapsByEvents(ctx context.Context, ids []string, limitPerEvent int) ([]readmodel.EventZap, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(ids) == 0 {
		return nil, nil
	}
	if limitPerEvent <= 0 {
		limitPerEvent = 5
	}
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, created_at, sender_pubkey, COALESCE(receiver_pubkey, ''), amount_sats, zap_receipt_id
		FROM (
			SELECT event_id, created_at, sender_pubkey, receiver_pubkey, amount_sats, zap_receipt_id,
			       row_number() OVER (PARTITION BY event_id ORDER BY amount_sats DESC, created_at DESC) AS rn
			FROM zap_receipts
			WHERE event_id = ANY($1::text[])
		) ranked
		WHERE rn <= $2
	`, ids, limitPerEvent)
	if err != nil {
		return nil, fmt.Errorf("get top zaps: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.EventZap, 0)
	for rows.Next() {
		var row readmodel.EventZap
		if err := rows.Scan(&row.EventID, &row.CreatedAt, &row.Sender, &row.Receiver, &row.AmountSats, &row.ZapReceiptID); err != nil {
			return nil, fmt.Errorf("scan top zap: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// ListDeletedEventIDs returns the subset of ids that have a deletion record.
func (s *Read) ListDeletedEventIDs(ctx context.Context, ids []string) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT target_event_id
		FROM deletion_events
		WHERE target_event_id = ANY($1::text[])
	`, ids)
	if err != nil {
		return nil, fmt.Errorf("list deleted event ids: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan deleted event id: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// GetAppSettings returns the stored settings JSON for pubkey.
func (s *Read) GetAppSettings(ctx context.Context, pubkey string) (json.RawMessage, string, error) {
	if s == nil || s.pool == nil {
		return nil, "", fmt.Errorf("store is not initialized")
	}
	var raw []byte
	var client string
	err := s.pool.QueryRow(ctx, `
		SELECT settings_json, client FROM primal_app_settings WHERE pubkey = $1
	`, strings.TrimSpace(pubkey)).Scan(&raw, &client)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, "", ErrNotFound
		}
		return nil, "", fmt.Errorf("get app settings: %w", err)
	}
	return json.RawMessage(raw), client, nil
}

// PutAppSettings upserts settings JSON for pubkey.
func (s *Read) PutAppSettings(ctx context.Context, pubkey string, client string, settings json.RawMessage) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	if len(settings) == 0 {
		settings = json.RawMessage(`{}`)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO primal_app_settings (pubkey, client, settings_json, updated_at)
		VALUES ($1, $2, $3::jsonb, now())
		ON CONFLICT (pubkey) DO UPDATE
		SET client = EXCLUDED.client,
		    settings_json = EXCLUDED.settings_json,
		    updated_at = now()
	`, strings.TrimSpace(pubkey), strings.TrimSpace(client), string(settings))
	if err != nil {
		return fmt.Errorf("put app settings: %w", err)
	}
	return nil
}

// GetAppSubsettings returns stored subsettings JSON.
func (s *Read) GetAppSubsettings(ctx context.Context, pubkey string, subkey string) (json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	var raw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT settings_json FROM primal_app_subsettings WHERE pubkey = $1 AND subkey = $2
	`, strings.TrimSpace(pubkey), strings.TrimSpace(subkey)).Scan(&raw)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get app subsettings: %w", err)
	}
	return json.RawMessage(raw), nil
}

// PutAppSubsettings upserts subsettings JSON.
func (s *Read) PutAppSubsettings(ctx context.Context, pubkey string, subkey string, settings json.RawMessage) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	if len(settings) == 0 {
		settings = json.RawMessage(`{}`)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO primal_app_subsettings (pubkey, subkey, settings_json, updated_at)
		VALUES ($1, $2, $3::jsonb, now())
		ON CONFLICT (pubkey, subkey) DO UPDATE
		SET settings_json = EXCLUDED.settings_json,
		    updated_at = now()
	`, strings.TrimSpace(pubkey), strings.TrimSpace(subkey), string(settings))
	if err != nil {
		return fmt.Errorf("put app subsettings: %w", err)
	}
	return nil
}

// ListPrimalNotifications returns a page of notifications for pubkey.
func (s *Read) ListPrimalNotifications(ctx context.Context, pubkey string, types []int, since, until int64, limit, offset int) ([]readmodel.PrimalNotification, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	if until <= 0 {
		until = 1 << 62
	}
	if types == nil {
		types = []int{}
	}
	pubkey = strings.TrimSpace(pubkey)
	// History lives on the engagement projections. primal_notifications only
	// supplies mentions, which those tables do not record. Each branch is
	// capped so a busy profile cannot sort its entire history.
	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, type, created_at, args
		FROM (
			(SELECT $1::text AS pubkey, 4::int AS type, created_at,
			        jsonb_build_object('your_post', target_event_id, 'who_liked_it', reactor_pubkey, 'reaction', content) AS args
			 FROM reaction_events
			 WHERE target_pubkey = $1
			   AND reactor_pubkey <> $1
			   AND created_at >= $2 AND created_at <= $3
			   AND (cardinality($4::int[]) = 0 OR 4 = ANY($4::int[]))
			 ORDER BY created_at DESC
			 LIMIT $7)
			UNION ALL
			(SELECT $1::text, 5, created_at,
			        jsonb_build_object('your_post', target_event_id, 'who_reposted_it', reposter_pubkey)
			 FROM repost_events
			 WHERE target_pubkey = $1
			   AND reposter_pubkey <> $1
			   AND created_at >= $2 AND created_at <= $3
			   AND (cardinality($4::int[]) = 0 OR 5 = ANY($4::int[]))
			 ORDER BY created_at DESC
			 LIMIT $7)
			UNION ALL
			(SELECT $1::text, 3, created_at,
			        jsonb_build_object('your_post', event_id, 'who_zapped_it', sender_pubkey, 'satszapped', amount_sats)
			 FROM zap_receipts
			 WHERE receiver_pubkey = $1
			   AND sender_pubkey <> $1
			   AND event_id IS NOT NULL
			   AND created_at >= $2 AND created_at <= $3
			   AND (cardinality($4::int[]) = 0 OR 3 = ANY($4::int[]))
			 ORDER BY created_at DESC
			 LIMIT $7)
			UNION ALL
			(SELECT $1::text, 1, gained_at,
			        jsonb_build_object('follower', follower_pubkey)
			 FROM follower_gain_events
			 WHERE followed_pubkey = $1
			   AND gained_at >= $2 AND gained_at <= $3
			   AND (cardinality($4::int[]) = 0 OR 1 = ANY($4::int[]))
			 ORDER BY gained_at DESC
			 LIMIT $7)
			UNION ALL
			(SELECT $1::text, 6, source_created_at,
			        jsonb_build_object('your_post', target_event_id, 'who_replied_to_it', source_pubkey, 'reply', source_event_id)
			 FROM reply_count_contributions
			 WHERE target_pubkey = $1
			   AND source_pubkey IS NOT NULL
			   AND source_pubkey <> $1
			   AND source_created_at IS NOT NULL
			   AND source_created_at >= $2 AND source_created_at <= $3
			   AND (cardinality($4::int[]) = 0 OR 6 = ANY($4::int[]) OR 601 = ANY($4::int[]))
			 ORDER BY source_created_at DESC
			 LIMIT $7)
			UNION ALL
			(SELECT pubkey, type::int, created_at, args
			 FROM primal_notifications
			 WHERE pubkey = $1
			   AND type IN (7, 8)
			   AND created_at >= $2 AND created_at <= $3
			   AND (cardinality($4::int[]) = 0 OR type = ANY($4::int[]))
			 ORDER BY created_at DESC
			 LIMIT $7)
		) page
		ORDER BY created_at DESC, type DESC
		LIMIT $5 OFFSET $6
	`, pubkey, since, until, types, limit, offset, limit+offset)
	if err != nil {
		return nil, fmt.Errorf("list primal notifications: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.PrimalNotification, 0, limit)
	for rows.Next() {
		var row readmodel.PrimalNotification
		var typ int
		var args []byte
		if err := rows.Scan(&row.Pubkey, &typ, &row.CreatedAt, &args); err != nil {
			return nil, fmt.Errorf("scan primal notification: %w", err)
		}
		row.Type = int(typ)
		row.Args = json.RawMessage(args)
		out = append(out, row)
	}
	return out, rows.Err()
}

// GetNotificationsSeen returns the seen-until timestamp.
func (s *Read) GetNotificationsSeen(ctx context.Context, pubkey string) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	var seen int64
	err := s.pool.QueryRow(ctx, `
		SELECT seen_until FROM primal_notifications_seen WHERE pubkey = $1
	`, strings.TrimSpace(pubkey)).Scan(&seen)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("get notifications seen: %w", err)
	}
	return seen, nil
}

// SetNotificationsSeen records seen-until and clears unread counts.
func (s *Read) SetNotificationsSeen(ctx context.Context, pubkey string, seenUntil int64) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin notifications seen: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO primal_notifications_seen (pubkey, seen_until)
		VALUES ($1, $2)
		ON CONFLICT (pubkey) DO UPDATE SET seen_until = GREATEST(primal_notifications_seen.seen_until, EXCLUDED.seen_until)
	`, pubkey, seenUntil); err != nil {
		return fmt.Errorf("upsert notifications seen: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM primal_notification_counts WHERE pubkey = $1`, pubkey); err != nil {
		return fmt.Errorf("reset notification counts: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit notifications seen: %w", err)
	}
	return nil
}

// ListNotificationCounts returns nonzero unread counters.
func (s *Read) ListNotificationCounts(ctx context.Context, pubkey string) ([]readmodel.NotificationCount, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT type, cnt FROM primal_notification_counts
		WHERE pubkey = $1 AND cnt > 0
		ORDER BY type ASC
	`, strings.TrimSpace(pubkey))
	if err != nil {
		return nil, fmt.Errorf("list notification counts: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.NotificationCount, 0)
	for rows.Next() {
		var row readmodel.NotificationCount
		var typ int16
		if err := rows.Scan(&typ, &row.Cnt); err != nil {
			return nil, fmt.Errorf("scan notification count: %w", err)
		}
		row.Type = int(typ)
		out = append(out, row)
	}
	return out, rows.Err()
}

// GetMediaMetadataByURLs loads stored media rows.
func (s *Read) GetMediaMetadataByURLs(ctx context.Context, urls []string) ([]readmodel.MediaMetadata, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(urls) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT url, mimetype, width, height, duration, blurhash
		FROM primal_media_metadata
		WHERE url = ANY($1::text[])
	`, urls)
	if err != nil {
		return nil, fmt.Errorf("get media metadata: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.MediaMetadata, 0, len(urls))
	for rows.Next() {
		var row readmodel.MediaMetadata
		if err := rows.Scan(&row.URL, &row.Mimetype, &row.Width, &row.Height, &row.Duration, &row.Blurhash); err != nil {
			return nil, fmt.Errorf("scan media metadata: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// PutMediaMetadata upserts one media row.
func (s *Read) PutMediaMetadata(ctx context.Context, row readmodel.MediaMetadata) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO primal_media_metadata (url, mimetype, width, height, duration, blurhash, fetched_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (url) DO UPDATE
		SET mimetype = EXCLUDED.mimetype,
		    width = EXCLUDED.width,
		    height = EXCLUDED.height,
		    duration = EXCLUDED.duration,
		    blurhash = EXCLUDED.blurhash,
		    fetched_at = now()
	`, row.URL, row.Mimetype, row.Width, row.Height, row.Duration, row.Blurhash)
	if err != nil {
		return fmt.Errorf("put media metadata: %w", err)
	}
	return nil
}

// GetLinkMetadataByURLs loads stored link previews.
func (s *Read) GetLinkMetadataByURLs(ctx context.Context, urls []string) ([]readmodel.LinkMetadata, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(urls) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT url, title, description, image, icon_url
		FROM primal_link_metadata
		WHERE url = ANY($1::text[])
	`, urls)
	if err != nil {
		return nil, fmt.Errorf("get link metadata: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.LinkMetadata, 0, len(urls))
	for rows.Next() {
		var row readmodel.LinkMetadata
		if err := rows.Scan(&row.URL, &row.Title, &row.Description, &row.Image, &row.IconURL); err != nil {
			return nil, fmt.Errorf("scan link metadata: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// PutLinkMetadata upserts one link preview.
func (s *Read) PutLinkMetadata(ctx context.Context, row readmodel.LinkMetadata) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO primal_link_metadata (url, title, description, image, icon_url, fetched_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (url) DO UPDATE
		SET title = EXCLUDED.title,
		    description = EXCLUDED.description,
		    image = EXCLUDED.image,
		    icon_url = EXCLUDED.icon_url,
		    fetched_at = now()
	`, row.URL, row.Title, row.Description, row.Image, row.IconURL)
	if err != nil {
		return fmt.Errorf("put link metadata: %w", err)
	}
	return nil
}

// GetPrimalTrendingNotes returns one Primal trending page.
// maxRank nil is the first page. A set maxRank keeps ranks at or below that
// cursor, and offset skips the boundary row the client already rendered.
// byZaps ranks by whole sats instead of the discovery score.
func (s *Read) GetPrimalTrendingNotes(ctx context.Context, scoreColumn string, minCreatedAt int64, limit, offset int, maxRank *int64, byZaps bool) ([]readmodel.TrendingNote, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	rankExpr := "s.score_24h"
	switch scoreColumn {
	case "score_7d":
		rankExpr = "s.score_7d"
	case "", "score_24h":
		rankExpr = "s.score_24h"
	default:
		return nil, fmt.Errorf("unsupported trending score column")
	}
	if byZaps {
		rankExpr = "(s.zap_msats / 1000.0)"
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	query := fmt.Sprintf(`
		SELECT event_id, author_pubkey, created_at, content, language,
		       reply_count, repost_count, reaction_count, zap_count, zap_msats, score
		FROM (
			SELECT
				s.event_id,
				s.author_pubkey,
				s.created_at,
				e.content,
				''::text AS language,
				s.reply_count,
				s.repost_count,
				s.reaction_count,
				s.zap_count,
				s.zap_msats,
				ROUND(%s)::float8 AS score
			FROM note_discovery_stats s
			JOIN events e ON e.id = s.event_id
			WHERE s.created_at >= $1
			  AND e.kind = 1
			  AND NOT EXISTS (
				SELECT 1 FROM thread_edges te WHERE te.child_event_id = s.event_id
			  )
			  AND (
				EXISTS (SELECT 1 FROM profiles_latest pl WHERE pl.pubkey = s.author_pubkey)
				OR EXISTS (
					SELECT 1 FROM events metadata
					WHERE metadata.pubkey = s.author_pubkey AND metadata.kind = 0
				)
			  )
		) ranked
		WHERE ($2::bigint IS NULL OR score::bigint <= $2)
		ORDER BY score DESC, created_at DESC, event_id ASC
		LIMIT $3 OFFSET $4
	`, rankExpr)
	rows, err := s.pool.Query(ctx, query, minCreatedAt, maxRank, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get primal trending notes: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.TrendingNote, 0, limit)
	for rows.Next() {
		var row readmodel.TrendingNote
		if err := rows.Scan(
			&row.EventID,
			&row.AuthorPubkey,
			&row.CreatedAt,
			&row.Content,
			&row.Language,
			&row.ReplyCount,
			&row.RepostCount,
			&row.ReactionCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.Score,
		); err != nil {
			return nil, fmt.Errorf("scan primal trending note: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read primal trending notes: %w", err)
	}
	return out, nil
}
