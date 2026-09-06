package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// defaultAuthorAnalyticsWindows controls which window_days values the live
// per-pubkey rebuild aggregates when a Handlers instance was not given an
// explicit list (via HandlersOptions.AuthorAnalyticsWindows), letting the live
// sweeper skip the most expensive windows when the operator prefers freshness
// over completeness.
//
// The default omits 90 because each window roughly doubles per-pubkey
// rebuild cost (each window runs 5 heavy CTE-based INSERTs scanning
// events/event_references/reaction_events/repost_events/zap_receipts
// joined with events for the cutoff lookup). Production observed
// per-pubkey rebuilds taking 30-160s with all three windows enabled,
// monopolizing pgxpool connections and starving bundle workers.
//
// Schema constraints permit window_days IN (7, 30, 90) — see migrations
// 000029 and 000030 — so dropping 90 from the live sweeper does not
// change schema. Existing window_days=90 rows remain queryable but stop
// being refreshed; operators who need 90d data refreshed in real time
// can set WORKER_AUTHOR_ANALYTICS_WINDOWS_DAYS=7,30,90.
var defaultAuthorAnalyticsWindows = []int{7, 30}

// normalizeAuthorAnalyticsWindows validates a requested window list against the
// schema CHECK (window_days IN (7, 30, 90)), dropping unknown/duplicate values.
// An empty or entirely-invalid list yields nil, signalling the caller to fall
// back to the package default.
func normalizeAuthorAnalyticsWindows(windows []int) []int {
	if len(windows) == 0 {
		return nil
	}
	allowed := map[int]struct{}{7: {}, 30: {}, 90: {}}
	seen := map[int]struct{}{}
	out := make([]int, 0, len(windows))
	for _, w := range windows {
		if _, ok := allowed[w]; !ok {
			continue
		}
		if _, dup := seen[w]; dup {
			continue
		}
		seen[w] = struct{}{}
		out = append(out, w)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// authorAnalyticsWindowList returns this handler's configured window list,
// falling back to the package default when none was provided.
func (h *Handlers) authorAnalyticsWindowList() []int {
	if len(h.authorAnalyticsWindows) > 0 {
		return h.authorAnalyticsWindows
	}
	return defaultAuthorAnalyticsWindows
}

// maxAuthorAnalyticsWindowDays returns the largest configured window_days
// value (default 30). Live sweeper incremental daily rebuilds use this as
// the retain/rebuild horizon so author_engagement_stats windows stay correct.
func maxAuthorAnalyticsWindowDays(windows []int) int {
	maxDays := 0
	for _, w := range windows {
		if w > maxDays {
			maxDays = w
		}
	}
	if maxDays <= 0 {
		return 30
	}
	return maxDays
}

func (h *Handlers) ProjectAuthorAnalytics(ctx context.Context, eventID string) error {
	return h.projectAuthorAnalyticsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectAuthorAnalyticsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var kind int
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &kind); err != nil {
		return fmt.Errorf("load event for author analytics projection: %w", err)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	references := deriveEventReferences(eventID, tags)
	pubkeys, err := h.authorAnalyticsAffectedPubkeys(ctx, eventID, kind, pubkey, references, tags)
	if err != nil {
		return err
	}
	for _, affectedPubkey := range pubkeys {
		if err := h.projectAuthorAnalyticsForPubkey(ctx, affectedPubkey, versionOverride); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handlers) authorAnalyticsAffectedPubkeys(
	ctx context.Context,
	sourceEventID string,
	kind int,
	sourcePubkey string,
	references []derivedReference,
	tags [][]string,
) ([]string, error) {
	pubkeys := []string{sourcePubkey}

	appendEventAuthor := func(eventID string) error {
		var targetPubkey string
		if err := h.pool.QueryRow(ctx, `
			SELECT pubkey
			FROM events
			WHERE id = $1
		`, eventID).Scan(&targetPubkey); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return fmt.Errorf("load target event pubkey: %w", err)
		}
		pubkeys = append(pubkeys, targetPubkey)
		return nil
	}

	switch kind {
	case 1:
		for _, targetID := range replyAffectTargets(references) {
			if err := appendEventAuthor(targetID); err != nil {
				return nil, err
			}
		}
		rows, err := h.pool.Query(ctx, `
			SELECT e.pubkey
			FROM reply_count_contributions c
			JOIN events e ON e.id = c.target_event_id
			WHERE c.source_event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing reply targets for author analytics: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetPubkey string
			if err := rows.Scan(&targetPubkey); err != nil {
				return nil, fmt.Errorf("scan existing reply target for author analytics: %w", err)
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing reply targets for author analytics: %w", err)
		}
	case 6:
		for _, ref := range references {
			if err := appendEventAuthor(ref.Referenced); err != nil {
				return nil, err
			}
		}
		rows, err := h.pool.Query(ctx, `
			SELECT e.pubkey
			FROM repost_events r
			JOIN events e ON e.id = r.target_event_id
			WHERE r.event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing repost targets for author analytics: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetPubkey string
			if err := rows.Scan(&targetPubkey); err != nil {
				return nil, fmt.Errorf("scan existing repost target for author analytics: %w", err)
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing repost targets for author analytics: %w", err)
		}
	case 7:
		for _, ref := range references {
			if err := appendEventAuthor(ref.Referenced); err != nil {
				return nil, err
			}
		}
		rows, err := h.pool.Query(ctx, `
			SELECT e.pubkey
			FROM reaction_events r
			JOIN events e ON e.id = r.target_event_id
			WHERE r.event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing reaction targets for author analytics: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetPubkey string
			if err := rows.Scan(&targetPubkey); err != nil {
				return nil, fmt.Errorf("scan existing reaction target for author analytics: %w", err)
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing reaction targets for author analytics: %w", err)
		}
	case 9735:
		pubkeys = append(pubkeys, firstTagValue(tags, "p"))
		var priorReceiverPubkey *string
		if err := h.pool.QueryRow(ctx, `
			SELECT receiver_pubkey
			FROM zap_receipts
			WHERE zap_receipt_id = $1
		`, sourceEventID).Scan(&priorReceiverPubkey); err == nil && priorReceiverPubkey != nil {
			pubkeys = append(pubkeys, *priorReceiverPubkey)
		}
	}

	return normalizeUniqueIDs(pubkeys), nil
}

func (h *Handlers) rebuildAuthorAnalyticsWithVersion(ctx context.Context, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	rows, err := h.pool.Query(ctx, `
		SELECT pubkey
		FROM (
			SELECT DISTINCT pubkey AS pubkey
			FROM events
			UNION
			SELECT DISTINCT receiver_pubkey AS pubkey
			FROM zap_receipts
			WHERE receiver_pubkey <> ''
			UNION
			SELECT DISTINCT sender_pubkey AS pubkey
			FROM zap_receipts
			WHERE sender_pubkey IS NOT NULL AND sender_pubkey <> ''
			UNION
			SELECT DISTINCT reactor_pubkey AS pubkey
			FROM reaction_events
			WHERE reactor_pubkey <> ''
			UNION
			SELECT DISTINCT reposter_pubkey AS pubkey
			FROM repost_events
			WHERE reposter_pubkey <> ''
		) authors
		ORDER BY pubkey ASC
	`)
	if err != nil {
		return fmt.Errorf("list authors for analytics rebuild: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return fmt.Errorf("scan author pubkey for analytics rebuild: %w", err)
		}
		if err := h.projectAuthorAnalyticsForPubkey(ctx, pubkey, versionOverride); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate authors for analytics rebuild: %w", err)
	}
	return nil
}

func (h *Handlers) projectAuthorAnalyticsForPubkey(ctx context.Context, pubkey string, versionOverride *int) error {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return fmt.Errorf("pubkey is required")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := h.projectAuthorAnalyticsForPubkeyTx(ctx, tx, pubkey, versionOverride, false); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit author analytics projection tx: %w", err)
	}
	return nil
}

func (h *Handlers) projectAuthorAnalyticsForPubkeyTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	versionOverride *int,
	incrementalDaily bool,
) error {
	if err := lockPubkeyForWriteTx(ctx, tx, pubkey, pubkeyLockNamespaceAuthorAnalytics); err != nil {
		return err
	}
	engagementVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorEngagementStats,
		AuthorEngagementStatsVersion,
		"Project windowed per-author engagement and cadence summaries",
		versionOverride,
	)
	if err != nil {
		return err
	}
	topicVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorTopicStats,
		AuthorTopicStatsVersion,
		"Project windowed per-author hashtag usage summaries",
		versionOverride,
	)
	if err != nil {
		return err
	}
	mediaVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorMediaMixStats,
		AuthorMediaMixStatsVersion,
		"Project windowed per-author media mix summaries",
		versionOverride,
	)
	if err != nil {
		return err
	}
	activityWindowVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorActivityWindows,
		AuthorActivityWindowsVersion,
		"Project windowed per-author engagement timing buckets by UTC day/hour",
		versionOverride,
	)
	if err != nil {
		return err
	}
	postingPatternVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorPostingPatterns,
		AuthorPostingPatternsVersion,
		"Project windowed per-author posting cadence buckets by UTC day/hour",
		versionOverride,
	)
	if err != nil {
		return err
	}

	// Live sweeper with incremental author_activity_daily enabled: skip the
	// expensive multi-CTE daily rebuild entirely. Deltas are applied in the
	// derive bundle; the sweeper only rolls windowed projections.
	// Explicit rebuild/backfill paths pass incrementalDaily=false and still
	// run a full DELETE+rebuild even when the incremental flag is on.
	if !(h.incrementalAuthorActivityDaily && incrementalDaily) {
		activityVersion, err := resolveDerivationWriteVersion(
			ctx,
			tx,
			DerivationAuthorActivityDaily,
			AuthorActivityDailyVersion,
			"Project per-author daily post cadence and engagement aggregates",
			versionOverride,
		)
		if err != nil {
			return err
		}
		lowerBoundUnix := int64(0)
		if incrementalDaily {
			days := maxAuthorAnalyticsWindowDays(h.authorAnalyticsWindowList())
			lowerBoundUnix = time.Now().UTC().AddDate(0, 0, -days).Unix()
		}
		if err := h.rebuildAuthorActivityDailyTx(ctx, tx, pubkey, activityVersion, lowerBoundUnix); err != nil {
			return err
		}
	}
	return h.rebuildAuthorWindowedStatsTx(
		ctx,
		tx,
		pubkey,
		engagementVersion,
		topicVersion,
		mediaVersion,
		activityWindowVersion,
		postingPatternVersion,
	)
}

func (h *Handlers) rebuildAuthorActivityDailyTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	version int,
	lowerBoundUnix int64,
) error {
	if lowerBoundUnix > 0 {
		if _, err := tx.Exec(ctx, `
			DELETE FROM author_activity_daily
			WHERE pubkey = $1
			  AND activity_date >= to_timestamp($2)::date
		`, pubkey, lowerBoundUnix); err != nil {
			return fmt.Errorf("delete recent author activity daily rows for %s: %w", pubkey, err)
		}
	} else {
		if _, err := tx.Exec(ctx, `DELETE FROM author_activity_daily WHERE pubkey = $1`, pubkey); err != nil {
			return fmt.Errorf("delete prior author activity daily rows for %s: %w", pubkey, err)
		}
	}

	_, err := tx.Exec(ctx, `
		WITH post_daily AS (
			SELECT
				to_timestamp(e.created_at)::date AS activity_date,
				COUNT(*) AS post_count,
				COUNT(*) FILTER (
					WHERE NOT EXISTS (
						SELECT 1
						FROM thread_edges te
						WHERE te.child_event_id = e.id
					)
				) AS note_count,
				COUNT(*) FILTER (
					WHERE EXISTS (
						SELECT 1
						FROM thread_edges te
						WHERE te.child_event_id = e.id
					)
				) AS reply_count
			FROM events e
			WHERE e.pubkey = $1
			  AND e.kind = 1
			  AND e.created_at >= $4
			  AND e.created_at <= $3
			GROUP BY to_timestamp(e.created_at)::date
		),
		received_sources AS (
			-- Engagement sources read the denormalized target/source pubkeys
			-- (migration 000082) instead of joining events per row: the join
			-- plans heap-scanned every event a prolific author wrote, and the
			-- denormalized columns match what the incremental deltas saw at
			-- projection time (see computeTrueAuthorActivityTotals).
			SELECT to_timestamp(rcc.source_created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM reply_count_contributions rcc
			WHERE rcc.target_pubkey = $1
			  AND rcc.source_pubkey <> $1
			  AND rcc.source_created_at >= $4
			  AND rcc.source_created_at <= $3
			GROUP BY to_timestamp(rcc.source_created_at)::date
			UNION ALL
			SELECT to_timestamp(re.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM reaction_events re
			WHERE re.target_pubkey = $1
			  AND re.reactor_pubkey <> $1
			  AND re.created_at >= $4
			  AND re.created_at <= $3
			GROUP BY to_timestamp(re.created_at)::date
			UNION ALL
			SELECT to_timestamp(re.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM repost_events re
			WHERE re.target_pubkey = $1
			  AND re.reposter_pubkey <> $1
			  AND re.created_at >= $4
			  AND re.created_at <= $3
			GROUP BY to_timestamp(re.created_at)::date
			UNION ALL
			SELECT to_timestamp(zr.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM zap_receipts zr
			WHERE zr.receiver_pubkey = $1
			  AND zr.sender_pubkey IS NOT NULL
			  AND zr.sender_pubkey <> $1
			  AND zr.created_at >= $4
			  AND zr.created_at <= $3
			GROUP BY to_timestamp(zr.created_at)::date
		),
		received_daily AS (
			SELECT activity_date, SUM(count_value)::bigint AS engagement_received
			FROM received_sources
			GROUP BY activity_date
		),
		given_sources AS (
			SELECT to_timestamp(rcc.source_created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM reply_count_contributions rcc
			WHERE rcc.source_pubkey = $1
			  AND rcc.target_pubkey IS NOT NULL
			  AND rcc.target_pubkey <> $1
			  AND rcc.source_created_at >= $4
			  AND rcc.source_created_at <= $3
			GROUP BY to_timestamp(rcc.source_created_at)::date
			UNION ALL
			SELECT to_timestamp(re.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM reaction_events re
			WHERE re.reactor_pubkey = $1
			  AND re.target_pubkey IS NOT NULL
			  AND re.target_pubkey <> $1
			  AND re.created_at >= $4
			  AND re.created_at <= $3
			GROUP BY to_timestamp(re.created_at)::date
			UNION ALL
			SELECT to_timestamp(re.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM repost_events re
			WHERE re.reposter_pubkey = $1
			  AND re.target_pubkey IS NOT NULL
			  AND re.target_pubkey <> $1
			  AND re.created_at >= $4
			  AND re.created_at <= $3
			GROUP BY to_timestamp(re.created_at)::date
			UNION ALL
			SELECT to_timestamp(zr.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM zap_receipts zr
			WHERE zr.sender_pubkey = $1
			  AND zr.receiver_pubkey <> $1
			  AND zr.created_at >= $4
			  AND zr.created_at <= $3
			GROUP BY to_timestamp(zr.created_at)::date
		),
		given_daily AS (
			SELECT activity_date, SUM(count_value)::bigint AS engagement_given
			FROM given_sources
			GROUP BY activity_date
		),
		all_days AS (
			SELECT activity_date FROM post_daily
			UNION
			SELECT activity_date FROM received_daily
			UNION
			SELECT activity_date FROM given_daily
		)
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
		SELECT
			$1,
			d.activity_date,
			COALESCE(p.post_count, 0),
			COALESCE(p.note_count, 0),
			COALESCE(p.reply_count, 0),
			COALESCE(r.engagement_received, 0),
			COALESCE(g.engagement_given, 0),
			$2
		FROM all_days d
		LEFT JOIN post_daily p ON p.activity_date = d.activity_date
		LEFT JOIN received_daily r ON r.activity_date = d.activity_date
		LEFT JOIN given_daily g ON g.activity_date = d.activity_date
		ON CONFLICT (pubkey, activity_date) DO UPDATE
		SET post_count = EXCLUDED.post_count,
		    note_count = EXCLUDED.note_count,
		    reply_count = EXCLUDED.reply_count,
		    engagement_received = EXCLUDED.engagement_received,
		    engagement_given = EXCLUDED.engagement_given,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, version, maxSaneUnixCreatedAt, lowerBoundUnix)
	if err != nil {
		return fmt.Errorf("rebuild author activity daily for %s: %w", pubkey, err)
	}
	return nil
}

func (h *Handlers) rebuildAuthorWindowedStatsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	engagementVersion int,
	topicVersion int,
	mediaVersion int,
	activityWindowVersion int,
	postingPatternVersion int,
) error {
	for _, windowDays := range h.authorAnalyticsWindowList() {
		cutoff := computeWindowCutoff(windowDays)
		if err := h.upsertAuthorEngagementWindowTx(ctx, tx, pubkey, windowDays, cutoff, engagementVersion); err != nil {
			return err
		}
		if err := h.upsertAuthorTopicWindowTx(ctx, tx, pubkey, windowDays, cutoff, topicVersion); err != nil {
			return err
		}
		if err := h.upsertAuthorMediaMixWindowTx(ctx, tx, pubkey, windowDays, cutoff, mediaVersion); err != nil {
			return err
		}
		if err := h.upsertAuthorActivityWindowsTx(ctx, tx, pubkey, windowDays, cutoff, activityWindowVersion); err != nil {
			return err
		}
		if err := h.upsertAuthorPostingPatternsTx(ctx, tx, pubkey, windowDays, cutoff, postingPatternVersion); err != nil {
			return err
		}
	}
	return nil
}
