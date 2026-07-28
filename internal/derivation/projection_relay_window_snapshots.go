package derivation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RefreshRelayWindowSnapshots recomputes every homepage-bundle
// snapshot stored in relay_window_snapshots:
//
//   - summary             — relay totals and 24h/7d activity
//   - top_relays_7d       — top 10 relays by 7-day activity
//   - home_window_24h     — note volume + active authors (24h)
//   - home_window_7d      — note volume + active authors (7d)
//   - top_languages_24h   — top 8 languages (24h)
//   - top_languages_7d    — top 8 languages (7d)
//   - top_hashtags_24h    — top 50 hashtags (24h)
//   - top_hashtags_7d     — top 50 hashtags (7d)
//
// Why this is a projection, not an inline query
// ---------------------------------------------
// Every one of the underlying queries is a COUNT(DISTINCT) over
// hundreds of thousands to millions of rows. On production each
// individual aggregate takes 1-9s of CPU; running them inline on
// every homepage cache miss was making /api/v1/discovery/home time
// out at 30s and starving every other endpoint of DB connections.
//
// There is no SQL trick or index that fixes COUNT(DISTINCT) at
// these cardinalities — the cost is fundamental to the input
// cardinality. The only fix is to compute these out-of-band on a
// fixed cadence and serve the homepage from sub-millisecond row
// lookups.
//
// Concurrency safety
// ------------------
// Two replicas calling this concurrently is safe — the upserts are
// idempotent and the reads are non-locking. We do not bother with
// an advisory lock to dedupe; doing the work twice every 5 minutes
// is cheaper than the lock-contention machinery.
//
// Failure handling
// ----------------
// On any error the entire transaction rolls back, leaving the
// previous snapshot rows in place. Callers (the worker loop) log
// the error but do not block — the homepage keeps serving the last
// good snapshot until the next refresh succeeds.
//
// We deliberately do not partition the refresh into per-label
// transactions: keeping all snapshots under one tx means a partial
// failure never publishes an inconsistent mix of "fresh relay
// summary, stale active authors". Either everything advances or
// nothing does.
func (h *Handlers) RefreshRelayWindowSnapshots(ctx context.Context) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		// Default work_mem (4MB) forces the COUNT(DISTINCT) hashtables
		// to spill ~100-200MB to disk for the 7d windows. 128MB keeps
		// everything in memory; SET LOCAL releases it on commit so the
		// pool connection is unaffected.
		if _, err := tx.Exec(ctx, `SET LOCAL work_mem = '128MB'`); err != nil {
			return fmt.Errorf("set work_mem: %w", err)
		}

		summary, err := computeRelaySummarySnapshot(ctx, tx)
		if err != nil {
			return fmt.Errorf("compute relay summary snapshot: %w", err)
		}
		if err := upsertSnapshotPayload(ctx, tx, relaySnapshotLabelSummary, summary); err != nil {
			return err
		}

		topRelays, err := computeTopRelaysSnapshot(ctx, tx, 10)
		if err != nil {
			return fmt.Errorf("compute top relays snapshot: %w", err)
		}
		if err := upsertSnapshotPayload(ctx, tx, relaySnapshotLabelTopRelays7d, jsonArray(topRelays)); err != nil {
			return err
		}

		now := time.Now().UTC()
		cutoff24h := now.Add(-24 * time.Hour).Unix()
		cutoff7d := now.Add(-7 * 24 * time.Hour).Unix()

		homeWindow24h, err := computeHomeWindowSnapshot(ctx, tx, cutoff24h)
		if err != nil {
			return fmt.Errorf("compute home window 24h snapshot: %w", err)
		}
		if err := upsertSnapshotPayload(ctx, tx, relaySnapshotLabelHomeWindow24h, homeWindow24h); err != nil {
			return err
		}
		if err := insertStatsSnapshotHistory(ctx, tx, now, homeWindow24h, summary); err != nil {
			return err
		}
		homeWindow7d, err := computeHomeWindowSnapshot(ctx, tx, cutoff7d)
		if err != nil {
			return fmt.Errorf("compute home window 7d snapshot: %w", err)
		}
		if err := upsertSnapshotPayload(ctx, tx, relaySnapshotLabelHomeWindow7d, homeWindow7d); err != nil {
			return err
		}

		topLanguages24h, err := computeTopLanguagesSnapshot(ctx, tx, cutoff24h, 8)
		if err != nil {
			return fmt.Errorf("compute top languages 24h snapshot: %w", err)
		}
		if err := upsertSnapshotPayload(ctx, tx, relaySnapshotLabelTopLanguages24h, jsonArray(topLanguages24h)); err != nil {
			return err
		}
		topLanguages7d, err := computeTopLanguagesSnapshot(ctx, tx, cutoff7d, 8)
		if err != nil {
			return fmt.Errorf("compute top languages 7d snapshot: %w", err)
		}
		if err := upsertSnapshotPayload(ctx, tx, relaySnapshotLabelTopLanguages7d, jsonArray(topLanguages7d)); err != nil {
			return err
		}

		// Hashtags are snapshotted at the API max (50) so the store
		// layer can serve any caller-requested limit ≤50 from one row.
		topHashtags24h, err := computeTopHashtagsSnapshot(ctx, tx, cutoff24h, 50)
		if err != nil {
			return fmt.Errorf("compute top hashtags 24h snapshot: %w", err)
		}
		if err := upsertSnapshotPayload(ctx, tx, relaySnapshotLabelTopHashtags24h, jsonArray(topHashtags24h)); err != nil {
			return err
		}
		topHashtags7d, err := computeTopHashtagsSnapshot(ctx, tx, cutoff7d, 50)
		if err != nil {
			return fmt.Errorf("compute top hashtags 7d snapshot: %w", err)
		}
		return upsertSnapshotPayload(ctx, tx, relaySnapshotLabelTopHashtags7d, jsonArray(topHashtags7d))
	})
}

// Snapshot label constants — also referenced by the API store
// when reading. Keep these in sync with the seeds in migrations
// 000047_relay_window_snapshots.sql and
// 000048_homepage_window_snapshots.sql.
const (
	relaySnapshotLabelSummary         = "summary"
	relaySnapshotLabelTopRelays7d     = "top_relays_7d"
	relaySnapshotLabelHomeWindow24h   = "home_window_24h"
	relaySnapshotLabelHomeWindow7d    = "home_window_7d"
	relaySnapshotLabelTopLanguages24h = "top_languages_24h"
	relaySnapshotLabelTopLanguages7d  = "top_languages_7d"
	relaySnapshotLabelTopHashtags24h  = "top_hashtags_24h"
	relaySnapshotLabelTopHashtags7d   = "top_hashtags_7d"
)

// jsonArray normalizes a nil slice to an empty slice so json.Marshal
// always emits "[]" instead of "null". The store-side reader expects
// arrays and a "null" payload would force every reader to handle an
// extra branch.
func jsonArray[T any](in []T) []T {
	if in == nil {
		return []T{}
	}
	return in
}

func upsertSnapshotPayload(ctx context.Context, tx pgx.Tx, label string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal snapshot payload %q: %w", label, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
		VALUES ($1, $2::jsonb, now())
		ON CONFLICT (snapshot_label) DO UPDATE
		SET payload     = EXCLUDED.payload,
		    computed_at = EXCLUDED.computed_at
	`, label, encoded); err != nil {
		return fmt.Errorf("upsert snapshot payload %q: %w", label, err)
	}
	return nil
}

// insertStatsSnapshotHistory records at most one rolling-24-hour metric point
// per UTC hour. The worker refreshes every five minutes and may run on more
// than one replica, so the bucket primary key makes this append-only write
// bounded and idempotent.
func insertStatsSnapshotHistory(
	ctx context.Context,
	tx pgx.Tx,
	computedAt time.Time,
	home homeWindowSnapshotPayload,
	relay relaySummarySnapshotPayload,
) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO stats_snapshot_history (
			bucket_start, computed_at, note_volume, active_authors, relay_events
		)
		VALUES (date_trunc('hour', $1::timestamptz), $1, $2, $3, $4)
		ON CONFLICT (bucket_start) DO NOTHING
	`, computedAt, home.NoteVolume, home.ActiveAuthors, relay.Events24h); err != nil {
		return fmt.Errorf("insert stats snapshot history: %w", err)
	}
	return nil
}

// relaySummarySnapshotPayload mirrors the JSONB shape written into
// relay_window_snapshots.payload for the "summary" row. Kept private
// to this package because it is purely a wire format between the
// projection (writer) and the store layer (reader); the API uses
// store.RelaySummaryStats for the public response shape.
type relaySummarySnapshotPayload struct {
	Total      int64 `json:"total"`
	Active24h  int64 `json:"active_24h"`
	Active7d   int64 `json:"active_7d"`
	Events24h  int64 `json:"events_24h"`
	Events7d   int64 `json:"events_7d"`
	Authors24h int64 `json:"authors_24h"`
	Authors7d  int64 `json:"authors_7d"`
}

type relayActivityRow struct {
	RelayURL      string `json:"relay_url"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

func computeRelaySummarySnapshot(ctx context.Context, tx pgx.Tx) (relaySummarySnapshotPayload, error) {
	var out relaySummarySnapshotPayload

	// Loose index scan for the all-time distinct relay count: there
	// are only ~20 distinct relay_urls in the entire 4.4M-row table,
	// but a plain COUNT(DISTINCT) on the index is forced to scan it
	// end-to-end (~1.6s). The recursive CTE walks the index by
	// repeatedly seeking to the next strictly-larger value, doing
	// one btree descent per distinct value.
	if err := tx.QueryRow(ctx, `
		WITH RECURSIVE distinct_relays AS (
			(SELECT relay_url FROM event_relays ORDER BY relay_url ASC LIMIT 1)
			UNION ALL
			SELECT (
				SELECT relay_url
				FROM event_relays
				WHERE relay_url > prev.relay_url
				ORDER BY relay_url ASC
				LIMIT 1
			)
			FROM distinct_relays prev
			WHERE prev.relay_url IS NOT NULL
		)
		SELECT COALESCE(COUNT(*), 0)::bigint
		FROM distinct_relays
		WHERE relay_url IS NOT NULL
	`).Scan(&out.Total); err != nil {
		return out, fmt.Errorf("query relay total: %w", err)
	}

	now := time.Now().UTC()
	cutoff24h := now.Add(-24 * time.Hour)
	cutoff7d := now.Add(-7 * 24 * time.Hour)

	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(COUNT(DISTINCT relay_url), 0)::bigint AS active,
			COALESCE(COUNT(*), 0)::bigint                  AS events,
			COALESCE(COUNT(DISTINCT pubkey), 0)::bigint    AS authors
		FROM event_relays
		WHERE seen_at >= $1
	`, cutoff24h).Scan(&out.Active24h, &out.Events24h, &out.Authors24h); err != nil {
		return out, fmt.Errorf("query 24h window: %w", err)
	}

	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(COUNT(DISTINCT relay_url), 0)::bigint AS active,
			COALESCE(COUNT(*), 0)::bigint                  AS events,
			COALESCE(COUNT(DISTINCT pubkey), 0)::bigint    AS authors
		FROM event_relays
		WHERE seen_at >= $1
	`, cutoff7d).Scan(&out.Active7d, &out.Events7d, &out.Authors7d); err != nil {
		return out, fmt.Errorf("query 7d window: %w", err)
	}
	return out, nil
}

// homeWindowSnapshotPayload mirrors the JSONB shape stored under
// snapshot_label = 'home_window_24h' / 'home_window_7d'. note_volume
// is COUNT(*) and active_authors is COUNT(DISTINCT author_pubkey)
// over note_discovery_stats for the matching window.
type homeWindowSnapshotPayload struct {
	NoteVolume    int64 `json:"note_volume"`
	ActiveAuthors int64 `json:"active_authors"`
}

type languageRow struct {
	Language string `json:"language"`
	Count    int64  `json:"count"`
}

type hashtagRow struct {
	Hashtag       string `json:"hashtag"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

func computeHomeWindowSnapshot(ctx context.Context, tx pgx.Tx, minCreatedAt int64) (homeWindowSnapshotPayload, error) {
	var out homeWindowSnapshotPayload
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(COUNT(*), 0)::bigint                    AS note_volume,
			COALESCE(COUNT(DISTINCT author_pubkey), 0)::bigint AS active_authors
		FROM note_discovery_stats
		WHERE created_at >= $1
	`, minCreatedAt).Scan(&out.NoteVolume, &out.ActiveAuthors); err != nil {
		return out, fmt.Errorf("query home window stats: %w", err)
	}
	return out, nil
}

func computeTopLanguagesSnapshot(ctx context.Context, tx pgx.Tx, minCreatedAt int64, limit int) ([]languageRow, error) {
	if limit <= 0 {
		limit = 8
	}
	rows, err := tx.Query(ctx, `
		SELECT
			COALESCE(primary_language, 'und') AS language,
			COUNT(*)::bigint                  AS count_value
		FROM note_discovery_stats
		WHERE created_at >= $1
		GROUP BY COALESCE(primary_language, 'und')
		ORDER BY count_value DESC, language ASC
		LIMIT $2
	`, minCreatedAt, limit)
	if err != nil {
		return nil, fmt.Errorf("query top languages: %w", err)
	}
	defer rows.Close()
	out := make([]languageRow, 0, limit)
	for rows.Next() {
		var row languageRow
		if err := rows.Scan(&row.Language, &row.Count); err != nil {
			return nil, fmt.Errorf("scan top language row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read top language rows: %w", err)
	}
	return out, nil
}

// computeTopHashtagsSnapshot mirrors the ranking used by
// store.GetTrendingHashtags: order by unique_authors DESC, then by
// diversity (unique_authors / event_count) DESC, then by event_count
// DESC, then by hashtag ASC. The store layer will slice this list
// down to the per-request limit, so we always materialize up to the
// API max.
func computeTopHashtagsSnapshot(ctx context.Context, tx pgx.Tx, minCreatedAt int64, limit int) ([]hashtagRow, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := tx.Query(ctx, `
		SELECT
			hashtag,
			COUNT(*)::bigint                       AS event_count,
			COUNT(DISTINCT author_pubkey)::bigint  AS unique_authors
		FROM event_hashtags
		WHERE created_at >= $1
		GROUP BY hashtag
		ORDER BY
			unique_authors DESC,
			(COUNT(DISTINCT author_pubkey))::double precision / GREATEST(COUNT(*), 1) DESC,
			event_count DESC,
			hashtag ASC
		LIMIT $2
	`, minCreatedAt, limit)
	if err != nil {
		return nil, fmt.Errorf("query top hashtags: %w", err)
	}
	defer rows.Close()
	out := make([]hashtagRow, 0, limit)
	for rows.Next() {
		var row hashtagRow
		if err := rows.Scan(&row.Hashtag, &row.EventCount, &row.UniqueAuthors); err != nil {
			return nil, fmt.Errorf("scan top hashtag row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read top hashtag rows: %w", err)
	}
	return out, nil
}

func computeTopRelaysSnapshot(ctx context.Context, tx pgx.Tx, limit int) ([]relayActivityRow, error) {
	if limit <= 0 {
		limit = 10
	}
	cutoff7d := time.Now().UTC().Add(-7 * 24 * time.Hour)
	rows, err := tx.Query(ctx, `
		SELECT
			er.relay_url,
			COUNT(*)::bigint                  AS event_count,
			COUNT(DISTINCT er.pubkey)::bigint AS unique_authors
		FROM event_relays er
		WHERE er.seen_at >= $1
		GROUP BY er.relay_url
		ORDER BY event_count DESC, unique_authors DESC, er.relay_url ASC
		LIMIT $2
	`, cutoff7d, limit)
	if err != nil {
		return nil, fmt.Errorf("query top relays: %w", err)
	}
	defer rows.Close()
	out := make([]relayActivityRow, 0, limit)
	for rows.Next() {
		var row relayActivityRow
		if err := rows.Scan(&row.RelayURL, &row.EventCount, &row.UniqueAuthors); err != nil {
			return nil, fmt.Errorf("scan top relay row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read top relay rows: %w", err)
	}
	return out, nil
}
