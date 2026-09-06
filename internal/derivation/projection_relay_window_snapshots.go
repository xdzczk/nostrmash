package derivation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/model"
)

// relayWindowSnapshotStatementTimeout is the per-statement Postgres budget for
// homepage snapshot aggregates. Kept under the worker loop's 120s refresh
// timeout so a stuck statement is cancelled by Postgres before the Go context
// expires and leaves an abandoned backend.
//
// These refreshes used to inherit the pool's ~15-30s statement_timeout. Under
// concurrent retention IO (buffer-cache eviction on event_relays), the 7d
// COUNT(DISTINCT) regularly crossed that ceiling even though the worker loop
// still had ~90s of budget left — causing NostrMashRelayWindowSnapshotStale
// whenever a catchup ran hot. SET LOCAL inside each refresh transaction
// scopes the raise to this work only.
const relayWindowSnapshotStatementTimeout = 100 * time.Second

// configureRelayWindowSnapshotTx applies per-transaction settings shared by
// both refresh phases: enough work_mem for the COUNT(DISTINCT) hashtables,
// and a statement_timeout that matches the intended refresh budget.
func configureRelayWindowSnapshotTx(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SET LOCAL work_mem = '128MB'`); err != nil {
		return fmt.Errorf("set work_mem: %w", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`SET LOCAL statement_timeout = %d`,
		relayWindowSnapshotStatementTimeout.Milliseconds(),
	)); err != nil {
		return fmt.Errorf("set statement_timeout: %w", err)
	}
	return nil
}

// domainMediaURLFilterClause excludes obvious inline media/attachment links
// from domain aggregation. Kept in sync with the identical constant in
// internal/store/read/parity_domains.go (the live GetTrendingDomains query)
// so the snapshot ranks the same candidate set.
const domainMediaURLFilterClause = `NOT (url ~* '\.(png|jpe?g|gif|webp|svg|bmp|ico|tiff?|avif|heic|mp4|mov|webm|m4v|avi|mkv|wmv|flv|mp3|wav|ogg|m4a|flac|aac|opus)(\?|#|$)')`

// trustedAuthorJoinClause restricts the trending hashtags and domains
// snapshots to links/tags authored by pubkeys inside the Web of Trust
// (trust_graph_snapshot: seeded npubs plus everyone reachable within the
// trust worker's configured hop bound). This is a deliberate product
// choice, not just an optimization: the homepage's "trending" surfaces
// should reflect what the trusted network is sharing, not whatever an
// unmoderated firehose of every pubkey on connected relays happens to
// post. It also shrinks the candidate set the COUNT(DISTINCT) aggregates
// below have to chew through, which helps with the timeout described in
// RefreshRelayWindowSnapshots's doc comment.
const trustedAuthorJoinClause = `INNER JOIN trust_graph_snapshot trusted ON trusted.pubkey = %s.author_pubkey`

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
//   - top_domains_24h     — top 50 domains (24h)
//   - top_domains_7d      — top 50 domains (7d)
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
// This is split into two independently-committed phases, each its
// own transaction:
//
//  1. refreshCoreRelayWindowSnapshots — relay summary, top relays,
//     home window (note volume / active authors), top languages.
//     These are cheap, reliable aggregates and drive the "Updated Xd
//     ago" freshness label everyone sees on the homepage, so they
//     must keep advancing even if phase 2 below is having a bad day.
//  2. refreshTrendingLinksSnapshots — top hashtags and top domains,
//     scoped to the Web of Trust (see trustedAuthorJoinClause). These
//     used to live in the same transaction as phase 1. On 2026-08-01
//     the 7d domains aggregate started reliably exceeding the (then)
//     60s worker timeout, and because everything shared one
//     transaction, that single query's timeout rolled back home
//     window / language / relay-summary snapshots too — the homepage
//     served 3-day-old numbers even though only the domains query was
//     actually broken. Splitting these into their own transaction
//     means a slow or failing hashtags/domains computation only
//     staleness-affects those two snapshot labels, not the whole
//     bundle.
//
// Within each phase, a failure still rolls back that phase's own
// upserts, leaving its previous snapshot rows in place — callers
// (the worker loop) log the error but do not block. RefreshRelayWindowSnapshots
// runs both phases regardless of whether the first one failed, and
// joins their errors so callers see everything that went wrong in a
// single tick.
func (h *Handlers) RefreshRelayWindowSnapshots(ctx context.Context) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	// The rollup feeds computeTopRelaysSnapshot. A rollup failure is not
	// fatal to the snapshots: the top-relays query unions the rollup with
	// the raw rows newer than the watermark, so a lagging watermark only
	// means more raw rows in that union (degraded, not wrong).
	rollupErr := h.refreshRelayActivityRollup(ctx)
	coreErr := h.refreshCoreRelayWindowSnapshots(ctx)
	trendingErr := h.refreshTrendingLinksSnapshots(ctx)
	return errors.Join(rollupErr, coreErr, trendingErr)
}

// Relay-activity rollup cadence knobs.
const (
	// relayActivityRollupLag keeps the watermark safely behind now() so
	// in-flight ingest transactions (whose seen_at defaults were assigned
	// at INSERT time, before commit) cannot commit behind an
	// already-advanced watermark and be missed forever. Ingest
	// transactions are sub-second; two minutes is comfortable margin.
	relayActivityRollupLag = 2 * time.Minute
	// relayActivityRollupChunk bounds how much raw event_relays range a
	// single rollup transaction aggregates, so each statement stays well
	// inside the statement budget even at flood-level ingest volume.
	relayActivityRollupChunk = 6 * time.Hour
	// relayActivityRollupRetention keeps one day of slack past the 7d
	// ranking window so bucket pruning never races the snapshot reads.
	relayActivityRollupRetention = 8 * 24 * time.Hour
)

// refreshRelayActivityRollup advances the relay_activity_hourly rollup to
// now()-lag, one committed chunk transaction at a time, then prunes buckets
// older than the retention horizon.
//
// Why incremental instead of recomputing per refresh: the top-relays
// aggregation used to GROUP BY over every raw event_relays row in the 7d
// window on each 5-minute refresh, so its cost scaled with total ingest
// volume — at ~4.8M rows/day (the relay-cap incident) it exceeded the 100s
// statement budget every cycle and the homepage snapshot froze for days.
// The rollup does that aggregation once per row: each refresh only scans
// rows with seen_at in (rolled_up_until, now()-lag], which is one refresh
// interval's worth of ingest in steady state.
//
// Why one transaction per chunk: the initial catchup (the migration seeds
// the watermark a full window back) and post-outage catchups can span many
// chunks. Committing each chunk separately means a context timeout mid
// catchup keeps the progress made so far; the next refresh resumes from the
// committed watermark instead of replaying from scratch.
func (h *Handlers) refreshRelayActivityRollup(ctx context.Context) error {
	for {
		caughtUp, err := h.rollupOneRelayActivityChunk(ctx)
		if err != nil {
			return fmt.Errorf("roll up relay activity chunk: %w", err)
		}
		if caughtUp {
			break
		}
	}
	if _, err := h.pool.Exec(ctx, `
		DELETE FROM relay_activity_hourly
		WHERE bucket_start < now() - $1::interval
	`, relayActivityRollupRetention.String()); err != nil {
		return fmt.Errorf("prune relay activity rollup: %w", err)
	}
	return nil
}

// rollupOneRelayActivityChunk aggregates one bounded slice of event_relays
// into the hourly rollup and advances the watermark, all in one committed
// transaction. Returns caughtUp=true when the watermark has reached
// now()-lag (including when this chunk was the one that got it there).
// The FOR UPDATE on the single watermark row serializes concurrent worker
// replicas so a slice is never rolled up twice.
func (h *Handlers) rollupOneRelayActivityChunk(ctx context.Context) (bool, error) {
	caughtUp := false
	err := pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		if err := configureRelayWindowSnapshotTx(ctx, tx); err != nil {
			return err
		}
		var watermark time.Time
		if err := tx.QueryRow(ctx, `
			SELECT rolled_up_until FROM relay_activity_rollup_state WHERE id FOR UPDATE
		`).Scan(&watermark); err != nil {
			return fmt.Errorf("lock rollup watermark: %w", err)
		}
		safeEnd := time.Now().UTC().Add(-relayActivityRollupLag)
		if !watermark.Before(safeEnd) {
			caughtUp = true
			return nil
		}
		chunkEnd := watermark.Add(relayActivityRollupChunk)
		if !chunkEnd.Before(safeEnd) {
			chunkEnd = safeEnd
			caughtUp = true
		}
		// The additive ON CONFLICT is what makes the (watermark, chunkEnd]
		// slices composable: a bucket hour usually spans multiple slices
		// (every 5-minute refresh touches the current hour), so each slice
		// adds its own row counts to whatever previous slices contributed.
		if _, err := tx.Exec(ctx, `
			INSERT INTO relay_activity_hourly (bucket_start, relay_url, event_count)
			SELECT date_trunc('hour', seen_at), relay_url, COUNT(*)
			FROM event_relays
			WHERE seen_at > $1 AND seen_at <= $2
			GROUP BY 1, 2
			ON CONFLICT (bucket_start, relay_url)
			DO UPDATE SET event_count = relay_activity_hourly.event_count + EXCLUDED.event_count
		`, watermark, chunkEnd); err != nil {
			return fmt.Errorf("aggregate slice into rollup: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE relay_activity_rollup_state SET rolled_up_until = $1 WHERE id
		`, chunkEnd); err != nil {
			return fmt.Errorf("advance rollup watermark: %w", err)
		}
		return nil
	})
	return caughtUp, err
}

// refreshCoreRelayWindowSnapshots recomputes the relay summary, top
// relays, home window (note volume / active authors), and top
// languages snapshots in a single transaction. See
// RefreshRelayWindowSnapshots for why this is split from the
// hashtags/domains phase.
func (h *Handlers) refreshCoreRelayWindowSnapshots(ctx context.Context) error {
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		// Default work_mem (4MB) forces the COUNT(DISTINCT) hashtables
		// to spill ~100-200MB to disk for the 7d windows. 128MB keeps
		// everything in memory; SET LOCAL releases it on commit so the
		// pool connection is unaffected. statement_timeout is raised in
		// the same helper — see configureRelayWindowSnapshotTx.
		if err := configureRelayWindowSnapshotTx(ctx, tx); err != nil {
			return err
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
		return upsertSnapshotPayload(ctx, tx, relaySnapshotLabelTopLanguages7d, jsonArray(topLanguages7d))
	})
}

// refreshTrendingLinksSnapshots recomputes the top hashtags and top
// domains snapshots in their own transaction, isolated from
// refreshCoreRelayWindowSnapshots (see RefreshRelayWindowSnapshots).
// Both aggregates are scoped to authors inside the Web of Trust
// (trust_graph_snapshot) — see trustedAuthorJoinClause — so the
// homepage's "trending" hashtags/domains reflect the trusted network
// rather than every pubkey ingest has ever seen a link or tag from.
func (h *Handlers) refreshTrendingLinksSnapshots(ctx context.Context) error {
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		if err := configureRelayWindowSnapshotTx(ctx, tx); err != nil {
			return err
		}

		now := time.Now().UTC()
		cutoff24h := now.Add(-24 * time.Hour).Unix()
		cutoff7d := now.Add(-7 * 24 * time.Hour).Unix()

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
		if err := upsertSnapshotPayload(ctx, tx, relaySnapshotLabelTopHashtags7d, jsonArray(topHashtags7d)); err != nil {
			return err
		}

		// Domains follow the same fixed-window snapshot shape as hashtags:
		// the homepage only ever requests the 24h or 7d window, and each
		// snapshot is ranked and filtered exactly like the live
		// GetTrendingDomains query for that window (see
		// internal/store/read/parity_domains.go), just precomputed and
		// WoT-scoped. Top 50 (the API max) so the store layer can serve
		// any caller-requested limit ≤50 from one row.
		topDomains24h, err := computeTopDomainsSnapshot(ctx, tx, cutoff24h, cutoff24h, cutoff7d, 50)
		if err != nil {
			return fmt.Errorf("compute top domains 24h snapshot: %w", err)
		}
		if err := upsertSnapshotPayload(ctx, tx, relaySnapshotLabelTopDomains24h, jsonArray(topDomains24h)); err != nil {
			return err
		}
		topDomains7d, err := computeTopDomainsSnapshot(ctx, tx, cutoff7d, cutoff24h, cutoff7d, 50)
		if err != nil {
			return fmt.Errorf("compute top domains 7d snapshot: %w", err)
		}
		return upsertSnapshotPayload(ctx, tx, relaySnapshotLabelTopDomains7d, jsonArray(topDomains7d))
	})
}

// RelayWindowSnapshotAge reports how long ago the "home_window_24h" row was
// last successfully computed — the same row backing the note-volume /
// active-authors numbers and the "Updated Xd ago" freshness label callers
// see on / and /api/v1/discovery/home. Unlike tracking a timestamp in
// process memory, this reads the actual database row, so it reports the
// true staleness of what users see even across worker restarts (e.g. an
// immediate refresh-on-start that itself fails would otherwise look
// "fresh" to an in-memory tracker). ok is false only when the row does not
// exist yet (a brand-new environment before the first successful refresh),
// so callers don't have to treat "never computed" as an alarming age.
//
// This exists specifically so RunRelayWindowSnapshotsLoop can feed
// metrics.SetRelayWindowSnapshotAge, which backs the
// NostrMashRelayWindowSnapshotStale alert (see
// observability/alerts/core_workflow_alerts.yml). Before this metric
// existed, a stuck/failing refresh loop had no signal beyond an old
// computed_at value nobody was watching — see the incident where the
// homepage silently served 3-day-old numbers.
func (h *Handlers) RelayWindowSnapshotAge(ctx context.Context) (time.Duration, bool, error) {
	if h == nil || h.pool == nil {
		return 0, false, fmt.Errorf("handlers are not initialized")
	}
	var computedAt time.Time
	err := h.pool.QueryRow(ctx, `
		SELECT computed_at FROM relay_window_snapshots WHERE snapshot_label = $1
	`, relaySnapshotLabelHomeWindow24h).Scan(&computedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read relay window snapshot age: %w", err)
	}
	return time.Since(computedAt), true, nil
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
	relaySnapshotLabelTopDomains24h   = "top_domains_24h"
	relaySnapshotLabelTopDomains7d    = "top_domains_7d"
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
	// AuthorsComputedAt (unix seconds) records when the COUNT(DISTINCT
	// pubkey) author aggregates were last recomputed from raw event_relays.
	// Internal bookkeeping for the throttled refresh below; the store-layer
	// reader ignores unknown fields.
	AuthorsComputedAt int64 `json:"authors_computed_at,omitempty"`
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
		  AND relay_url <> $1
	`, model.FallbackRelayURL).Scan(&out.Total); err != nil {
		return out, fmt.Errorf("query relay total: %w", err)
	}

	now := time.Now().UTC()
	cutoff24h := now.Add(-24 * time.Hour)
	cutoff7d := now.Add(-7 * 24 * time.Hour)

	// Active relay and event-volume counts come from the incrementally
	// maintained relay_activity_hourly rollup instead of scanning the raw
	// 24h/7d event_relays windows (~8.5s combined) on every 5-minute
	// refresh. Rows newer than the rollup watermark are merged in from the
	// raw table — the same rolled+tail pattern computeTopRelaysSnapshot
	// uses — so the counts stay exact despite the rollup's ingest lag.
	var watermark time.Time
	if err := tx.QueryRow(ctx, `
		SELECT rolled_up_until FROM relay_activity_rollup_state WHERE id
	`).Scan(&watermark); err != nil {
		return out, fmt.Errorf("read rollup watermark: %w", err)
	}
	const windowFromRollupQuery = `
		SELECT
			COALESCE(COUNT(DISTINCT relay_url), 0)::bigint AS active,
			COALESCE(SUM(event_count), 0)::bigint          AS events
		FROM (
			SELECT relay_url, SUM(event_count)::bigint AS event_count
			FROM relay_activity_hourly
			WHERE bucket_start >= date_trunc('hour', $1::timestamptz)
			GROUP BY relay_url
			UNION ALL
			SELECT relay_url, COUNT(*)::bigint
			FROM event_relays
			WHERE seen_at > $2
			  AND seen_at >= $1
			GROUP BY relay_url
		) merged
		WHERE relay_url <> $3
	`
	if err := tx.QueryRow(ctx, windowFromRollupQuery, cutoff24h, watermark, model.FallbackRelayURL).
		Scan(&out.Active24h, &out.Events24h); err != nil {
		return out, fmt.Errorf("query 24h window rollup: %w", err)
	}
	if err := tx.QueryRow(ctx, windowFromRollupQuery, cutoff7d, watermark, model.FallbackRelayURL).
		Scan(&out.Active7d, &out.Events7d); err != nil {
		return out, fmt.Errorf("query 7d window rollup: %w", err)
	}

	// The distinct-author counts have no rollup (a per-relay-author rollup
	// would be nearly as large as the raw table), so they still scan raw
	// event_relays — but only once per relaySummaryAuthorsRefreshInterval.
	// In between, the values from the previous snapshot are carried
	// forward: a distinct count over a 24h/7d window moves far too slowly
	// for hourly staleness to matter.
	prev, hasPrev, err := loadPreviousRelaySummarySnapshot(ctx, tx)
	if err != nil {
		return out, err
	}
	if hasPrev && prev.AuthorsComputedAt > 0 &&
		now.Unix()-prev.AuthorsComputedAt < int64(relaySummaryAuthorsRefreshInterval/time.Second) {
		out.Authors24h = prev.Authors24h
		out.Authors7d = prev.Authors7d
		out.AuthorsComputedAt = prev.AuthorsComputedAt
		return out, nil
	}

	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(COUNT(DISTINCT pubkey), 0)::bigint
		FROM event_relays
		WHERE seen_at >= $1
		  AND relay_url <> $2
	`, cutoff24h, model.FallbackRelayURL).Scan(&out.Authors24h); err != nil {
		return out, fmt.Errorf("query 24h window authors: %w", err)
	}
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(COUNT(DISTINCT pubkey), 0)::bigint
		FROM event_relays
		WHERE seen_at >= $1
		  AND relay_url <> $2
	`, cutoff7d, model.FallbackRelayURL).Scan(&out.Authors7d); err != nil {
		return out, fmt.Errorf("query 7d window authors: %w", err)
	}
	out.AuthorsComputedAt = now.Unix()
	return out, nil
}

// relaySummaryAuthorsRefreshInterval is how often the raw COUNT(DISTINCT
// pubkey) author aggregates in the relay summary are recomputed. See
// computeRelaySummarySnapshot for why they are throttled independently of
// the 5-minute snapshot cadence.
const relaySummaryAuthorsRefreshInterval = time.Hour

// loadPreviousRelaySummarySnapshot reads the currently stored summary
// payload (if any) so throttled fields can be carried forward.
func loadPreviousRelaySummarySnapshot(ctx context.Context, tx pgx.Tx) (relaySummarySnapshotPayload, bool, error) {
	var payload []byte
	err := tx.QueryRow(ctx, `
		SELECT payload
		FROM relay_window_snapshots
		WHERE snapshot_label = $1
	`, relaySnapshotLabelSummary).Scan(&payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return relaySummarySnapshotPayload{}, false, nil
		}
		return relaySummarySnapshotPayload{}, false, fmt.Errorf("read previous relay summary snapshot: %w", err)
	}
	var prev relaySummarySnapshotPayload
	if err := json.Unmarshal(payload, &prev); err != nil {
		// A corrupt or legacy payload just means we recompute everything.
		return relaySummarySnapshotPayload{}, false, nil
	}
	return prev, true, nil
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
//
// The candidate set is restricted to authors inside the Web of Trust
// via trustedAuthorJoinClause — see refreshTrendingLinksSnapshots for
// why. This means "trending hashtags" on the homepage is a trusted-
// network view, not a network-wide one; the live
// /api/v1/discovery/hashtags/trending endpoint (store.GetTrendingHashtags)
// is unaffected and remains network-wide.
func computeTopHashtagsSnapshot(ctx context.Context, tx pgx.Tx, minCreatedAt int64, limit int) ([]hashtagRow, error) {
	if limit <= 0 {
		limit = 50
	}
	query := fmt.Sprintf(`
		SELECT
			h.hashtag,
			COUNT(*)::bigint                       AS event_count,
			COUNT(DISTINCT h.author_pubkey)::bigint AS unique_authors
		FROM event_hashtags h
		%s
		WHERE h.created_at >= $1
		GROUP BY h.hashtag
		ORDER BY
			unique_authors DESC,
			(COUNT(DISTINCT h.author_pubkey))::double precision / GREATEST(COUNT(*), 1) DESC,
			event_count DESC,
			h.hashtag ASC
		LIMIT $2
	`, fmt.Sprintf(trustedAuthorJoinClause, "h"))
	rows, err := tx.Query(ctx, query, minCreatedAt, limit)
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

// domainActivityRow mirrors one 24h/7d activity sub-object inside a
// top_domains_24h / top_domains_7d snapshot entry.
type domainActivityRow struct {
	LinkCount     int64 `json:"link_count"`
	NoteCount     int64 `json:"note_count"`
	UniqueAuthors int64 `json:"unique_authors"`
}

// domainRow mirrors one entry inside the top_domains_24h / top_domains_7d
// snapshot payload — the same shape store.GetTrendingDomains scans from a
// live query, just precomputed.
type domainRow struct {
	Domain        string            `json:"domain"`
	LatestEventAt *int64            `json:"latest_event_at"`
	Activity24h   domainActivityRow `json:"activity_24h"`
	Activity7d    domainActivityRow `json:"activity_7d"`
}

// computeTopDomainsSnapshot mirrors the ranking used by
// store.GetTrendingDomains for a fixed window: the candidate set is
// restricted to event_urls rows created after windowFloor (24h-ago for the
// "24h" snapshot, 7d-ago for the "7d" snapshot), ranked by unique-author
// breadth within that same window, then diversity, note count, and link
// count, all computed within the restricted candidate set. cutoff24h and
// cutoff7d additionally populate the 24h/7d activity sub-objects the
// response exposes regardless of which window is being ranked (matching the
// live query's two FILTER clauses).
//
// The candidate set is further restricted to authors inside the Web of
// Trust via trustedAuthorJoinClause — see refreshTrendingLinksSnapshots.
// The live /api/v1/discovery/domains/trending endpoint
// (store.GetTrendingDomains) is unaffected and remains network-wide.
func computeTopDomainsSnapshot(
	ctx context.Context,
	tx pgx.Tx,
	windowFloor int64,
	cutoff24h int64,
	cutoff7d int64,
	limit int,
) ([]domainRow, error) {
	if limit <= 0 {
		limit = 50
	}
	query := fmt.Sprintf(`
		SELECT
			u.canonical_domain,
			MAX(u.created_at),
			COUNT(*) FILTER (WHERE u.created_at >= $1),
			COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $1),
			COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $1),
			COUNT(*) FILTER (WHERE u.created_at >= $2),
			COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $2),
			COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $2)
		FROM event_urls u
		%s
		WHERE u.created_at >= $3
		  AND %s
		GROUP BY u.canonical_domain
		ORDER BY
			COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $3) DESC,
			(COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $3))::double precision /
				GREATEST(COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $3), 1) DESC,
			COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $3) DESC,
			COUNT(*) FILTER (WHERE u.created_at >= $3) DESC,
			u.canonical_domain ASC
		LIMIT $4
	`, fmt.Sprintf(trustedAuthorJoinClause, "u"), domainMediaURLFilterClause)
	rows, err := tx.Query(ctx, query, cutoff24h, cutoff7d, windowFloor, limit)
	if err != nil {
		return nil, fmt.Errorf("query top domains: %w", err)
	}
	defer rows.Close()
	out := make([]domainRow, 0, limit)
	for rows.Next() {
		var row domainRow
		if err := rows.Scan(
			&row.Domain,
			&row.LatestEventAt,
			&row.Activity24h.LinkCount,
			&row.Activity24h.NoteCount,
			&row.Activity24h.UniqueAuthors,
			&row.Activity7d.LinkCount,
			&row.Activity7d.NoteCount,
			&row.Activity7d.UniqueAuthors,
		); err != nil {
			return nil, fmt.Errorf("scan top domain row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read top domain rows: %w", err)
	}
	return out, nil
}

// computeTopRelaysSnapshot ranks relays by 7d event volume using the
// incremental relay_activity_hourly rollup instead of aggregating the raw
// event_relays window (see refreshRelayActivityRollup for why — the raw
// GROUP BY scaled with ingest volume and froze the snapshot during the
// relay-cap incident).
//
// Exactness: the rollup covers seen_at <= rolled_up_until and the "tail"
// branch covers seen_at > rolled_up_until from the raw table, so together
// they count every row exactly once. The tail is at most the rollup lag
// plus one refresh interval of ingest. Two deliberate approximations vs the
// old query: the rollup's oldest bucket is hour-truncated, so up to one
// extra hour of activity can count into the 7d window; and the LIMIT is
// ranked by event_count with relay_url as tie-break (unique_authors — which
// would need a per-relay DISTINCT for every candidate — only orders the
// final top rows, where it is computed via bounded index-only probes of
// idx_event_relays_relay_url_seen_at).
func computeTopRelaysSnapshot(ctx context.Context, tx pgx.Tx, limit int) ([]relayActivityRow, error) {
	if limit <= 0 {
		limit = 10
	}
	var watermark time.Time
	if err := tx.QueryRow(ctx, `
		SELECT rolled_up_until FROM relay_activity_rollup_state WHERE id
	`).Scan(&watermark); err != nil {
		return nil, fmt.Errorf("read rollup watermark: %w", err)
	}
	cutoff7d := time.Now().UTC().Add(-7 * 24 * time.Hour)
	rows, err := tx.Query(ctx, `
		WITH rolled AS (
			SELECT relay_url, SUM(event_count)::bigint AS event_count
			FROM relay_activity_hourly
			WHERE bucket_start >= date_trunc('hour', $1::timestamptz)
			GROUP BY relay_url
		), tail AS (
			SELECT relay_url, COUNT(*)::bigint AS event_count
			FROM event_relays
			WHERE seen_at > $2
			  AND seen_at >= $1
			GROUP BY relay_url
		), ranked AS (
			SELECT relay_url, SUM(event_count)::bigint AS event_count
			FROM (
				SELECT relay_url, event_count FROM rolled
				UNION ALL
				SELECT relay_url, event_count FROM tail
			) merged
			WHERE relay_url <> $3
			GROUP BY relay_url
			ORDER BY event_count DESC, relay_url ASC
			LIMIT $4
		)
		SELECT
			r.relay_url,
			r.event_count,
			(
				SELECT COALESCE(COUNT(DISTINCT er.pubkey), 0)
				FROM event_relays er
				WHERE er.relay_url = r.relay_url
				  AND er.seen_at >= $1
			)::bigint AS unique_authors
		FROM ranked r
		ORDER BY r.event_count DESC, unique_authors DESC, r.relay_url ASC
	`, cutoff7d, watermark, model.FallbackRelayURL, limit)
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
