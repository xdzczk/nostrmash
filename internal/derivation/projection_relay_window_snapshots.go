package derivation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RefreshRelayWindowSnapshots recomputes the homepage relay summary
// stats (24h / 7d windows + all-time relay total) and the top-10
// relays by 7-day activity, then upserts them into
// relay_window_snapshots.
//
// Why this is a projection, not an inline query
// ---------------------------------------------
// The underlying COUNT(DISTINCT pubkey) over event_relays is
// CPU-bound at ~9s on production for the 7d window — it has to hash
// 3.7M rows into 120k distinct pubkey buckets. There is no SQL trick
// or index that can reduce that cost; it is fundamental to the
// cardinality of the input. Running it on every homepage cache miss
// caused the /api/v1/discovery/home endpoint to time out (30s) and
// starve every other endpoint of database connections.
//
// Running it once every few minutes from a background worker, then
// serving the result from a single-row lookup, makes the homepage
// O(1) and removes the slow query from the request path entirely.
//
// Concurrency safety
// ------------------
// Two replicas calling this concurrently is safe — the upsert is
// idempotent and the reads are non-locking. We do not bother with
// an advisory lock to dedupe; doing the work twice every 5 minutes
// is cheaper than the lock contention machinery.
//
// Failure handling
// ----------------
// On error the previous snapshot row is left in place. Callers
// (the worker loop) log the error but do not block — the homepage
// keeps serving the last good snapshot until the next refresh
// succeeds.
func (h *Handlers) RefreshRelayWindowSnapshots(ctx context.Context) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		// Default work_mem (4MB) forces the COUNT(DISTINCT pubkey)
		// hashtables to spill ~200MB to disk for the 7d window. 128MB
		// keeps everything in memory; SET LOCAL releases it on commit
		// so the pool connection is unaffected.
		if _, err := tx.Exec(ctx, `SET LOCAL work_mem = '128MB'`); err != nil {
			return fmt.Errorf("set work_mem: %w", err)
		}

		summary, err := computeRelaySummarySnapshot(ctx, tx)
		if err != nil {
			return fmt.Errorf("compute relay summary snapshot: %w", err)
		}
		summaryPayload, err := json.Marshal(summary)
		if err != nil {
			return fmt.Errorf("marshal relay summary snapshot: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
			VALUES ($1, $2::jsonb, now())
			ON CONFLICT (snapshot_label) DO UPDATE
			SET payload     = EXCLUDED.payload,
			    computed_at = EXCLUDED.computed_at
		`, relaySnapshotLabelSummary, summaryPayload); err != nil {
			return fmt.Errorf("upsert relay summary snapshot: %w", err)
		}

		topRelays, err := computeTopRelaysSnapshot(ctx, tx, 10)
		if err != nil {
			return fmt.Errorf("compute top relays snapshot: %w", err)
		}
		// Always marshal as a JSON array, even when empty, so the
		// reader can JSON-decode without a NULL-check branch.
		if topRelays == nil {
			topRelays = []relayActivityRow{}
		}
		topPayload, err := json.Marshal(topRelays)
		if err != nil {
			return fmt.Errorf("marshal top relays snapshot: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
			VALUES ($1, $2::jsonb, now())
			ON CONFLICT (snapshot_label) DO UPDATE
			SET payload     = EXCLUDED.payload,
			    computed_at = EXCLUDED.computed_at
		`, relaySnapshotLabelTopRelays7d, topPayload); err != nil {
			return fmt.Errorf("upsert top relays snapshot: %w", err)
		}
		return nil
	})
}

// Snapshot label constants — also referenced by the API store
// when reading. Keep these in sync with the seed in migration
// 000047_relay_window_snapshots.sql.
const (
	relaySnapshotLabelSummary     = "summary"
	relaySnapshotLabelTopRelays7d = "top_relays_7d"
)

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
