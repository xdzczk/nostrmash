package retention

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// untrustedPurgeKinds are the author-gated kinds PurgeUntrustedAuthorEvents
// reclaims. Kind 1 replies are filtered out per page (thread_edges), not by
// dropping kind 1 entirely.
var untrustedPurgeKinds = []int32{1, 4, 5, 9802, 10000, 10003, 30023}

const (
	untrustedPurgePageSize = 2000
	// untrustedPurgeSyncBudget caps one sync. While the per-kind cursor is
	// behind the horizon the tick spends up to this long paging
	// idx_events_kind_first_seen. Once caught up, each kind is an empty
	// index probe and the tick returns immediately. This is the bound the
	// old full-table anti-join did not have.
	untrustedPurgeSyncBudget = 3 * time.Minute
	// Distinct from the migration advisory lock. Held on one pool connection
	// for the duration of a sync so the retention loop and the storage
	// governor cannot page the same cursor concurrently.
	untrustedPurgeSyncLockID int64 = 87120319824401
)

// errUntrustedSnapshotEmpty stops a sync without advancing cursors. An empty
// trust_graph_snapshot must not classify every author as untrusted.
var errUntrustedSnapshotEmpty = errors.New("trust snapshot empty")

// syncUntrustedPurgeCandidates pages newly-eligible untrusted events into
// untrusted_purge_candidates. Safe to call on every purge: an empty snapshot
// is a no-op, and a second caller observes the advisory lock and returns.
func (s *Retention) syncUntrustedPurgeCandidates(ctx context.Context, olderThan time.Time) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire untrusted purge sync conn: %w", err)
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, untrustedPurgeSyncLockID).Scan(&locked); err != nil {
		return fmt.Errorf("lock untrusted purge sync: %w", err)
	}
	if !locked {
		return nil
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, untrustedPurgeSyncLockID)
	}()

	// 2 minutes covers the one-time mirror fill (~150k pubkeys) under memory
	// pressure. A page that still exceeds it is a failed plan, not a scan
	// we should let run to the old 13-minute timeout.
	if _, err := conn.Exec(ctx, `SET statement_timeout = '2min'`); err != nil {
		return fmt.Errorf("set untrusted purge sync statement_timeout: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `RESET statement_timeout`)
	}()

	if err := untrustedSnapshotPresent(ctx, conn); err != nil {
		if errors.Is(err, errUntrustedSnapshotEmpty) {
			return nil
		}
		return err
	}
	if err := diffUntrustedTrustMirror(ctx, conn); err != nil {
		return err
	}

	deadline := time.Now().Add(untrustedPurgeSyncBudget)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := untrustedSnapshotPresent(ctx, conn); err != nil {
			if errors.Is(err, errUntrustedSnapshotEmpty) {
				return nil
			}
			return err
		}
		progressed, err := resyncOneUntrustedAuthor(ctx, conn, olderThan)
		if err != nil {
			return err
		}
		if !progressed {
			break
		}
	}
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := untrustedSnapshotPresent(ctx, conn); err != nil {
			if errors.Is(err, errUntrustedSnapshotEmpty) {
				return nil
			}
			return err
		}
		n, err := promoteDeferredUntrusted(ctx, conn, olderThan)
		if err != nil {
			return err
		}
		if n < untrustedPurgePageSize {
			break
		}
	}
	for _, kind := range untrustedPurgeKinds {
		for time.Now().Before(deadline) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := untrustedSnapshotPresent(ctx, conn); err != nil {
				if errors.Is(err, errUntrustedSnapshotEmpty) {
					return nil
				}
				return err
			}
			full, err := backfillUntrustedKindPage(ctx, conn, kind, olderThan)
			if err != nil {
				return err
			}
			if !full {
				break
			}
		}
	}
	return nil
}

func untrustedSnapshotPresent(ctx context.Context, conn *pgxpool.Conn) error {
	var present bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM trust_graph_snapshot)`).Scan(&present); err != nil {
		return fmt.Errorf("check trust snapshot: %w", err)
	}
	if !present {
		return errUntrustedSnapshotEmpty
	}
	return nil
}

// diffUntrustedTrustMirror queues authors who disappeared from the snapshot,
// drops candidates who became trusted, and refreshes the pubkey mirror.
// Data-modifying CTEs all run to completion.
const diffUntrustedTrustMirrorSQL = `
WITH removed AS (
    INSERT INTO untrusted_purge_author_resync (pubkey)
    SELECT m.pubkey
    FROM untrusted_purge_trusted_pubkeys m
    WHERE NOT EXISTS (
        SELECT 1 FROM trust_graph_snapshot s WHERE s.pubkey = m.pubkey
    )
    ON CONFLICT (pubkey) DO NOTHING
    RETURNING pubkey
),
drop_mirror AS (
    DELETE FROM untrusted_purge_trusted_pubkeys m
    WHERE NOT EXISTS (
        SELECT 1 FROM trust_graph_snapshot s WHERE s.pubkey = m.pubkey
    )
    RETURNING m.pubkey
),
drop_cand AS (
    DELETE FROM untrusted_purge_candidates c
    WHERE EXISTS (
        SELECT 1 FROM trust_graph_snapshot s WHERE s.pubkey = c.pubkey
    )
    RETURNING c.event_id
),
drop_def AS (
    DELETE FROM untrusted_purge_deferred d
    WHERE EXISTS (
        SELECT 1 FROM trust_graph_snapshot s WHERE s.pubkey = d.pubkey
    )
    RETURNING d.event_id
)
INSERT INTO untrusted_purge_trusted_pubkeys (pubkey)
SELECT s.pubkey
FROM trust_graph_snapshot s
WHERE (SELECT count(*) FROM removed) >= 0
  AND (SELECT count(*) FROM drop_mirror) >= 0
  AND (SELECT count(*) FROM drop_cand) >= 0
  AND (SELECT count(*) FROM drop_def) >= 0
ON CONFLICT (pubkey) DO NOTHING
`

func diffUntrustedTrustMirror(ctx context.Context, conn *pgxpool.Conn) error {
	if _, err := conn.Exec(ctx, diffUntrustedTrustMirrorSQL); err != nil {
		return fmt.Errorf("diff untrusted trust mirror: %w", err)
	}
	return nil
}

// Page of one author's events. The pubkey index bounds this to that author.
// Eligible rows land in the candidate or deferred table; the caller advances
// the per-author cursor to the last scanned id.
const resyncUntrustedAuthorPageSQL = `
WITH page AS (
    SELECT e.id, e.pubkey, e.kind, e.created_at, e.first_seen_at
    FROM events e
    WHERE e.pubkey = $1
      AND e.kind = ANY($2::int[])
      AND (e.created_at, e.id) > ($3::bigint, $4::text)
    ORDER BY e.created_at ASC, e.id ASC
    LIMIT $5
),
eligible AS (
    SELECT p.id, p.pubkey, p.kind, p.created_at, p.first_seen_at
    FROM page p
    WHERE NOT EXISTS (
        SELECT 1 FROM trust_graph_snapshot s WHERE s.pubkey = p.pubkey
    )
      AND NOT (
        p.kind = 1
        AND EXISTS (
            SELECT 1 FROM thread_edges te WHERE te.child_event_id = p.id
        )
    )
),
ins_cand AS (
    INSERT INTO untrusted_purge_candidates (event_id, pubkey, kind, created_at, first_seen_at)
    SELECT id, pubkey, kind, created_at, first_seen_at
    FROM eligible
    WHERE created_at < $6::bigint
      AND first_seen_at < $7
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id
),
ins_def AS (
    INSERT INTO untrusted_purge_deferred (
        event_id, pubkey, kind, created_at, first_seen_at, eligible_at
    )
    SELECT id, pubkey, kind, created_at, first_seen_at,
           GREATEST(first_seen_at, to_timestamp(created_at))
    FROM eligible
    WHERE created_at >= $6::bigint
       OR first_seen_at >= $7
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id
)
SELECT p.id, p.created_at, p.first_seen_at
FROM page p
WHERE (SELECT count(*) FROM ins_cand) >= 0
  AND (SELECT count(*) FROM ins_def) >= 0
ORDER BY p.created_at ASC, p.id ASC
`

func resyncOneUntrustedAuthor(ctx context.Context, conn *pgxpool.Conn, olderThan time.Time) (bool, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin untrusted author resync: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var pubkey, cursorID string
	var cursorAt int64
	err = tx.QueryRow(ctx, `
		SELECT pubkey, cursor_created_at, cursor_event_id
		FROM untrusted_purge_author_resync
		ORDER BY pubkey
		LIMIT 1
	`).Scan(&pubkey, &cursorAt, &cursorID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load untrusted author resync: %w", err)
	}

	// The author may have been trusted again since the diff queued them.
	// Drop the queue row; their events must not become candidates.
	var trusted bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM trust_graph_snapshot WHERE pubkey = $1)`, pubkey).Scan(&trusted); err != nil {
		return false, fmt.Errorf("check resync author trust: %w", err)
	}
	if trusted {
		if _, err := tx.Exec(ctx, `DELETE FROM untrusted_purge_author_resync WHERE pubkey = $1`, pubkey); err != nil {
			return false, fmt.Errorf("drop retrusted author resync: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit retrusted author resync: %w", err)
		}
		return true, nil
	}

	scanned, err := scanUntrustedPage(ctx, tx, resyncUntrustedAuthorPageSQL, pubkey, untrustedPurgeKinds, cursorAt, cursorID, untrustedPurgePageSize, olderThan.UTC().Unix(), olderThan.UTC())
	if err != nil {
		return false, fmt.Errorf("resync untrusted author %s: %w", pubkey, err)
	}
	if len(scanned) == 0 || len(scanned) < untrustedPurgePageSize {
		if _, err := tx.Exec(ctx, `DELETE FROM untrusted_purge_author_resync WHERE pubkey = $1`, pubkey); err != nil {
			return false, fmt.Errorf("finish untrusted author resync: %w", err)
		}
	} else {
		last := scanned[len(scanned)-1]
		if _, err := tx.Exec(ctx, `
			UPDATE untrusted_purge_author_resync
			SET cursor_created_at = $2, cursor_event_id = $3
			WHERE pubkey = $1
		`, pubkey, last.createdAt, last.id); err != nil {
			return false, fmt.Errorf("advance untrusted author resync: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit untrusted author resync: %w", err)
	}
	return true, nil
}

const promoteDeferredUntrustedSQL = `
WITH due AS (
    SELECT event_id, pubkey, kind, created_at, first_seen_at
    FROM untrusted_purge_deferred
    WHERE eligible_at <= $1
    ORDER BY eligible_at ASC, event_id ASC
    LIMIT $2
),
ins AS (
    INSERT INTO untrusted_purge_candidates (event_id, pubkey, kind, created_at, first_seen_at)
    SELECT d.event_id, d.pubkey, d.kind, d.created_at, d.first_seen_at
    FROM due d
    WHERE EXISTS (SELECT 1 FROM trust_graph_snapshot)
      AND NOT EXISTS (
          SELECT 1 FROM trust_graph_snapshot s WHERE s.pubkey = d.pubkey
      )
      AND d.created_at < $3::bigint
      AND d.first_seen_at < $1
      AND NOT (
          d.kind = 1
          AND EXISTS (
              SELECT 1 FROM thread_edges te WHERE te.child_event_id = d.event_id
          )
      )
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id
),
del AS (
    -- Only drop a deferred row once it is a candidate, already queued, or
    -- no longer purgeable. A row that is due only because eligible_at
    -- landed on the cutoff second must stay: deleting it would lose the
    -- event, because the kind cursor has already moved past it.
    DELETE FROM untrusted_purge_deferred d
    USING due
    WHERE d.event_id = due.event_id
      AND EXISTS (SELECT 1 FROM trust_graph_snapshot)
      AND (
          EXISTS (SELECT 1 FROM ins i WHERE i.event_id = d.event_id)
          OR EXISTS (
              SELECT 1 FROM untrusted_purge_candidates c WHERE c.event_id = d.event_id
          )
          OR EXISTS (
              SELECT 1 FROM trust_graph_snapshot s WHERE s.pubkey = d.pubkey
          )
          OR (
              d.kind = 1
              AND EXISTS (
                  SELECT 1 FROM thread_edges te WHERE te.child_event_id = d.event_id
              )
          )
      )
    RETURNING d.event_id
)
SELECT count(*) FROM del
WHERE (SELECT count(*) FROM ins) >= 0
`

func promoteDeferredUntrusted(ctx context.Context, conn *pgxpool.Conn, olderThan time.Time) (int, error) {
	var n int64
	if err := conn.QueryRow(ctx, promoteDeferredUntrustedSQL, olderThan.UTC(), untrustedPurgePageSize, olderThan.UTC().Unix()).Scan(&n); err != nil {
		return 0, fmt.Errorf("promote deferred untrusted events: %w", err)
	}
	return int(n), nil
}

// backfillUntrustedKindPageSQL pages one kind by first_seen_at so Postgres
// can walk idx_events_kind_first_seen. The page is already inside the horizon
// (first_seen_at < cutoff). Trusted rows and kind-1 replies are not stored;
// the cursor still moves past them. Rows dated after the horizon
// (created_at still in the future relative to cutoff) go to
// untrusted_purge_deferred and are promoted once created_at ages in.
const backfillUntrustedKindPageSQL = `
WITH page AS (
    SELECT e.id, e.pubkey, e.kind, e.created_at, e.first_seen_at
    FROM events e
    WHERE e.kind = $1
      AND (e.first_seen_at, e.id) > ($2::timestamptz, $3::text)
      AND e.first_seen_at < $4
    ORDER BY e.first_seen_at ASC, e.id ASC
    LIMIT $5
),
eligible AS (
    SELECT p.id, p.pubkey, p.kind, p.created_at, p.first_seen_at
    FROM page p
    WHERE EXISTS (SELECT 1 FROM trust_graph_snapshot)
      AND NOT EXISTS (
          SELECT 1 FROM trust_graph_snapshot s WHERE s.pubkey = p.pubkey
      )
      AND NOT (
          p.kind = 1
          AND EXISTS (
              SELECT 1 FROM thread_edges te WHERE te.child_event_id = p.id
          )
      )
),
ins_cand AS (
    INSERT INTO untrusted_purge_candidates (event_id, pubkey, kind, created_at, first_seen_at)
    SELECT id, pubkey, kind, created_at, first_seen_at
    FROM eligible
    WHERE created_at < $6::bigint
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id
),
ins_def AS (
    INSERT INTO untrusted_purge_deferred (
        event_id, pubkey, kind, created_at, first_seen_at, eligible_at
    )
    SELECT id, pubkey, kind, created_at, first_seen_at,
           GREATEST(first_seen_at, to_timestamp(created_at))
    FROM eligible
    WHERE created_at >= $6::bigint
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id
)
SELECT p.id, p.created_at, p.first_seen_at
FROM page p
WHERE (SELECT count(*) FROM ins_cand) >= 0
  AND (SELECT count(*) FROM ins_def) >= 0
ORDER BY p.first_seen_at ASC, p.id ASC
`

func backfillUntrustedKindPage(ctx context.Context, conn *pgxpool.Conn, kind int32, olderThan time.Time) (bool, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin untrusted kind backfill: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var cursorSeen time.Time
	var cursorID string
	if err := tx.QueryRow(ctx, `
		SELECT first_seen_at, event_id
		FROM untrusted_purge_kind_cursor
		WHERE kind = $1
	`, kind).Scan(&cursorSeen, &cursorID); err != nil {
		return false, fmt.Errorf("load untrusted kind cursor %d: %w", kind, err)
	}

	scanned, err := scanUntrustedPage(ctx, tx, backfillUntrustedKindPageSQL, kind, cursorSeen.UTC(), cursorID, olderThan.UTC(), untrustedPurgePageSize, olderThan.UTC().Unix())
	if err != nil {
		return false, fmt.Errorf("backfill untrusted kind %d: %w", kind, err)
	}
	if len(scanned) == 0 {
		return false, nil
	}
	last := scanned[len(scanned)-1]
	if _, err := tx.Exec(ctx, `
		UPDATE untrusted_purge_kind_cursor
		SET first_seen_at = $2, event_id = $3
		WHERE kind = $1
	`, kind, last.firstSeen.UTC(), last.id); err != nil {
		return false, fmt.Errorf("advance untrusted kind cursor %d: %w", kind, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit untrusted kind backfill: %w", err)
	}
	return len(scanned) == untrustedPurgePageSize, nil
}

type untrustedScannedEvent struct {
	id        string
	createdAt int64
	firstSeen time.Time
}

func scanUntrustedPage(ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]untrustedScannedEvent, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []untrustedScannedEvent
	for rows.Next() {
		var ev untrustedScannedEvent
		if err := rows.Scan(&ev.id, &ev.createdAt, &ev.firstSeen); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
