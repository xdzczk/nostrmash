-- Static retention sweeps for the retention bounded context. Every statement
-- is a bounded DELETE/UPDATE that returns the number of affected rows
-- (:execrows). The hand-written wrappers in internal/store/retention own
-- validation, metrics, and transaction shape; these are pure SQL plumbing.

-- name: PurgeExpiredEngagementEvents :execrows
WITH candidates AS (
    SELECT e.id
    FROM events e
    WHERE e.kind IN (6, 7, 9735)
      AND e.created_at < @created_before_unix::bigint
      AND NOT EXISTS (
        SELECT 1
        FROM jobs j
        WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
          AND (
            j.status IN ('pending', 'running')
            OR (j.status = 'dead' AND j.updated_at > @dead_grace_before)
          )
      )
    ORDER BY e.created_at ASC, e.id ASC
    LIMIT @row_limit
)
DELETE FROM events e
USING candidates c
WHERE e.id = c.id;

-- name: SelectExpiredEngagementEventCandidates :many
-- Read-only counterpart to PurgeExpiredEngagementEvents' candidates CTE,
-- used by the Go wrapper to reverse incremental author-stat deltas for each
-- candidate before deleting it (see internal/store/retention/events_retention.go).
SELECT e.id
FROM events e
WHERE e.kind IN (6, 7, 9735)
  AND e.created_at < @created_before_unix::bigint
  AND NOT EXISTS (
    SELECT 1
    FROM jobs j
    WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
      AND (
        j.status IN ('pending', 'running')
        OR (j.status = 'dead' AND j.updated_at > @dead_grace_before)
      )
  )
ORDER BY e.created_at ASC, e.id ASC
LIMIT @row_limit;

-- name: PurgeSupersededReplaceableEvents :execrows
WITH candidates AS (
    SELECT e.id
    FROM events e
    JOIN LATERAL (
        SELECT COALESCE((
            SELECT et.value
            FROM event_tags et
            WHERE et.event_id = e.id
              AND et.tag_name = 'd'
              AND et.tag_index >= 0
              AND et.value_index = 0
            ORDER BY et.tag_index ASC
            LIMIT 1
        ), '') AS d_tag
    ) dd ON true
    JOIN replaceable_state rs
      ON rs.pubkey = e.pubkey
     AND rs.kind = e.kind
     AND rs.d_tag = dd.d_tag
    WHERE e.kind = ANY(@kinds::int[])
      AND e.first_seen_at < @superseded_before
      AND (
        rs.created_at > e.created_at
        OR (rs.created_at = e.created_at AND rs.event_id > e.id)
      )
      AND NOT EXISTS (
        SELECT 1
        FROM jobs j
        WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
          AND (
            j.status IN ('pending', 'running')
            OR (j.status = 'dead' AND j.updated_at > @dead_grace_before)
          )
      )
    ORDER BY e.created_at ASC, e.id ASC
    LIMIT @row_limit
)
DELETE FROM events e
USING candidates c
WHERE e.id = c.id;

-- name: PurgeProcessedDeletionEvents :execrows
WITH candidates AS (
    SELECT e.id
    FROM events e
    WHERE e.kind = 5
      AND e.created_at < @created_before_unix::bigint
      AND NOT EXISTS (
        SELECT 1
        FROM jobs j
        WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
          AND (
            j.status IN ('pending', 'running')
            OR (j.status = 'dead' AND j.updated_at > @dead_grace_before)
          )
      )
    ORDER BY e.created_at ASC, e.id ASC
    LIMIT @row_limit
)
DELETE FROM events e
USING candidates c
WHERE e.id = c.id;

-- name: PurgeUntrustedAuthorEvents :execrows
WITH candidates AS (
    SELECT e.id
    FROM events e
    WHERE e.kind IN (1, 4, 5, 9802, 10000, 10003, 30023)
      AND e.created_at < @created_before_unix::bigint
      AND e.first_seen_at < @first_seen_before
      AND EXISTS (SELECT 1 FROM trust_graph_snapshot)
      AND NOT EXISTS (
        SELECT 1 FROM trust_graph_snapshot s
        WHERE s.pubkey = e.pubkey
      )
      -- Kind-1 replies accepted via target-exists ingest are kept so conversation
      -- totals are not rolled back by untrusted-author retention. Root notes from
      -- untrusted authors remain purgeable.
      AND NOT (
        e.kind = 1
        AND EXISTS (
          SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
        )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM jobs j
        WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
          AND (
            j.status IN ('pending', 'running')
            OR (j.status = 'dead' AND j.updated_at > @dead_grace_before)
          )
      )
    ORDER BY e.created_at ASC, e.id ASC
    LIMIT @row_limit
)
DELETE FROM events e
USING candidates c
WHERE e.id = c.id;

-- name: SelectUntrustedAuthorEventCandidates :many
-- Read-only counterpart to PurgeUntrustedAuthorEvents' candidates CTE, used
-- by the Go wrapper to reverse incremental author-stat deltas for each
-- candidate before deleting it (see
-- internal/store/retention/events_retention_untrusted.go).
SELECT e.id
FROM events e
WHERE e.kind IN (1, 4, 5, 9802, 10000, 10003, 30023)
  AND e.created_at < @created_before_unix::bigint
  AND e.first_seen_at < @first_seen_before
  AND EXISTS (SELECT 1 FROM trust_graph_snapshot)
  AND NOT EXISTS (
    SELECT 1 FROM trust_graph_snapshot s
    WHERE s.pubkey = e.pubkey
  )
  AND NOT (
    e.kind = 1
    AND EXISTS (
      SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
    )
  )
  AND NOT EXISTS (
    SELECT 1
    FROM jobs j
    WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
      AND (
        j.status IN ('pending', 'running')
        OR (j.status = 'dead' AND j.updated_at > @dead_grace_before)
      )
  )
ORDER BY e.created_at ASC, e.id ASC
LIMIT @row_limit;

-- name: DeleteEventsByID :execrows
-- Deletes an explicit, already-decided set of event ids. Used after
-- SelectExpiredEngagementEventCandidates / SelectUntrustedAuthorEventCandidates
-- so the same candidate set that had its incremental stats reversed is the
-- exact set deleted (no re-evaluating the candidate predicate a second time
-- against a possibly-changed table state).
DELETE FROM events
WHERE id = ANY(@ids::text[]);

-- name: PurgeUntrustedAuthorEventURLs :execrows
WITH candidates AS (
    SELECT u.event_id, u.url
    FROM event_urls u
    WHERE EXISTS (SELECT 1 FROM trust_graph_snapshot)
      AND NOT EXISTS (
        SELECT 1 FROM trust_graph_snapshot s
        WHERE s.pubkey = u.author_pubkey
      )
    ORDER BY u.created_at ASC, u.event_id ASC, u.url ASC
    LIMIT @row_limit
)
DELETE FROM event_urls u
USING candidates c
WHERE u.event_id = c.event_id
  AND u.url = c.url;

-- name: PurgeUntrustedAuthorEventHashtags :execrows
WITH candidates AS (
    SELECT h.event_id, h.hashtag
    FROM event_hashtags h
    WHERE EXISTS (SELECT 1 FROM trust_graph_snapshot)
      AND NOT EXISTS (
        SELECT 1 FROM trust_graph_snapshot s
        WHERE s.pubkey = h.author_pubkey
      )
    ORDER BY h.created_at ASC, h.event_id ASC, h.hashtag ASC
    LIMIT @row_limit
)
DELETE FROM event_hashtags h
USING candidates c
WHERE h.event_id = c.event_id
  AND h.hashtag = c.hashtag;

-- name: PurgeStaleEventRelays :execrows
WITH candidates AS (
    SELECT er.event_id, er.relay_url
    FROM event_relays er
    WHERE er.seen_at < @seen_before
      AND EXISTS (
        SELECT 1
        FROM event_relays first
        WHERE first.event_id = er.event_id
          AND (
            first.seen_at < er.seen_at
            OR (first.seen_at = er.seen_at AND first.relay_url < er.relay_url)
          )
      )
    LIMIT @row_limit
)
DELETE FROM event_relays er
USING candidates c
WHERE er.event_id = c.event_id
  AND er.relay_url = c.relay_url;

-- name: PruneAuthorRecentEventsByAge :execrows
WITH candidates AS (
    SELECT author_pubkey, event_id
    FROM author_recent_events
    WHERE created_at < @created_before_unix::bigint
    LIMIT @delete_batch_limit
)
DELETE FROM author_recent_events a
USING candidates c
WHERE a.author_pubkey = c.author_pubkey
  AND a.event_id = c.event_id;

-- name: PruneAuthorRecentEventsByCap :execrows
WITH offenders AS (
    SELECT author_pubkey
    FROM author_recent_events
    GROUP BY author_pubkey
    HAVING count(*) > @per_author_cap::bigint
    LIMIT @author_batch_limit
),
victims AS (
    SELECT r.author_pubkey, r.event_id
    FROM offenders o
    CROSS JOIN LATERAL (
        SELECT a.author_pubkey, a.event_id
        FROM author_recent_events a
        WHERE a.author_pubkey = o.author_pubkey
        ORDER BY a.created_at DESC, a.event_id DESC
        OFFSET @per_author_cap::bigint
    ) r
    LIMIT @delete_batch_limit
)
DELETE FROM author_recent_events a
USING victims v
WHERE a.author_pubkey = v.author_pubkey
  AND a.event_id = v.event_id;

-- name: GroomSearchDocumentsTrim :execrows
WITH candidates AS (
    SELECT cand.entity_type, cand.entity_id
    FROM search_documents cand
    WHERE cand.entity_type = 'note'
      AND cand.freshness < @freshness_before
      AND length(cand.body) > @max_body_chars
    LIMIT @row_limit
)
UPDATE search_documents sd
SET body = left(sd.body, @max_body_chars),
    updated_at = now()
FROM candidates c
WHERE sd.entity_type = c.entity_type
  AND sd.entity_id = c.entity_id;

-- name: GroomSearchDocumentsPrune :execrows
WITH candidates AS (
    SELECT entity_type, entity_id
    FROM search_documents sd
    WHERE sd.entity_type = 'note'
      AND NOT EXISTS (
        SELECT 1 FROM events e WHERE e.id = sd.entity_id
      )
    LIMIT @row_limit
)
DELETE FROM search_documents sd
USING candidates c
WHERE sd.entity_type = c.entity_type
  AND sd.entity_id = c.entity_id;

-- name: PurgeStaleTrustedNoteDiscoveryCandidates :execrows
WITH candidates AS (
    SELECT cand.event_id
    FROM trusted_note_discovery_candidates cand
    WHERE (cand.min_hops IS NOT NULL AND cand.projected_at < @trusted_before)
       OR (cand.min_hops IS NULL AND cand.projected_at < @untrusted_before)
    LIMIT @row_limit
)
DELETE FROM trusted_note_discovery_candidates t
USING candidates c
WHERE t.event_id = c.event_id;

-- name: PurgeStaleTrustedProfileDiscoveryCandidates :execrows
WITH candidates AS (
    SELECT cand.pubkey
    FROM trusted_profile_discovery_candidates cand
    WHERE (cand.min_hops IS NOT NULL AND cand.projected_at < @trusted_before)
       OR (cand.min_hops IS NULL AND cand.projected_at < @untrusted_before)
    LIMIT @row_limit
)
DELETE FROM trusted_profile_discovery_candidates t
USING candidates c
WHERE t.pubkey = c.pubkey;

-- name: PruneOrphanedAppliedStatDeltas :execrows
-- Deletes applied_stat_deltas ledger rows whose source event no longer
-- exists in events. This is the only condition under which a ledger row is
-- guaranteed to serve no further purpose: as long as the event exists,
-- projection_incremental_stats.go's unclaimStatDeltaTx path may still need
-- the row to gate a future decrement (see reverseAndDeleteTx in retention.go,
-- which atomically reverses deltas and deletes the event together, itself
-- deleting the ledger rows it unclaims). Once the event row is gone — via
-- that reversal-aware path, or via a purge that never touched incremental
-- stats (PurgeSupersededReplaceableEvents, PurgeProcessedDeletionEvents) —
-- any ledger row still referencing that event_id is a pure orphan.
--
-- applied_before is a conservative grace buffer on top of the orphan check
-- (not itself a correctness requirement) that keeps freshly-inserted rows
-- out of the scan and bounds it via idx_applied_stat_deltas_applied_at.
WITH candidates AS (
    SELECT d.event_id, d.projection
    FROM applied_stat_deltas d
    WHERE d.applied_at < @applied_before
      AND NOT EXISTS (
        SELECT 1 FROM events e WHERE e.id = d.event_id
      )
    LIMIT @row_limit
)
DELETE FROM applied_stat_deltas d
USING candidates c
WHERE d.event_id = c.event_id
  AND d.projection = c.projection;

-- name: PurgeIdleAccountStates :execrows
WITH candidates AS (
    SELECT a.pubkey
    FROM account_states a
    WHERE a.state IN ('unknown', 'observed')
      AND a.manual_override IS NULL
      AND a.last_observed_at < CASE
        WHEN EXISTS (
            SELECT 1 FROM trust_graph_snapshot s WHERE s.pubkey = a.pubkey
        ) THEN @trusted_before::timestamptz
        ELSE @untrusted_before::timestamptz
      END
    LIMIT @row_limit
)
DELETE FROM account_states a
USING candidates c
WHERE a.pubkey = c.pubkey;

-- name: PruneFilteredEventTags :execrows
-- Drop event_tags rows that the ingest allowlist would no longer write.
-- Matches internal/eventtags.ShouldPersist:
--   * tag_name outside @allowed_tag_names
--   * kind-3 contact-list p-tags
--   * kind-10002 relay-list r-tags
-- events.raw_json remains the source of truth; this only shrinks the
-- derived join index. Batched via LIMIT so the worker catchup loop can
-- drain hundreds of millions of rows without a single long transaction.
WITH candidates AS (
    SELECT et.event_id, et.tag_index, et.value_index
    FROM event_tags et
    INNER JOIN events e ON e.id = et.event_id
    WHERE (et.tag_name = 'p' AND e.kind = 3)
       OR (et.tag_name = 'r' AND e.kind = 10002)
       OR (et.tag_name <> ALL (@allowed_tag_names::text[]))
    LIMIT @row_limit
)
DELETE FROM event_tags et
USING candidates c
WHERE et.event_id = c.event_id
  AND et.tag_index = c.tag_index
  AND et.value_index = c.value_index;
