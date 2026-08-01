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
    WHERE e.kind IN (1, 4, 9802, 10000, 10003, 30023)
      AND e.created_at < @created_before_unix::bigint
      AND e.first_seen_at < @first_seen_before
      AND EXISTS (SELECT 1 FROM trust_graph_snapshot)
      AND NOT EXISTS (
        SELECT 1 FROM trust_graph_snapshot s
        WHERE s.pubkey = e.pubkey
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
