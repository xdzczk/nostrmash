-- Kind-1 quote posts (NIP-18 q tags) are counted separately from kind-6
-- reposts. q tags are not copied into event_tags, so the backfill reads
-- events.raw_json. Address zaps (articles, live streams) name a NIP-33
-- coordinate instead of an e-tag; store that coordinate and fold it into
-- the current replaceable's zap total.
--
-- Production statement_timeout is 15s. The kind-1 scan does not finish in
-- that window. This setting lasts only for the migration transaction.
SET LOCAL statement_timeout = 0;

CREATE TABLE IF NOT EXISTS quote_contributions (
    source_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    target_event_id TEXT NOT NULL DEFAULT '',
    target_address TEXT NOT NULL DEFAULT '',
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_event_id, target_event_id, target_address),
    CHECK (target_event_id <> '' OR target_address <> '')
);

CREATE INDEX IF NOT EXISTS idx_quote_contributions_target_event
    ON quote_contributions (target_event_id)
    WHERE target_event_id <> '';

CREATE INDEX IF NOT EXISTS idx_quote_contributions_target_address
    ON quote_contributions (target_address)
    WHERE target_address <> '';

-- DISTINCT: one note can repeat the same q tag. A single INSERT .. ON
-- CONFLICT cannot update that row twice.
INSERT INTO quote_contributions (
    source_event_id, target_event_id, target_address, derivation_version
)
SELECT DISTINCT parsed.source_event_id, parsed.target_event_id, parsed.target_address, 1
FROM (
    SELECT
        e.id AS source_event_id,
        CASE
            WHEN q.val ~ '^[0-9a-fA-F]{64}$' THEN lower(q.val)
            ELSE ''
        END AS target_event_id,
        CASE
            WHEN q.val ~ '^[0-9]+:[0-9a-fA-F]{64}:'
                THEN split_part(q.val, ':', 1) || ':' || lower(split_part(q.val, ':', 2)) || ':' ||
                    substr(q.val, char_length(split_part(q.val, ':', 1)) + char_length(split_part(q.val, ':', 2)) + 3)
            ELSE ''
        END AS target_address
    FROM events e
    CROSS JOIN LATERAL (
        SELECT tag->>1 AS val
        FROM jsonb_array_elements(
            CASE
                WHEN jsonb_typeof(e.raw_json->'tags') = 'array' THEN e.raw_json->'tags'
                ELSE '[]'::jsonb
            END
        ) AS tag
        WHERE jsonb_typeof(tag) = 'array'
          AND jsonb_array_length(tag) >= 2
          AND tag->>0 = 'q'
    ) q
    WHERE e.kind = 1
) parsed
WHERE (parsed.target_event_id <> '' OR parsed.target_address <> '')
  AND parsed.target_event_id IS DISTINCT FROM lower(parsed.source_event_id)
ON CONFLICT (source_event_id, target_event_id, target_address) DO UPDATE
SET derivation_version = EXCLUDED.derivation_version,
    projected_at = now();

ALTER TABLE zap_receipts
    ADD COLUMN IF NOT EXISTS target_address TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_zap_receipts_target_address
    ON zap_receipts (target_address)
    WHERE target_address <> '';

UPDATE zap_receipts AS z
SET target_address = split_part(et.value, ':', 1) || ':' || lower(split_part(et.value, ':', 2)) || ':' ||
    substr(et.value, char_length(split_part(et.value, ':', 1)) + char_length(split_part(et.value, ':', 2)) + 3)
FROM event_tags et
WHERE et.event_id = z.zap_receipt_id
  AND et.tag_name = 'a'
  AND et.value_index = 0
  AND et.value ~ '^[0-9]+:[0-9a-fA-F]{64}:'
  AND et.tag_index = (
      SELECT MIN(first.tag_index)
      FROM event_tags first
      WHERE first.event_id = z.zap_receipt_id
        AND first.tag_name = 'a'
        AND first.value_index = 0
  )
  AND z.target_address IS DISTINCT FROM (
      split_part(et.value, ':', 1) || ':' || lower(split_part(et.value, ':', 2)) || ':' ||
      substr(et.value, char_length(split_part(et.value, ':', 1)) + char_length(split_part(et.value, ':', 2)) + 3)
  );

-- Receipts stored before the invoice parser ran still have amount_sats = 0
-- even when bolt11 carries the amount. Recover that amount, then recompute
-- the note totals those receipts belong to.
UPDATE zap_receipts AS z
SET amount_sats = parsed.sats
FROM (
    SELECT
        e.id AS zap_receipt_id,
        FLOOR(match[1]::numeric * CASE match[2]
            WHEN 'm' THEN 100000
            WHEN 'u' THEN 100
            WHEN 'n' THEN 0.1
            WHEN 'p' THEN 0.0001
            ELSE 100000000
        END)::bigint AS sats
    FROM events e
    CROSS JOIN LATERAL (
        SELECT tag->>1 AS bolt11
        FROM jsonb_array_elements(
            CASE
                WHEN jsonb_typeof(e.raw_json->'tags') = 'array' THEN e.raw_json->'tags'
                ELSE '[]'::jsonb
            END
        ) AS tag
        WHERE tag->>0 = 'bolt11'
          AND jsonb_typeof(tag) = 'array'
        LIMIT 1
    ) bolt
    CROSS JOIN LATERAL (
        SELECT regexp_match(
            lower(split_part(bolt.bolt11, '1', 1)),
            '^ln(?:bc|tb|bcrt)([0-9]*\.?[0-9]+)([munp]?)$'
        ) AS match
    ) hrp
    WHERE e.kind = 9735
      AND hrp.match IS NOT NULL
) AS parsed
WHERE z.zap_receipt_id = parsed.zap_receipt_id
  AND z.amount_sats = 0
  AND parsed.sats > 0;

UPDATE note_discovery_stats AS stats
SET zap_count = computed.zap_count,
    zap_msats = computed.zap_msats
FROM (
    SELECT
        z.event_id,
        COUNT(*)::bigint AS zap_count,
        COALESCE(SUM(z.amount_sats), 0)::bigint * 1000 AS zap_msats
    FROM zap_receipts z
    WHERE z.event_id IS NOT NULL
      AND z.event_id <> ''
    GROUP BY z.event_id
) AS computed
WHERE stats.event_id = computed.event_id
  AND (
      stats.zap_count IS DISTINCT FROM computed.zap_count
      OR stats.zap_msats IS DISTINCT FROM computed.zap_msats
  );

UPDATE note_discovery_stats AS stats
SET zap_count = computed.zap_count,
    zap_msats = computed.zap_msats
FROM (
    SELECT
        rs.event_id,
        COUNT(*)::bigint AS zap_count,
        COALESCE(SUM(z.amount_sats), 0)::bigint * 1000 AS zap_msats
    FROM replaceable_state rs
    JOIN zap_receipts z ON (
        z.event_id = rs.event_id
        OR (
            z.target_address <> ''
            AND z.target_address = (rs.kind::text || ':' || lower(rs.pubkey) || ':' || rs.d_tag)
            AND z.event_id IS DISTINCT FROM rs.event_id
        )
    )
    WHERE EXISTS (
        SELECT 1
        FROM zap_receipts extra
        WHERE extra.target_address <> ''
          AND extra.target_address = (rs.kind::text || ':' || lower(rs.pubkey) || ':' || rs.d_tag)
          AND extra.event_id IS DISTINCT FROM rs.event_id
    )
    GROUP BY rs.event_id
) AS computed
WHERE stats.event_id = computed.event_id
  AND (
      stats.zap_count IS DISTINCT FROM computed.zap_count
      OR stats.zap_msats IS DISTINCT FROM computed.zap_msats
  );
