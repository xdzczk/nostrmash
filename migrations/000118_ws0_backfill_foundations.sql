-- Keep the superseded-replaceable purge aligned with the Go kind list.
-- 10019 and 10050 were author-gated replaceables in Go and missing from
-- enqueue_superseded_replaceable, so their older versions were retained.
-- Also persist the ends tag. Live activities already filter on it, and
-- the tag was dropped at ingest.

SET LOCAL statement_timeout = 0;

DROP INDEX IF EXISTS idx_event_tags_disallowed_tag_name;
CREATE INDEX IF NOT EXISTS idx_event_tags_disallowed_tag_name
    ON event_tags (tag_name)
    WHERE tag_name NOT IN (
        'a', 'd', 'e', 'ends', 'g', 'group', 'image', 'imeta', 'm', 'p', 'r',
        'series', 'starts', 'status', 'summary', 't', 'thumb', 'title', 'u', 'url', 'video', 'word'
    );

CREATE OR REPLACE FUNCTION enqueue_superseded_replaceable(
    p_pubkey TEXT,
    p_kind INTEGER,
    p_d_tag TEXT
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    winner_id TEXT;
    winner_created BIGINT;
BEGIN
    IF p_kind NOT IN (0, 3, 10000, 10001, 10002, 10003, 10019, 10050, 30000, 30023, 30311, 34235, 34236) THEN
        RETURN;
    END IF;

    SELECT event_id, created_at
      INTO winner_id, winner_created
      FROM replaceable_state
     WHERE pubkey = p_pubkey
       AND kind = p_kind
       AND d_tag = p_d_tag;
    IF winner_id IS NULL THEN
        RETURN;
    END IF;

    DELETE FROM replaceable_event_purge WHERE event_id = winner_id;

    INSERT INTO replaceable_event_purge (event_id, created_at)
    SELECT e.id, e.created_at
    FROM events e
    WHERE e.pubkey = p_pubkey
      AND e.kind = p_kind
      AND e.id <> winner_id
      AND (
          e.created_at < winner_created
          OR (e.created_at = winner_created AND e.id < winner_id)
      )
      AND (
          p_d_tag = ''
          OR COALESCE((
              SELECT et.value
              FROM event_tags et
              WHERE et.event_id = e.id
                AND et.tag_name = 'd'
                AND et.value_index = 0
              ORDER BY et.tag_index ASC
              LIMIT 1
          ), '') = p_d_tag
      )
    ON CONFLICT (event_id) DO NOTHING;
END;
$$;
