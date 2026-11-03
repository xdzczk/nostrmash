-- Superseded replaceable events used to be found by scanning events of
-- those kinds and deriving d_tag per row (~8 minutes and ~50GB of reads
-- per batch). The deletable set is now stamped when a winner is recorded.
-- The purge reads this queue, so an empty backlog is one index probe.
--
-- The backfill below is the one-time catch-up for winners that already
-- exist. New winners go through enqueue_superseded_replaceable.

SET LOCAL statement_timeout = '20min';

-- No foreign key. The backfill inserts ids it just read from events, and a
-- concurrent delete commits before the row is written. A foreign key check
-- then fails and rolls back the whole migration. The purge deletes queue
-- rows itself, including ones whose event is already gone.
CREATE TABLE IF NOT EXISTS replaceable_event_purge (
    event_id TEXT PRIMARY KEY,
    created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_replaceable_event_purge_created
    ON replaceable_event_purge (created_at, event_id);

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
    IF p_kind NOT IN (0, 3, 10000, 10001, 10002, 10003, 30000, 30023, 30311, 34235, 34236) THEN
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

CREATE OR REPLACE FUNCTION replaceable_state_enqueue_purge() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM enqueue_superseded_replaceable(NEW.pubkey, NEW.kind, NEW.d_tag);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS replaceable_state_enqueue_purge ON replaceable_state;
CREATE TRIGGER replaceable_state_enqueue_purge
    AFTER INSERT OR UPDATE OF event_id, created_at, d_tag
    ON replaceable_state
    FOR EACH ROW
    EXECUTE FUNCTION replaceable_state_enqueue_purge();

-- Non-parameterized kinds (d_tag '') do not need an event_tags lookup.
INSERT INTO replaceable_event_purge (event_id, created_at)
SELECT e.id, e.created_at
FROM replaceable_state rs
JOIN events e
  ON e.pubkey = rs.pubkey
 AND e.kind = rs.kind
 AND e.id <> rs.event_id
 AND (
     e.created_at < rs.created_at
     OR (e.created_at = rs.created_at AND e.id < rs.event_id)
 )
WHERE rs.kind IN (0, 3, 10000, 10001, 10002, 10003)
  AND rs.d_tag = ''
ON CONFLICT (event_id) DO NOTHING;

-- Parameterized kinds match the winner's d tag.
INSERT INTO replaceable_event_purge (event_id, created_at)
SELECT e.id, e.created_at
FROM replaceable_state rs
JOIN events e
  ON e.pubkey = rs.pubkey
 AND e.kind = rs.kind
 AND e.id <> rs.event_id
 AND (
     e.created_at < rs.created_at
     OR (e.created_at = rs.created_at AND e.id < rs.event_id)
 )
WHERE rs.kind IN (30000, 30023, 30311, 34235, 34236)
  AND COALESCE((
      SELECT et.value
      FROM event_tags et
      WHERE et.event_id = e.id
        AND et.tag_name = 'd'
        AND et.value_index = 0
      ORDER BY et.tag_index ASC
      LIMIT 1
  ), '') = rs.d_tag
ON CONFLICT (event_id) DO NOTHING;

-- Drop ids that were deleted after the inserts above read them.
DELETE FROM replaceable_event_purge p
WHERE NOT EXISTS (
    SELECT 1 FROM events e WHERE e.id = p.event_id
);
