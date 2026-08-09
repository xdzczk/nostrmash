-- Retention query cost hardening.
--
-- Background
-- ----------
-- PurgeStaleEventRelays used a correlated self-EXISTS to decide "is this the
-- earliest-seen row for this event?" on every batch. After the deletable
-- backlog was drained, each tick still re-scanned millions of keeper rows at
-- the head of idx_event_relays_seen_at_pubkey, so batch latency grew with
-- table size instead of backlog size (weeks of catchup, IO storms).
--
-- Fix: stamp is_first_seen at write time and purge with a partial index on
-- non-first stale rows. Cost stays proportional to rows actually deleted.
--
-- Existing rows default to is_first_seen = true (safe: purge skips them).
-- The historical duplicate backlog was already drained by an ops fast-purge;
-- new inserts/updates maintain the flag via triggers. A one-shot backfill of
-- legacy non-first rows is optional and intentionally NOT done here (would
-- hold the migration transaction for tens of minutes on production):
--
--   UPDATE event_relays er
--   SET is_first_seen = (er.relay_url = w.relay_url)
--   FROM (
--     SELECT DISTINCT ON (event_id) event_id, relay_url
--     FROM event_relays
--     ORDER BY event_id, seen_at ASC, relay_url ASC
--   ) w
--   WHERE er.event_id = w.event_id;
--
-- PruneAuthorRecentEventsByCap previously GROUP BY'd the entire
-- author_recent_events table every tick. Bound the offender scan to authors
-- touched recently (projected_at), which is the only population that can
-- newly exceed the per-author cap.

ALTER TABLE event_relays
    ADD COLUMN IF NOT EXISTS is_first_seen BOOLEAN NOT NULL DEFAULT true;

CREATE INDEX IF NOT EXISTS idx_event_relays_purge_nonfirst_seen_at
    ON event_relays (seen_at ASC, event_id ASC, relay_url ASC)
    WHERE NOT is_first_seen;

CREATE OR REPLACE FUNCTION event_relays_before_insert_is_first_seen()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Decide from true seen_at ordering, not from possibly-stale flags on
    -- legacy rows that still carry the DEFAULT true from this migration.
    IF NOT EXISTS (
        SELECT 1 FROM event_relays WHERE event_id = NEW.event_id
    ) THEN
        NEW.is_first_seen := true;
        RETURN NEW;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM event_relays
        WHERE event_id = NEW.event_id
          AND (
            seen_at < NEW.seen_at
            OR (seen_at = NEW.seen_at AND relay_url < NEW.relay_url)
          )
    ) THEN
        NEW.is_first_seen := true;
        UPDATE event_relays
        SET is_first_seen = false
        WHERE event_id = NEW.event_id
          AND is_first_seen;
    ELSE
        NEW.is_first_seen := false;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION event_relays_after_update_recompute_is_first_seen()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    winner_url text;
BEGIN
    -- ON CONFLICT DO UPDATE SET seen_at = LEAST(...) can make a non-first
    -- row become the earliest. Recompute from ordering; only touches
    -- is_first_seen so this trigger does not recurse.
    SELECT relay_url INTO winner_url
    FROM event_relays
    WHERE event_id = NEW.event_id
    ORDER BY seen_at ASC, relay_url ASC
    LIMIT 1;

    UPDATE event_relays
    SET is_first_seen = (relay_url = winner_url)
    WHERE event_id = NEW.event_id
      AND is_first_seen IS DISTINCT FROM (relay_url = winner_url);

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_event_relays_before_insert_is_first_seen ON event_relays;
CREATE TRIGGER trg_event_relays_before_insert_is_first_seen
    BEFORE INSERT ON event_relays
    FOR EACH ROW
    EXECUTE FUNCTION event_relays_before_insert_is_first_seen();

DROP TRIGGER IF EXISTS trg_event_relays_after_update_is_first_seen ON event_relays;
CREATE TRIGGER trg_event_relays_after_update_is_first_seen
    AFTER UPDATE OF seen_at ON event_relays
    FOR EACH ROW
    WHEN (NEW.seen_at IS DISTINCT FROM OLD.seen_at)
    EXECUTE FUNCTION event_relays_after_update_recompute_is_first_seen();

CREATE INDEX IF NOT EXISTS idx_author_recent_events_projected_at
    ON author_recent_events (projected_at DESC, author_pubkey);
