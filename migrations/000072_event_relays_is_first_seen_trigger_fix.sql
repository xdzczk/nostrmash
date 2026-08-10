-- Fix event_relays is_first_seen triggers vs ON CONFLICT DO UPDATE.
--
-- Migration 000070 broke InsertCanonicalEvent idempotent re-ingest of the
-- same (event_id, relay_url) when seen_at moved earlier. Postgres raised
-- SQLSTATE 21000:
--   "ON CONFLICT DO UPDATE command cannot affect row a second time"
--
-- Two interacting bugs:
--
-- 1) BEFORE INSERT fired before conflict resolution and ran
--    UPDATE event_relays ... WHERE event_id = NEW.event_id, which rewrote
--    the existing conflicting row. ON CONFLICT DO UPDATE then tried to
--    update that same row again.
--
-- 2) AFTER UPDATE recomputed flags with
--    UPDATE event_relays ... WHERE event_id = NEW.event_id, which could
--    rewrite NEW's own row during the ON CONFLICT UPDATE.
--
-- Fix: never touch the row identified by NEW.(event_id, relay_url) from
-- either trigger's peer UPDATE; stamp NEW.is_first_seen on BEFORE
-- INSERT/UPDATE instead. Verified by
-- TestInsertCanonicalEventIdempotentOnIDAndPreservesEarliestFirstSeen.

CREATE OR REPLACE FUNCTION event_relays_before_insert_is_first_seen()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Decide from true seen_at ordering among *other* relays for this
    -- event. Exclude NEW.relay_url so an ON CONFLICT re-ingest of the
    -- same pair does not UPDATE the conflicting row before DO UPDATE runs.
    IF NOT EXISTS (
        SELECT 1
        FROM event_relays
        WHERE event_id = NEW.event_id
          AND relay_url IS DISTINCT FROM NEW.relay_url
          AND (
            seen_at < NEW.seen_at
            OR (seen_at = NEW.seen_at AND relay_url < NEW.relay_url)
          )
    ) THEN
        NEW.is_first_seen := true;
        UPDATE event_relays
        SET is_first_seen = false
        WHERE event_id = NEW.event_id
          AND relay_url IS DISTINCT FROM NEW.relay_url
          AND is_first_seen;
    ELSE
        NEW.is_first_seen := false;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION event_relays_before_update_is_first_seen()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM event_relays
        WHERE event_id = NEW.event_id
          AND relay_url IS DISTINCT FROM NEW.relay_url
          AND (
            seen_at < NEW.seen_at
            OR (seen_at = NEW.seen_at AND relay_url < NEW.relay_url)
          )
    ) THEN
        NEW.is_first_seen := true;
    ELSE
        NEW.is_first_seen := false;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION event_relays_after_update_demote_other_first_seen()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- LEAST(seen_at) can only move a row earlier, so the only peer update
    -- needed is demoting other first-seen rows when NEW became first.
    IF NEW.is_first_seen THEN
        UPDATE event_relays
        SET is_first_seen = false
        WHERE event_id = NEW.event_id
          AND relay_url IS DISTINCT FROM NEW.relay_url
          AND is_first_seen;
    END IF;
    RETURN NULL;
END;
$$;

-- Keep the old function name as a safe alias until triggers are rewired
-- below. Do NOT raise here: CREATE FUNCTION commits before DROP TRIGGER,
-- and DROP TRIGGER can wait on a long UPDATE (ops backfill) — a raising
-- body would break ON CONFLICT seen_at updates in that window.
CREATE OR REPLACE FUNCTION event_relays_after_update_recompute_is_first_seen()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.is_first_seen THEN
        UPDATE event_relays
        SET is_first_seen = false
        WHERE event_id = NEW.event_id
          AND relay_url IS DISTINCT FROM NEW.relay_url
          AND is_first_seen;
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_event_relays_before_insert_is_first_seen ON event_relays;
CREATE TRIGGER trg_event_relays_before_insert_is_first_seen
    BEFORE INSERT ON event_relays
    FOR EACH ROW
    EXECUTE FUNCTION event_relays_before_insert_is_first_seen();

DROP TRIGGER IF EXISTS trg_event_relays_before_update_is_first_seen ON event_relays;
CREATE TRIGGER trg_event_relays_before_update_is_first_seen
    BEFORE UPDATE OF seen_at ON event_relays
    FOR EACH ROW
    WHEN (NEW.seen_at IS DISTINCT FROM OLD.seen_at)
    EXECUTE FUNCTION event_relays_before_update_is_first_seen();

DROP TRIGGER IF EXISTS trg_event_relays_after_update_is_first_seen ON event_relays;
CREATE TRIGGER trg_event_relays_after_update_is_first_seen
    AFTER UPDATE OF seen_at ON event_relays
    FOR EACH ROW
    WHEN (NEW.seen_at IS DISTINCT FROM OLD.seen_at)
    EXECUTE FUNCTION event_relays_after_update_demote_other_first_seen();

-- Now that no trigger points here, retire the old name loudly.
CREATE OR REPLACE FUNCTION event_relays_after_update_recompute_is_first_seen()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'event_relays_after_update_recompute_is_first_seen is retired; use event_relays_after_update_demote_other_first_seen';
END;
$$;