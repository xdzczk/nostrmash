-- enqueue_superseded_replaceable scanned every older event of a pubkey
-- and kind, then looked up each d tag. That work ran inside the
-- replaceable_state upsert trigger, so a busy profile or contact list
-- held the statement until the 15s timeout and the derive job retried.
--
-- The displaced winner is one row, available as OLD. A version that loses
-- without updating the row is queued by the derivation itself. History
-- already in the purge queue stays there.

CREATE OR REPLACE FUNCTION replaceable_state_enqueue_purge() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.event_id IS DISTINCT FROM NEW.event_id THEN
        INSERT INTO replaceable_event_purge (event_id, created_at)
        VALUES (OLD.event_id, OLD.created_at)
        ON CONFLICT (event_id) DO NOTHING;
    END IF;
    DELETE FROM replaceable_event_purge WHERE event_id = NEW.event_id;
    RETURN NEW;
END;
$$;
