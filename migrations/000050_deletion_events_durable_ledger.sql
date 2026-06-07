-- Decouple deletion_events from the raw events lifetime so it can act as a
-- durable tombstone ledger. Previously deletion_events.event_id had
-- ON DELETE CASCADE to events(id), which meant purging a raw kind-5 event also
-- destroyed its deletion record. Dropping the FK lets retention reclaim raw
-- kind-5 events (and their cascade-cleaned tags/references) while the distilled
-- (deleter_pubkey, target_event_id, created_at) ledger row survives.
--
-- event_id stays the PRIMARY KEY (the deletion event's own id); it simply no
-- longer carries a referential constraint to events.
ALTER TABLE deletion_events
    DROP CONSTRAINT IF EXISTS deletion_events_event_id_fkey;
