-- A probe that writes REQ successfully and then receives CLOSED is not
-- subscribe_failed. The booleans already recorded that distinction;
-- last_probe_status needs a value that matches them.
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT con.conname INTO constraint_name
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE nsp.nspname = current_schema()
      AND rel.relname = 'relay_registry'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) ILIKE '%last_probe_status%';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE relay_registry DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;

ALTER TABLE relay_registry
    ADD CONSTRAINT relay_registry_last_probe_status_check
    CHECK (last_probe_status IS NULL OR last_probe_status IN (
        'ok', 'connect_failed', 'subscribe_failed', 'subscription_closed',
        'eose_timeout', 'protocol_error', 'rate_limited', 'unknown_error'
    ));
