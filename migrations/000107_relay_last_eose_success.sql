-- Catalog membership follows the newest successful read, not the newest
-- attempt. A later failure keeps the previous success time so the relay
-- stays listed until that success ages out of the freshness window.
ALTER TABLE relay_registry
    ADD COLUMN IF NOT EXISTS last_eose_success_at TIMESTAMPTZ;

UPDATE relay_registry AS registry
SET last_eose_success_at = successes.last_ok
FROM (
    SELECT url_key, max(probed_at) AS last_ok
    FROM relay_probe_observations
    WHERE eose_ok
    GROUP BY url_key
) AS successes
WHERE registry.url_key = successes.url_key
  AND (
    registry.last_eose_success_at IS NULL
    OR registry.last_eose_success_at < successes.last_ok
  );

UPDATE relay_registry
SET last_eose_success_at = last_probe_at
WHERE last_eose_ok IS TRUE
  AND last_probe_status = 'ok'
  AND last_probe_at IS NOT NULL
  AND (
    last_eose_success_at IS NULL
    OR last_eose_success_at < last_probe_at
  );
