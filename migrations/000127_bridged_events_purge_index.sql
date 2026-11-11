-- Bridged event purge.
--
-- The ingestor now drops every event that carries a NIP-48 proxy tag, so no
-- new bridged rows arrive. This partial index is the candidate set for the
-- one-time purge of the bridged rows already stored: the worker walks it in
-- batches, and once it is empty each tick is an index probe. The index stays
-- empty afterwards, so it costs nothing in steady state.

CREATE INDEX IF NOT EXISTS idx_event_attributes_bridged
    ON event_attributes (event_id)
    WHERE proxy_protocol <> '' OR proxy_id <> '';
