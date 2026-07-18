-- Storage discipline Phase 3: drop the derived pubkey_references table.
--
-- Production snapshot showed 5.2 GB (~10.4 M rows) for a table whose only
-- read owner was GetEventsReferencingPubkey (mentions). That read is now
-- served directly from canonical event_tags via idx_event_tags_p_lookup
-- (tag_name = 'p', value_index = 0), so the materialized copy is pure rent.
-- The table is rebuildable from canonical events, so this drop is safe and
-- reversible by re-adding the projection.
DROP TABLE IF EXISTS pubkey_references;
