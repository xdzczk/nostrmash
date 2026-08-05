-- Unblock event retention cascades.
--
-- Several tables reference events(id) ON DELETE CASCADE without a leading
-- index on the FK column. Postgres then seq-scans the child heap for every
-- deleted event. On production that made a single kind-5 DELETE of 100
-- events take ~100s (and a 2000-row retention batch exceed the 15-minute
-- retention statement_timeout), so every deletion/engagement/replaceable/
-- untrusted-author retention loop failed every hour and never drained.
--
-- Confirmed missing leading indexes (2026-08-05):
--   author_recent_events.event_id          (~5 GB heap, the observed blocker)
--   follower_edges.source_event_id         (~2 GB heap; kind-3 replaceable)
--   profiles_latest.metadata_event_id
--   relay_lists_latest.event_id
--   contact_lists_latest.event_id
--
-- Production builds these with CREATE INDEX CONCURRENTLY before this
-- migration ships; IF NOT EXISTS keeps redeploys idempotent. The migrate
-- runner wraps each file in a transaction, so CONCURRENTLY cannot live here.
CREATE INDEX IF NOT EXISTS idx_author_recent_events_event_id
    ON author_recent_events (event_id);

CREATE INDEX IF NOT EXISTS idx_follower_edges_source_event_id
    ON follower_edges (source_event_id);

CREATE INDEX IF NOT EXISTS idx_profiles_latest_metadata_event_id
    ON profiles_latest (metadata_event_id);

CREATE INDEX IF NOT EXISTS idx_relay_lists_latest_event_id
    ON relay_lists_latest (event_id);

CREATE INDEX IF NOT EXISTS idx_contact_lists_latest_event_id
    ON contact_lists_latest (event_id);
