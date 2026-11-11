-- Viewer-scoped discovery aggregates engagement and authorship by the
-- accounts one profile follows. event_urls already has an author/time
-- index. event_hashtags and follower_gain_events did not, so a cohort
-- scan would walk the whole window.
--
-- The database enforces a short global statement_timeout; raise it so
-- the index builds are not killed mid-migration.
SET LOCAL statement_timeout = '10min';

CREATE INDEX IF NOT EXISTS idx_event_hashtags_author_created
    ON event_hashtags (author_pubkey, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_follower_gain_events_follower_gained
    ON follower_gain_events (follower_pubkey, gained_at DESC);
