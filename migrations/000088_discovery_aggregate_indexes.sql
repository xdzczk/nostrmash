-- Covering indexes for the per-entity discovery summaries.
--
-- GET /api/v1/discovery/domains/{domain} and
-- GET /api/v1/discovery/hashtags/{hashtag} aggregate one key out of
-- event_urls / event_hashtags. The existing indexes find the row ids, then
-- a bitmap heap scan fetches every matching tuple. On production a single
-- popular key is scattered across tens of thousands of heap pages
-- (github.com: 53k urls, 2.3s; #bitcoin: 131k rows, 4.5s), and the request
-- path does that aggregate plus the note lists, so a cold cache miss ran
-- 10-18s.
--
-- event_hashtags (hashtag, created_at) INCLUDE (event_id, author_pubkey)
-- answers the summary with an index-only scan.
--
-- event_urls cannot be index-only while the media-extension regex has to
-- read `url`. is_media stores that classification at projection time (same
-- extension list as the old regex) and the partial index below covers the
-- domain aggregate. DEFAULT false matches the pre-column behavior for rows
-- this migration has not classified yet; the UPDATE marks the media rows.
--
-- The database enforces a short global statement_timeout. The backfill and
-- index builds need longer. On a database where operators already applied
-- this out of band, the column/index guards make the statements no-ops and
-- the UPDATE only classifies rows still at the default.
SET LOCAL statement_timeout = '20min';

ALTER TABLE event_urls
    ADD COLUMN IF NOT EXISTS is_media BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE event_urls
SET is_media = TRUE
WHERE is_media = FALSE
  AND url ~* '\.(png|jpe?g|gif|webp|svg|bmp|ico|tiff?|avif|heic|mp4|mov|webm|m4v|avi|mkv|wmv|flv|mp3|wav|ogg|m4a|flac|aac|opus)(\?|#|$)';

CREATE INDEX IF NOT EXISTS idx_event_hashtags_hashtag_stats
    ON event_hashtags (hashtag, created_at)
    INCLUDE (event_id, author_pubkey);

CREATE INDEX IF NOT EXISTS idx_event_urls_domain_activity
    ON event_urls (canonical_domain, created_at)
    INCLUDE (event_id, author_pubkey)
    WHERE NOT is_media;
