ALTER TABLE event_urls
    ADD COLUMN IF NOT EXISTS canonical_domain TEXT;

UPDATE event_urls
SET canonical_domain = CASE
    WHEN regexp_replace(lower(domain), '^www\.', '') = 'youtu.be' THEN 'youtube.com'
    ELSE regexp_replace(lower(domain), '^www\.', '')
END
WHERE canonical_domain IS NULL;

ALTER TABLE event_urls
    ALTER COLUMN canonical_domain SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_event_urls_canonical_domain_created_at
    ON event_urls (canonical_domain, created_at DESC);

COMMENT ON COLUMN event_urls.domain IS
    'Normalized hostname observed in the event URL, retained for event-level provenance.';

COMMENT ON COLUMN event_urls.canonical_domain IS
    'Backend-owned discovery identity used for domain aggregation and lookup.';
