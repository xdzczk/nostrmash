CREATE TABLE IF NOT EXISTS event_urls (
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    author_pubkey TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    url TEXT NOT NULL,
    domain TEXT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, url)
);

CREATE INDEX IF NOT EXISTS idx_event_urls_domain_created_at
    ON event_urls (domain, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_event_urls_author_created_at
    ON event_urls (author_pubkey, created_at DESC);
