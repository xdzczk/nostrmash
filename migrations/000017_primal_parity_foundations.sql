ALTER TABLE events
    ADD COLUMN IF NOT EXISTS content_tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', COALESCE(content, ''))) STORED;

ALTER TABLE profiles_latest
    ADD COLUMN IF NOT EXISTS search_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'simple',
            COALESCE(pubkey, '') || ' ' ||
            COALESCE(name, '') || ' ' ||
            COALESCE(display_name, '') || ' ' ||
            COALESCE(about, '') || ' ' ||
            COALESCE(nip05, '')
        )
    ) STORED;

CREATE INDEX IF NOT EXISTS idx_events_content_tsv
    ON events USING gin (content_tsv)
    WHERE kind = 1;

CREATE INDEX IF NOT EXISTS idx_profiles_latest_search_tsv
    ON profiles_latest USING gin (search_tsv);

CREATE INDEX IF NOT EXISTS idx_event_tags_p_lookup
    ON event_tags (value, event_id, tag_index)
    WHERE tag_name = 'p' AND value_index = 0;

CREATE INDEX IF NOT EXISTS idx_event_tags_e_lookup
    ON event_tags (value, event_id, tag_index)
    WHERE tag_name = 'e' AND value_index = 0;

CREATE INDEX IF NOT EXISTS idx_events_kind_created
    ON events (kind, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS dm_read_cursors (
    user_pubkey TEXT NOT NULL,
    peer_pubkey TEXT NOT NULL,
    last_read_created_at BIGINT NOT NULL DEFAULT 0,
    last_read_event_id TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_pubkey, peer_pubkey)
);

CREATE TABLE IF NOT EXISTS curated_reads_topics (
    topic TEXT PRIMARY KEY,
    rank INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS curated_featured_authors (
    pubkey TEXT PRIMARY KEY,
    rank INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS curated_recommended_reads (
    event_id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    rank INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS curated_creator_paid_tiers (
    pubkey TEXT NOT NULL,
    tier_id TEXT NOT NULL,
    title TEXT NOT NULL,
    price_sats BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (pubkey, tier_id)
);
