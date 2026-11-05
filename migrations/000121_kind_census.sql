CREATE TABLE IF NOT EXISTS kind_census_events (
    event_id TEXT PRIMARY KEY,
    kind INTEGER NOT NULL,
    pubkey TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    seen_day DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kind_census_events_seen_day
    ON kind_census_events (seen_day);

CREATE TABLE IF NOT EXISTS kind_activity_daily (
    day DATE NOT NULL,
    kind INTEGER NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    unique_authors BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, kind)
);

CREATE TABLE IF NOT EXISTS kind_author_daily (
    day DATE NOT NULL,
    kind INTEGER NOT NULL,
    pubkey TEXT NOT NULL,
    PRIMARY KEY (day, kind, pubkey)
);
