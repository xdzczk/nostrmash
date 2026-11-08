-- The follows feed walked innerLimit candidates per followed author and
-- anti-joined each one against thread_edges. author_feed_events is the
-- top-level kind 1/6 page, maintained at insert and by thread projection,
-- then backfilled by the worker. Reads stay on the events scan until
-- author_feed_index_state.complete is set, so a partial backfill cannot
-- drop history from the page.
--
-- hashtag_author_daily and domain_author_daily are one row per author per
-- day. The 30-day homepage snapshots aggregate those rows instead of
-- scanning event_hashtags / event_urls. The worker fills history; until
-- both cursors reach today the snapshots keep the raw queries.

CREATE TABLE author_feed_events (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX idx_author_feed_events_page
    ON author_feed_events (pubkey, kind, created_at DESC, event_id DESC);

CREATE TABLE author_feed_index_state (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    cursor_kind INTEGER NOT NULL DEFAULT 1,
    cursor_created_at BIGINT,
    cursor_event_id TEXT NOT NULL DEFAULT '',
    complete BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO author_feed_index_state (id) VALUES (TRUE);

CREATE TABLE hashtag_author_daily (
    day DATE NOT NULL,
    hashtag TEXT NOT NULL,
    author_pubkey TEXT NOT NULL,
    language TEXT NOT NULL DEFAULT '',
    events INTEGER NOT NULL,
    PRIMARY KEY (day, hashtag, author_pubkey, language)
);

CREATE TABLE domain_author_daily (
    day DATE NOT NULL,
    canonical_domain TEXT NOT NULL,
    author_pubkey TEXT NOT NULL,
    language TEXT NOT NULL DEFAULT '',
    links INTEGER NOT NULL,
    notes INTEGER NOT NULL,
    latest_created_at BIGINT NOT NULL,
    PRIMARY KEY (day, canonical_domain, author_pubkey, language)
);

CREATE TABLE discovery_daily_rollup_state (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    hashtag_next DATE,
    domain_next DATE
);

INSERT INTO discovery_daily_rollup_state (id) VALUES (TRUE);
