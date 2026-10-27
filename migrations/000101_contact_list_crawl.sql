-- Queue for the paced kind-3 contact-list crawl.
--
-- Live ingest only tails events newer than a short lookback, so a follow
-- list whose created_at is older than this server never arrives.
-- contact_list_crawl records pubkeys to fetch by author, with no since.
-- Eligibility is next_eligible_at: due rows are the backlog, and a claim
-- pushes that timestamp forward so a second ingestor does not double-fetch.
-- Hop 0 is seeds and known authors. Higher hops are p-tags walked from
-- lists those fetches return.
CREATE TABLE IF NOT EXISTS contact_list_crawl (
    pubkey TEXT PRIMARY KEY,
    hop INTEGER NOT NULL CHECK (hop >= 0),
    next_eligible_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_fetched_at TIMESTAMPTZ,
    found_list BOOLEAN,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contact_list_crawl_due
    ON contact_list_crawl (next_eligible_at, hop, pubkey);

-- One-row keyset cursor over profile_public_stats (recent_activity_at DESC,
-- pubkey ASC). After a pass reaches the end, next_seed_at waits out the
-- rescan interval so the crawl does not walk the whole stats index every cycle.
CREATE TABLE IF NOT EXISTS contact_list_crawl_cursor (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    recent_activity_at BIGINT,
    pubkey TEXT NOT NULL DEFAULT '',
    in_null_activity BOOLEAN NOT NULL DEFAULT false,
    next_seed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO contact_list_crawl_cursor (id) VALUES (1)
ON CONFLICT DO NOTHING;
