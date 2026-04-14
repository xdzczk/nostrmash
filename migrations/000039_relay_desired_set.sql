CREATE TABLE IF NOT EXISTS relay_desired_set (
    id                BIGSERIAL   PRIMARY KEY,
    published_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    relay_urls_json   JSONB       NOT NULL DEFAULT '[]'::jsonb,
    source            TEXT        NOT NULL DEFAULT 'controller'
        CHECK (source IN ('controller', 'manual', 'seed_only')),
    notes             TEXT
);

CREATE INDEX IF NOT EXISTS relay_desired_set_published_at_idx
    ON relay_desired_set (published_at DESC);
