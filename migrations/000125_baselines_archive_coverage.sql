-- Baselines, the daily archive, and the coverage log.
--
-- entity_baseline and domain_momentum are rebuilt at most once per UTC day
-- from hashtag_author_daily / domain_author_daily (migration 000124). That
-- work stays off the request path. discovery_archive is one overview JSON
-- per UTC day. relay_coverage_changes is one row when a relay enters or
-- leaves the indexed set (active or pinned).

CREATE TABLE entity_baseline (
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    metric TEXT NOT NULL,
    typical DOUBLE PRECISION NOT NULL,
    sample_days INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_key, metric)
);

CREATE TABLE entity_baseline_state (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    rebuilt_on DATE
);

INSERT INTO entity_baseline_state (id) VALUES (TRUE);

CREATE TABLE domain_momentum (
    domain TEXT PRIMARY KEY,
    z_score_7d DOUBLE PRECISION NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT '',
    half_life_days DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE discovery_archive (
    day DATE NOT NULL,
    language TEXT NOT NULL DEFAULT '',
    window_label TEXT NOT NULL,
    payload JSONB NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (day, language, window_label)
);

CREATE TABLE relay_coverage_changes (
    id BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    url_key TEXT NOT NULL,
    host TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    change_kind TEXT NOT NULL
);

CREATE INDEX idx_relay_coverage_changes_occurred
    ON relay_coverage_changes (occurred_at DESC);
