CREATE TABLE IF NOT EXISTS relay_probe_observations (
    id                        BIGSERIAL   PRIMARY KEY,
    url_key                   TEXT        NOT NULL REFERENCES relay_registry(url_key) ON DELETE CASCADE,
    probed_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    connect_ok                BOOLEAN     NOT NULL DEFAULT FALSE,
    subscribe_ok              BOOLEAN     NOT NULL DEFAULT FALSE,
    eose_ok                   BOOLEAN     NOT NULL DEFAULT FALSE,
    connect_latency_ms        DOUBLE PRECISION,
    eose_latency_ms           DOUBLE PRECISION,
    error_code                TEXT,
    error_text_short          TEXT,
    sample_yield_count        INTEGER,
    sample_duplicate_ratio    DOUBLE PRECISION,
    capability_snapshot_json  JSONB
);

CREATE INDEX IF NOT EXISTS relay_probe_observations_url_key_probed_at_idx
    ON relay_probe_observations (url_key, probed_at DESC);
