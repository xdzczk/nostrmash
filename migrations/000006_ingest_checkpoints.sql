CREATE TABLE IF NOT EXISTS ingest_checkpoints (
    relay_url TEXT NOT NULL,
    mode TEXT NOT NULL,
    filter_group TEXT NOT NULL,
    since BIGINT,
    "until" BIGINT,
    cursor TEXT,
    eose_seen_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (relay_url, mode, filter_group)
);

CREATE INDEX IF NOT EXISTS idx_ingest_checkpoints_mode_status
    ON ingest_checkpoints (mode, status);
