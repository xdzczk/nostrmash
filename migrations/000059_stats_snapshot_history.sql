-- Append-only hourly history for the public discovery stats series.
--
-- Values are rolling 24-hour totals captured by the homepage snapshot worker.
-- Keeping one row per UTC hour makes the write idempotent across the worker's
-- five-minute cadence and across multiple worker replicas.
CREATE TABLE IF NOT EXISTS stats_snapshot_history (
    bucket_start   TIMESTAMPTZ NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL,
    note_volume    BIGINT      NOT NULL,
    active_authors BIGINT      NOT NULL,
    relay_events   BIGINT      NOT NULL,
    PRIMARY KEY (bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_stats_snapshot_history_bucket_start
    ON stats_snapshot_history (bucket_start DESC);

COMMENT ON TABLE stats_snapshot_history IS
    'Hourly append-only history of rolling 24-hour public discovery metrics, written by RefreshRelayWindowSnapshots.';
