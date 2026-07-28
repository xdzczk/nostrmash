-- events is a ~30M-row hot table. Percentage-based autovacuum thresholds
-- (scale_factor=0.05 → ~1.5M dead tuples) let the table go days without a
-- vacuum, which empties the visibility map and disables index-only scans.
-- That turns every per-pubkey analytics rebuild into random heap I/O across
-- a 34 GB heap and stalls author-analytics / profile-stats sweepers.
--
-- Switch to fixed absolute thresholds so vacuum/analyze keep pace with
-- retention deletes and steady ingest regardless of table size.
ALTER TABLE events SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 200000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 100000
);
