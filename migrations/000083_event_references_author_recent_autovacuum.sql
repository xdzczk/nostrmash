-- Eager autovacuum for the two churniest projection tables after
-- follower_edges (which got the same treatment in 000077).
--
-- Both tables CASCADE-delete with events retention and author_recent_events
-- additionally has its own pruning loop (age horizon + per-author cap), so
-- they sustain millions of deletes while their live row counts stay modest
-- (~8.7M / ~3.8M rows in production). With the default autovacuum scale
-- factor (0.2, i.e. ~1.7M dead tuples before a vacuum triggers) dead index
-- entries accumulated far faster than they were reclaimed: production ended
-- up with 15 GB of indexes over an 8 GB heap on event_references and 14 GB
-- over 5 GB on author_recent_events — pure churn bloat, not data. (The
-- standing bloat itself is reclaimed out-of-band with REINDEX CONCURRENTLY;
-- this migration keeps it from coming back.)
ALTER TABLE event_references SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 100000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 100000
);

ALTER TABLE author_recent_events SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 100000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 100000
);
