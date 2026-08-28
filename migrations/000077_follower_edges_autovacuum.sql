-- follower_edges has extreme churn: every kind=3 contact-list update
-- deletes and reinserts the author's entire edge set (~42M lifetime
-- inserts and ~42M lifetime deletes against ~5-6M live rows). With the
-- default autovacuum scale factor (0.2, ~1.2M dead tuples before a
-- vacuum triggers), dead line pointers accumulated in the wide
-- idx_follower_edges_lookup composite index until it bloated to 7.1 GB
-- against an 11 GB heap. At that size a single autovacuum's index
-- vacuuming phase ran for 2.5+ days at 100% CPU, starving the host
-- (sustained 500+ MB/s reads, ~20% iowait, statement timeouts across
-- the API) while further churn kept piling up behind it.
--
-- Vacuum far more eagerly so each pass stays small and the indexes
-- never accumulate enough garbage to bloat: trigger at ~100k dead
-- tuples (roughly one large follow-list rewrite burst) instead of 1.2M.
ALTER TABLE follower_edges SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 100000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 100000
);
