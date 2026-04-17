-- Backing table for asynchronous profile-stats recomputes.
--
-- The previous design ran ProjectProfilePublicStats and
-- ProjectProfileDiscoveryStats inline inside derive_event_bundle for
-- every incoming event. Each step does a fan-out of multi-second
-- COUNT/JOIN aggregates over the events / reply_count_contributions /
-- reaction_events / repost_events / zap_receipts tables for the affected
-- pubkey, then takes a per-pubkey advisory lock for the upsert. For hot
-- pubkeys receiving high-frequency replies/reposts/reactions/zaps, this
-- meant: (a) bundle workers serialized on the per-(pubkey, namespace)
-- advisory lock, and (b) each acquired lock then ran multi-second
-- aggregates — production observed advisory-lock waits of 8-13 seconds
-- per worker with the underlying COUNT queries running 19-30 seconds.
--
-- The new design: derive_event_bundle does a cheap upsert into this
-- table marking each affected pubkey as dirty. A separate background
-- sweeper in the worker process drains a batch of dirty pubkeys at a
-- time and runs BOTH ProjectProfilePublicStats AND
-- ProjectProfileDiscoveryStats once per pubkey per cycle, naturally
-- coalescing bursts into a single recompute.
--
-- Affected-pubkey semantics match the previous inline behavior: for
-- public_stats only the event author is affected; for discovery_stats
-- the event author plus referenced pubkeys (reply targets, repost
-- targets, reaction targets, zap receivers, kind=3 follow targets).
-- The sweeper recomputes BOTH projections per dirty pubkey — this
-- slightly over-computes public_stats for non-author dirty pubkeys
-- (their public_stats values won't change), but the work is
-- bounded-cost and the coalescing win dominates.
--
-- The (marked_at, pubkey) ordering lets the sweeper claim the
-- oldest-marked rows first via FOR UPDATE SKIP LOCKED so multiple
-- sweeper workers can drain the table in parallel without contention.

CREATE TABLE IF NOT EXISTS pending_profile_stats_recomputes (
    pubkey TEXT PRIMARY KEY,
    marked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pending_profile_stats_marked_at
    ON pending_profile_stats_recomputes (marked_at ASC, pubkey ASC);
