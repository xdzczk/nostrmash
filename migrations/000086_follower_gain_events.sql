-- True edge-diff follower gains.
--
-- Replaces the withdrawn 000085_follower_gain_events.sql, which took down
-- boot: migrations run inside a single transaction before the API/worker
-- start serving (internal/dbmigrate), and 000085 both bulk-seeded this
-- table from a seq scan of follower_edges (millions of rows — rewrite
-- churn keeps most contact_list_created_at values recent) and took an
-- ACCESS EXCLUSIVE lock to drop follower_gains_daily while old-code
-- services still wrote to it. Container health checks killed the process
-- mid-migration, rolling everything back and restarting the seed from
-- scratch on every boot. This version is DDL-only and idempotent: it
-- applies instantly whether or not 000085 ever committed.
--
-- Deliberate consequences:
--   * No seed: new-follower counts and rising follower momentum start
--     from zero and warm up as kind=3 contact lists flow in. Rising
--     scores still rank on engagement in the meantime. This is the
--     correct trade — the seed could only ever carry the old
--     rewrite-inflated semantics this table exists to fix.
--   * follower_gains_daily (migration 000065) is NOT dropped here. No
--     code reads or writes it anymore; dropping it needs an ACCESS
--     EXCLUSIVE lock and belongs in a later release, once no old-code
--     writers can still be running during a rolling deploy.
--
-- Why this table: "new followers" used to be derived by counting
-- follower_edges rows whose contact_list_created_at fell inside the
-- 24h/7d window. kind=3 contact lists are whole-list replaceables: every
-- rewrite refreshes contact_list_created_at on ALL of the author's
-- current edges, so everyone they already followed re-counted as a "new
-- follower" of every profile on the list. follower_gain_events stores one
-- row per true gain (a pubkey newly appearing on a contact list, diffed
-- against the author's previous edge set) and preserves the gained
-- follower's identity so trust-weighted discovery scoring can weight each
-- gained follower. Rows are pruned by insert age (created_at, not the
-- event-supplied gained_at, which a hostile contact list could post-date
-- to dodge an age prune) via WORKER_RETENTION_FOLLOWER_GAIN_EVENTS_*.
--
-- The (followed, follower) primary key with ON CONFLICT DO NOTHING at
-- write time also dedupes unfollow/refollow churn inside the retention
-- horizon: a re-gain keeps the original gained_at instead of counting or
-- refreshing again.
CREATE TABLE IF NOT EXISTS follower_gain_events (
    followed_pubkey TEXT NOT NULL,
    follower_pubkey TEXT NOT NULL,
    gained_at BIGINT NOT NULL,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (followed_pubkey, follower_pubkey),
    CHECK (followed_pubkey <> follower_pubkey)
);

-- Windowed per-profile counts: the discovery payload's recent_new_followers
-- and the rising-score new-follower inputs all count rows for one
-- followed_pubkey with gained_at >= cutoff.
CREATE INDEX IF NOT EXISTS idx_follower_gain_events_followed_gained
    ON follower_gain_events (followed_pubkey, gained_at DESC);

-- Insert-age retention prune (see retention-query-cost rules: candidate
-- scan is one bounded range read of this index).
CREATE INDEX IF NOT EXISTS idx_follower_gain_events_created_at
    ON follower_gain_events (created_at);
