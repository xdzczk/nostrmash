-- True edge-diff follower gains.
--
-- "New followers" used to be derived by counting follower_edges rows whose
-- contact_list_created_at fell inside the 24h/7d window. kind=3 contact
-- lists are whole-list replaceables: every rewrite (following or unfollowing
-- anyone, or a client resync) refreshes contact_list_created_at on ALL of
-- the author's current edges, so everyone they already followed re-counted
-- as a "new follower" of every profile on the list. Profiles with active
-- followers showed hundreds of daily "new followers" while their actual
-- follower_count barely moved.
--
-- follower_gain_events stores one row per true gain (a pubkey newly
-- appearing on a contact list, diffed against the author's previous edge
-- set — the same diff that feeds profile_public_stats follower_count
-- deltas). It preserves the gained follower's identity so trust-weighted
-- discovery scoring can weight each gained follower by trust proximity,
-- which the previous integer-per-day rollup (follower_gains_daily) could
-- not support. Nothing reads past the 7d discovery window, so rows are
-- pruned by insert age (created_at, not the event-supplied gained_at,
-- which a hostile contact list could post-date to dodge an age prune) via
-- the WORKER_RETENTION_FOLLOWER_GAIN_EVENTS_* retention loop.
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

-- Seed from current follower_edges timestamps so discovery scores and the
-- recent_new_followers payload don't collapse to zero at deploy. Seeded
-- rows necessarily carry the old rewrite-inflated semantics (an edge's
-- contact_list_created_at is the follower's latest list rewrite, not when
-- the follow began); live writes are true edge-diff gains, so the
-- inflation washes out as the seed ages past the 7d read window. Edges
-- with future timestamps are skipped so a post-dated contact list can't
-- plant rows that outlive every window.
INSERT INTO follower_gain_events (followed_pubkey, follower_pubkey, gained_at, derivation_version)
SELECT
    fe.followed_pubkey,
    fe.follower_pubkey,
    fe.contact_list_created_at,
    1
FROM follower_edges fe
WHERE fe.contact_list_created_at >= extract(epoch FROM now() - interval '7 days')::bigint
  AND fe.contact_list_created_at <= extract(epoch FROM now() + interval '1 hour')::bigint
  AND fe.followed_pubkey <> fe.follower_pubkey
ON CONFLICT (followed_pubkey, follower_pubkey) DO NOTHING;

-- follower_gains_daily (migration 000065) fed only the incremental
-- rising-score path; every new-follower reader now uses
-- follower_gain_events (exact rolling windows instead of calendar days,
-- plus follower identity for trust weighting) and the kind=3 write path
-- stopped maintaining it.
DROP TABLE IF EXISTS follower_gains_daily;
