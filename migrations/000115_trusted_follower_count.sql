-- trusted_follower_count is how many latest contact lists that include this
-- pubkey were published by someone in the trust graph (trust_pubkeys_latest,
-- within TRUST_MAX_HOPS). Self-follows stay in follower_count and out of this
-- column. A raw follower_count is not proof an account is popular: a follow
-- farm clears any raw floor and used to lead Most followed and the ingest
-- frontier.
--
-- Do not backfill from follower_edges here. Migration 000086 records that a
-- follower_edges scan during migrate-on-boot was killed by health checks,
-- rolled back, and restarted. The trust worker recounts this column after
-- each snapshot commit. Until that runs the column is 0, so a farm does not
-- rank and does not jump the frontier.
--
-- 000105 and 000110 index raw follower_count. Those predicates cannot serve
-- the trusted-count reads, so they are dropped. The new partial indexes
-- match the inlined Go predicates exactly (trusted_follower_count > 0 and
-- trusted_follower_count >= 1000).
--
-- Production statement_timeout is 15s. Adding a constant-default column is
-- metadata-only. Building the indexes on profile_public_stats is not.
SET LOCAL statement_timeout = 0;

ALTER TABLE profile_public_stats
    ADD COLUMN IF NOT EXISTS trusted_follower_count BIGINT NOT NULL DEFAULT 0
        CHECK (trusted_follower_count >= 0);

DROP INDEX IF EXISTS idx_profile_public_stats_large_followers;
DROP INDEX IF EXISTS idx_profile_public_stats_follower_rank;

CREATE INDEX IF NOT EXISTS idx_profile_public_stats_trusted_follower_rank
    ON profile_public_stats (trusted_follower_count DESC, pubkey ASC)
    WHERE trusted_follower_count > 0;

CREATE INDEX IF NOT EXISTS idx_profile_public_stats_large_trusted_followers
    ON profile_public_stats (trusted_follower_count DESC, pubkey ASC)
    WHERE trusted_follower_count >= 1000;
