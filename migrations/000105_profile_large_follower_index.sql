-- The ingest trust snapshot and pubkey frontier both select accounts at or
-- above LargeAccountFollowerFloor (internal/store/trust/large_accounts.go).
-- Frontier refresh runs every couple of minutes and must not seq-scan
-- profile_public_stats. The predicate has to stay equal to that constant;
-- the Go queries inline the same number so the planner can use this index.
SET LOCAL statement_timeout = '20min';

CREATE INDEX IF NOT EXISTS idx_profile_public_stats_large_followers
    ON profile_public_stats (follower_count DESC, pubkey ASC)
    WHERE follower_count >= 1000;
