-- Denormalize the engaged author onto the engagement projection tables so
-- profile-level engagement aggregation needs no events joins.
--
-- loadProfileWeightedScoreInputsTx (the trust-weighted trending/rising score
-- inputs) used to discover "engagement toward pubkey X" by scanning each
-- engagement table's full 7d window and probing the 54 GB events table per
-- row to test the target author (~300k buffer reads and multiple seconds per
-- profile rebuild). Under IO pressure that exceeded the statement timeout for
-- high-engagement profiles, so the profile-stats sweeper failed ~300
-- times/day on the same whales and their discovery stats never rebuilt.
--
-- With target_pubkey stored at projection time (and, for reply
-- contributions, the replier + reply timestamp too), each branch becomes one
-- (target_pubkey, created_at) index range scan proportional to the profile's
-- own received engagement.
--
-- target_pubkey is NULL when the target event is not stored at projection
-- time — the old INNER JOIN to events skipped those rows too, so scoring
-- semantics are preserved. (A target that arrives later no longer
-- retroactively counts; that edge case was worth trading for bounded cost.)
ALTER TABLE reaction_events ADD COLUMN IF NOT EXISTS target_pubkey TEXT;
ALTER TABLE repost_events ADD COLUMN IF NOT EXISTS target_pubkey TEXT;
ALTER TABLE reply_count_contributions ADD COLUMN IF NOT EXISTS target_pubkey TEXT;
ALTER TABLE reply_count_contributions ADD COLUMN IF NOT EXISTS source_pubkey TEXT;
ALTER TABLE reply_count_contributions ADD COLUMN IF NOT EXISTS source_created_at BIGINT;

-- Backfill from stored events. These tables are small (~1.4M rows total in
-- production) and every probe is an events PK lookup, so each UPDATE is
-- minutes at worst. Rows whose target event is absent keep NULL.
UPDATE reaction_events r
SET target_pubkey = e.pubkey
FROM events e
WHERE e.id = r.target_event_id
  AND r.target_pubkey IS NULL;

UPDATE repost_events r
SET target_pubkey = e.pubkey
FROM events e
WHERE e.id = r.target_event_id
  AND r.target_pubkey IS NULL;

-- Source event is FK-guaranteed stored; two passes keep target-missing rows
-- from blocking the source columns.
UPDATE reply_count_contributions c
SET source_pubkey = se.pubkey,
    source_created_at = se.created_at
FROM events se
WHERE se.id = c.source_event_id
  AND c.source_pubkey IS NULL;

UPDATE reply_count_contributions c
SET target_pubkey = te.pubkey
FROM events te
WHERE te.id = c.target_event_id
  AND c.target_pubkey IS NULL;

-- Partial indexes: NULL target_pubkey rows can never match a per-profile
-- lookup, and reply contributions additionally need source_created_at for
-- the window bound.
CREATE INDEX IF NOT EXISTS idx_reaction_events_target_pubkey_created
    ON reaction_events (target_pubkey, created_at)
    WHERE target_pubkey IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_repost_events_target_pubkey_created
    ON repost_events (target_pubkey, created_at)
    WHERE target_pubkey IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_reply_contributions_target_pubkey_created
    ON reply_count_contributions (target_pubkey, source_created_at)
    WHERE target_pubkey IS NOT NULL;
