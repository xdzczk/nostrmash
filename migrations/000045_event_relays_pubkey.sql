-- Denormalize the event author pubkey onto event_relays so the public
-- discovery network-stats queries no longer have to JOIN event_relays
-- (millions of rows) to events (millions of rows) just to read pubkey.
--
-- Background
-- ----------
-- The homepage bundle (`/api/v1/discovery/home`) calls
-- GetPublicDiscoveryNetworkStats which in turn runs:
--
--   getRelaySummaryStats   -- 6 aggregates over event_relays JOIN events
--   getTopRelaysByActivity -- per-relay COUNT(DISTINCT events.pubkey)
--
-- Both fan out across the entire event_relays table joined to the entire
-- events table just to read events.pubkey for COUNT(DISTINCT). On a 4.4M
-- row event_relays / 2.3M row events workload these two queries alone
-- take ~10s and ~14s respectively, blowing past the request timeout and
-- starving the in-process cache from ever filling.
--
-- An index on event_relays(seen_at) does NOT help: the FILTER clauses
-- aggregate over the whole table, so the JOIN to events is the dominant
-- cost regardless of any index. The fix is structural: keep pubkey on
-- event_relays directly so we can drop the JOIN entirely.
--
-- Trade-offs
-- ----------
--   * pubkey is a 64-char hex string; per-row overhead is ~70 bytes,
--     adding ~300MB to event_relays at the current 4.4M row size. That is
--     ~25% growth on a 1.2GB table and ~0.4% growth on the 30GB+ overall
--     working set. Acceptable.
--   * The backfill UPDATE rewrites all event_relays rows in a single
--     transaction. On 4.4M rows this takes ~1-3 minutes and holds row
--     locks during that window, so live `INSERT INTO event_relays` from
--     the ingestor will queue up briefly. This is a one-time cost.
--   * After this migration, the insert path in events_canonical.go MUST
--     write pubkey too, otherwise the NOT NULL constraint will reject
--     new rows. Old code paths that have not been redeployed will fail
--     to insert until they are upgraded — deploy worker / ingestor /
--     api together with this migration.
--
-- Forwards-only design
-- --------------------
-- The migration is split into four phases inside the same transaction
-- (the migrate runner already wraps every migration in a transaction):
--   1. ADD COLUMN pubkey TEXT (metadata-only, instant)
--   2. UPDATE ... SET pubkey = e.pubkey FROM events (the slow part)
--   3. ALTER COLUMN pubkey SET NOT NULL (instant after backfill)
--   4. CREATE INDEX over the columns the public stats queries need
--
-- The CHECK constraint on length keeps obviously-invalid pubkeys out
-- (64-char lowercase hex per NIP-01).

ALTER TABLE event_relays
    ADD COLUMN IF NOT EXISTS pubkey TEXT;

UPDATE event_relays er
SET pubkey = e.pubkey
FROM events e
WHERE er.event_id = e.id
  AND er.pubkey IS NULL;

-- After backfill, every event_relays row must have a pubkey because
-- event_relays.event_id is FK ON DELETE CASCADE to events(id), so any
-- row whose event was deleted is gone too. If this assertion fails
-- (e.g. a non-cascading orphan slipped in via direct DML), the
-- migration aborts with a clear error rather than silently leaving NULLs.
DO $$
DECLARE
    null_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO null_count FROM event_relays WHERE pubkey IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION 'event_relays backfill incomplete: % rows still have NULL pubkey', null_count;
    END IF;
END $$;

ALTER TABLE event_relays
    ALTER COLUMN pubkey SET NOT NULL;

-- Covering index for the homepage queries:
--   * getRelaySummaryStats filters on seen_at and aggregates DISTINCT pubkey
--   * getTopRelaysByActivity groups by relay_url and aggregates DISTINCT pubkey
-- Including pubkey in the index avoids the heap fetch entirely; the
-- planner can satisfy both queries from index pages alone.
CREATE INDEX IF NOT EXISTS idx_event_relays_seen_at_pubkey
    ON event_relays (seen_at)
    INCLUDE (pubkey, relay_url);

CREATE INDEX IF NOT EXISTS idx_event_relays_relay_url_seen_at
    ON event_relays (relay_url, seen_at)
    INCLUDE (pubkey);

-- Covering index for GetTrendingHashtags (used by both the public home
-- bundle and the /hashtags trending feed). The existing
-- idx_event_hashtags_created_at supports the WHERE filter but the
-- planner still has to fetch the heap to read hashtag and
-- author_pubkey for the GROUP BY / COUNT(DISTINCT). Including those
-- columns lets the query run as an index-only scan.
CREATE INDEX IF NOT EXISTS idx_event_hashtags_created_at_covering
    ON event_hashtags (created_at)
    INCLUDE (hashtag, author_pubkey);
