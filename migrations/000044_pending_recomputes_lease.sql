-- Decouple sweeper rebuild work from the pending-row lifetime.
--
-- Background
-- ----------
-- The previous sweeper design (migrations 000041 and 000043) used a single
-- transaction that:
--   1. claimed a dirty pubkey by DELETE ... RETURNING
--   2. acquired the per-pubkey advisory lock
--   3. ran the heavy rebuild aggregations (30-160s on hot pubkeys)
--   4. committed (only at this point releasing all locks).
--
-- That avoided the older sweeper-vs-sweeper advisory-lock chain, but it
-- introduced a new sweeper-vs-producer row-lock chain: the DELETE in step 1
-- holds a row-level lock on the pending row for the entire 30-160s rebuild.
-- Concurrent bundle workers running
--     INSERT INTO pending_profile_stats_recomputes (pubkey)
--     SELECT unnest($1::text[]) ON CONFLICT (pubkey) DO NOTHING
-- for the same pubkey must wait to see whether the DELETE will commit (no
-- conflict) or roll back (conflict), so they pile up on the row's
-- transactionid lock for the same 30-160s. Production observed dozens of
-- bundle workers stalled at this point, completely starving the
-- derive_event_bundle pool of database connections.
--
-- New design (this migration + the matching Go refactor)
-- ------------------------------------------------------
-- Replace the single long transaction with three short/long phases:
--
--   Phase 1 (short tx): atomic claim by setting claimed_at = now() and a
--     fresh claim_token. Producer INSERTs ON CONFLICT DO NOTHING see the row
--     as already present, hit the conflict path, and return in microseconds.
--   Phase 2 (long tx): re-acquire the per-pubkey advisory lock (blocking;
--     uncontested because phase 1's pg_try_advisory_xact_lock filter
--     guarantees no other sweeper picked the same pubkey) and run the
--     rebuild. No locks on pending_*_recomputes are held during this phase.
--   Phase 3 (short tx): DELETE WHERE pubkey = $1 AND claim_token = $2. The
--     CAS on claim_token ensures we never delete a row that was re-marked
--     dirty after we claimed it (in that case, claim_token was reset by a
--     subsequent re-claim and the next sweeper cycle will pick it up).
--
-- Stale-claim recovery: phase 1's filter is
--     claimed_at IS NULL OR claimed_at < now() - interval '5 minutes'
-- so a worker that crashes mid-rebuild has its claim auto-released after
-- the lease expires.
--
-- Backwards compatibility: the columns are nullable with NULL defaults, so
-- code paths that have not been updated continue to work (they will see
-- claimed_at IS NULL and treat all rows as claimable). Any orphan rows
-- written by old code are picked up by the new sweeper without intervention.

ALTER TABLE pending_author_analytics_recomputes
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS claim_token UUID;

ALTER TABLE pending_profile_stats_recomputes
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS claim_token UUID;

-- Partial index over unclaimed/expired rows to keep the phase-1 candidate
-- scan O(log n) regardless of how many rows are mid-rebuild. The lease
-- threshold (5 minutes) is hard-coded in the WHERE clause is intentional:
-- the index does not need to be perfectly tight, only good enough to skip
-- the bulk of in-flight claims; the sweeper's runtime filter is the source
-- of truth for the actual lease duration.
CREATE INDEX IF NOT EXISTS idx_pending_author_analytics_claimable
    ON pending_author_analytics_recomputes (marked_at ASC, pubkey ASC)
    WHERE claimed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_pending_profile_stats_claimable
    ON pending_profile_stats_recomputes (marked_at ASC, pubkey ASC)
    WHERE claimed_at IS NULL;
