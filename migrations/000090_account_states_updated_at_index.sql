-- ListAccountSignalsForRecompute orders account_states by updated_at and
-- does not filter on state. The only updated_at index leads with state, so
-- production planned a parallel sequential scan plus a sort of ~1.9M rows
-- and spilled hundreds of gigabytes of temp files (pg_stat_statements,
-- 2026-10-21..24: 3,959 calls, 776 GB temp).
--
-- (updated_at) matches `WHERE updated_at < $1 ORDER BY updated_at LIMIT n`.
-- account_states is small relative to events; a non-concurrent build inside
-- the migrate transaction is acceptable. IF NOT EXISTS keeps this a no-op
-- where the index was built CONCURRENTLY ahead of the deploy.
SET LOCAL statement_timeout = '10min';

CREATE INDEX IF NOT EXISTS idx_account_states_updated_at
    ON account_states (updated_at ASC);
