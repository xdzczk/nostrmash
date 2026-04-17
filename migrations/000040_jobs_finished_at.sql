-- Adds an explicit `finished_at` timestamp to the jobs queue so terminal-job
-- retention can purge by completion time instead of `updated_at`, which moves
-- on every claim/retry/maintenance touch.
--
-- The column is nullable so workers running OLD code keep writing terminal
-- rows without it (the new code sets it on every terminal transition). The
-- partial index keeps the retention purge cheap.
--
-- The inline backfill is bounded by the current jobs row count (~5.5M in the
-- known production deployment). Operators with very large jobs tables may run
-- the UPDATE manually in batches before deploying; on the next deploy the
-- IF NOT EXISTS / WHERE finished_at IS NULL guard makes the statement a no-op.

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;

UPDATE jobs
   SET finished_at = updated_at
 WHERE status IN ('succeeded', 'dead')
   AND finished_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_terminal_finished_at
    ON jobs (status, finished_at)
    WHERE status IN ('succeeded', 'dead');
