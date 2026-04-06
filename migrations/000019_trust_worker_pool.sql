ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS worker_pool TEXT;

UPDATE jobs
SET worker_pool = 'default'
WHERE worker_pool IS NULL OR btrim(worker_pool) = '';

ALTER TABLE jobs
    ALTER COLUMN worker_pool SET DEFAULT 'default';

ALTER TABLE jobs
    ALTER COLUMN worker_pool SET NOT NULL;

DROP INDEX IF EXISTS idx_jobs_claim_pending;
CREATE INDEX IF NOT EXISTS idx_jobs_claim_pending
    ON jobs (worker_pool, run_after ASC, id ASC)
    WHERE status = 'pending';
