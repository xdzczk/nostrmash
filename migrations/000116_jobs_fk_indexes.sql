-- Deleting a terminal job checks five foreign keys. None of those columns
-- were indexed, so each deleted row sequentially scanned trust_runs and
-- projection_rebuild_runs. The production batch is 20000 rows and the
-- statement timeout is 15s, so the job-retention purge timed out every tick
-- and the succeeded backlog never shrank.
--
-- These tables are a few thousand rows. The indexes make each foreign-key
-- check a lookup, so a batch costs the batch size rather than the batch
-- size times the referencing tables.

CREATE INDEX IF NOT EXISTS idx_trust_runs_job_id
    ON trust_runs (job_id);

CREATE INDEX IF NOT EXISTS idx_trust_runs_sync_job_id
    ON trust_runs (sync_job_id);

CREATE INDEX IF NOT EXISTS idx_trust_runs_compute_job_id
    ON trust_runs (compute_job_id);

CREATE INDEX IF NOT EXISTS idx_trust_runs_promote_job_id
    ON trust_runs (promote_job_id);

CREATE INDEX IF NOT EXISTS idx_projection_rebuild_runs_job_id
    ON projection_rebuild_runs (job_id);
