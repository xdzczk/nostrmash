-- Allow the opt-in neighborhoods phase in trust_runs.current_phase.
ALTER TABLE trust_runs
    DROP CONSTRAINT IF EXISTS trust_runs_current_phase_check;

ALTER TABLE trust_runs
    ADD CONSTRAINT trust_runs_current_phase_check
    CHECK (current_phase IS NULL OR current_phase IN ('sync', 'compute', 'neighborhoods', 'promote'));
