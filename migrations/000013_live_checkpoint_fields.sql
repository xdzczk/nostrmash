ALTER TABLE ingest_checkpoints
    ADD COLUMN IF NOT EXISTS last_event_id TEXT,
    ADD COLUMN IF NOT EXISTS last_progress_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_error TEXT;
