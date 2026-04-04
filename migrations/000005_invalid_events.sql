CREATE TABLE IF NOT EXISTS invalid_events (
    id BIGSERIAL PRIMARY KEY,
    source_relay TEXT,
    error_code TEXT NOT NULL,
    error_message TEXT NOT NULL,
    raw_payload JSONB,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
