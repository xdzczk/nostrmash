SET LOCAL statement_timeout = '10min';

CREATE INDEX IF NOT EXISTS idx_events_pubkey_kind_created
    ON events (pubkey, kind, created_at DESC, id DESC);
