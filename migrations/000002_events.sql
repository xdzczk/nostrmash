CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    pubkey TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    kind INTEGER NOT NULL,
    sig TEXT NOT NULL,
    content TEXT NOT NULL,
    raw_json JSONB NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_pubkey_created_at ON events (pubkey, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_kind_created_at ON events (kind, created_at DESC);
