CREATE TABLE IF NOT EXISTS primal_notifications (
    dedupe_key TEXT PRIMARY KEY,
    pubkey TEXT NOT NULL,
    type SMALLINT NOT NULL,
    created_at BIGINT NOT NULL,
    args JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_primal_notifications_pubkey_created
    ON primal_notifications (pubkey, created_at DESC, dedupe_key DESC);

CREATE TABLE IF NOT EXISTS primal_notifications_seen (
    pubkey TEXT PRIMARY KEY,
    seen_until BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS primal_notification_counts (
    pubkey TEXT NOT NULL,
    type SMALLINT NOT NULL,
    cnt BIGINT NOT NULL CHECK (cnt >= 0),
    PRIMARY KEY (pubkey, type)
);
