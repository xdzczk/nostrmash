CREATE TABLE IF NOT EXISTS dm_unread_counts (
    receiver_pubkey TEXT NOT NULL,
    sender_pubkey TEXT NOT NULL DEFAULT '',
    cnt BIGINT NOT NULL CHECK (cnt >= 0),
    latest_at BIGINT NOT NULL DEFAULT 0,
    latest_event_id TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (receiver_pubkey, sender_pubkey)
);

CREATE INDEX IF NOT EXISTS idx_dm_unread_counts_receiver_latest
    ON dm_unread_counts (receiver_pubkey, latest_at DESC, sender_pubkey);

CREATE TABLE IF NOT EXISTS zap_receipts (
    zap_receipt_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    event_id TEXT,
    sender_pubkey TEXT NOT NULL,
    receiver_pubkey TEXT,
    amount_sats BIGINT NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_zap_receipts_receiver_created
    ON zap_receipts (receiver_pubkey, created_at DESC, zap_receipt_id DESC);

CREATE INDEX IF NOT EXISTS idx_zap_receipts_receiver_amount
    ON zap_receipts (receiver_pubkey, amount_sats DESC, created_at DESC, zap_receipt_id DESC);

CREATE INDEX IF NOT EXISTS idx_zap_receipts_event_amount
    ON zap_receipts (event_id, amount_sats DESC, created_at DESC, zap_receipt_id DESC);

CREATE INDEX IF NOT EXISTS idx_zap_receipts_sender_created
    ON zap_receipts (sender_pubkey, created_at DESC, zap_receipt_id DESC);
