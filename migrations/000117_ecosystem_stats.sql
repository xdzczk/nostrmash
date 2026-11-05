-- Ecosystem stats: event attributes that were previously dropped at ingest,
-- identity fields, NIP-05 verification state, wrapped DMs, nutzaps, zap
-- economics, reaction breakdowns, and bounded relay-propagation rollups.
-- Daily counter tables are first-write aggregates. Raw engagement and
-- wrapped-DM rows are still purged; these rollups are what survives.

CREATE TABLE IF NOT EXISTS event_attributes (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    client TEXT NOT NULL DEFAULT '',
    client_pubkey TEXT NOT NULL DEFAULT '',
    expires_at BIGINT,
    content_warning BOOLEAN NOT NULL DEFAULT FALSE,
    content_warning_reason TEXT NOT NULL DEFAULT '',
    proxy_protocol TEXT NOT NULL DEFAULT '',
    proxy_id TEXT NOT NULL DEFAULT '',
    pow_bits SMALLINT NOT NULL DEFAULT 0,
    pow_target SMALLINT NOT NULL DEFAULT 0,
    target_kind INTEGER,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_event_attributes_expires_at
    ON event_attributes (expires_at)
    WHERE expires_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_event_attributes_client
    ON event_attributes (client)
    WHERE client <> '';

CREATE TABLE IF NOT EXISTS client_daily (
    day DATE NOT NULL,
    client TEXT NOT NULL,
    kind INTEGER NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, client, kind)
);

CREATE TABLE IF NOT EXISTS client_author_daily (
    day DATE NOT NULL,
    client TEXT NOT NULL,
    pubkey TEXT NOT NULL,
    PRIMARY KEY (day, client, pubkey)
);

CREATE TABLE IF NOT EXISTS content_flags_daily (
    day DATE PRIMARY KEY,
    total_count BIGINT NOT NULL DEFAULT 0,
    bridged_count BIGINT NOT NULL DEFAULT 0,
    content_warning_count BIGINT NOT NULL DEFAULT 0,
    pow_count BIGINT NOT NULL DEFAULT 0,
    bridged_by_protocol JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS profile_fields (
    pubkey TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    display_name TEXT NOT NULL DEFAULT '',
    nip05 TEXT NOT NULL DEFAULT '',
    nip05_local TEXT NOT NULL DEFAULT '',
    nip05_domain TEXT NOT NULL DEFAULT '',
    lud16 TEXT NOT NULL DEFAULT '',
    lud16_domain TEXT NOT NULL DEFAULT '',
    has_lud06 BOOLEAN NOT NULL DEFAULT FALSE,
    website_domain TEXT NOT NULL DEFAULT '',
    has_picture BOOLEAN NOT NULL DEFAULT FALSE,
    has_banner BOOLEAN NOT NULL DEFAULT FALSE,
    has_about BOOLEAN NOT NULL DEFAULT FALSE,
    is_bot BOOLEAN NOT NULL DEFAULT FALSE,
    completeness_score SMALLINT NOT NULL DEFAULT 0,
    metadata_created_at BIGINT NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_profile_fields_nip05_domain
    ON profile_fields (nip05_domain)
    WHERE nip05_domain <> '';

CREATE INDEX IF NOT EXISTS idx_profile_fields_lud16_domain
    ON profile_fields (lud16_domain)
    WHERE lud16_domain <> '';

CREATE TABLE IF NOT EXISTS nip05_verifications (
    pubkey TEXT PRIMARY KEY,
    nip05 TEXT NOT NULL,
    domain TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'verified', 'mismatch', 'not_found', 'unreachable', 'invalid', 'blocked')),
    checked_at TIMESTAMPTZ,
    next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    failure_count INTEGER NOT NULL DEFAULT 0,
    last_error_code TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nip05_verifications_due
    ON nip05_verifications (next_check_at)
    WHERE status <> 'blocked';

CREATE TABLE IF NOT EXISTS dm_volume_daily (
    day DATE NOT NULL,
    receiver_pubkey TEXT NOT NULL,
    protocol TEXT NOT NULL,
    message_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, receiver_pubkey, protocol)
);

CREATE TABLE IF NOT EXISTS dm_relay_lists_latest (
    pubkey TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    relays_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS nutzap_receipts (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    sender_pubkey TEXT NOT NULL,
    receiver_pubkey TEXT NOT NULL DEFAULT '',
    target_event_id TEXT NOT NULL DEFAULT '',
    target_address TEXT NOT NULL DEFAULT '',
    amount_sats BIGINT NOT NULL DEFAULT 0,
    mint_url TEXT NOT NULL DEFAULT '',
    unit TEXT NOT NULL DEFAULT '',
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nutzap_receipts_receiver_created
    ON nutzap_receipts (receiver_pubkey, created_at DESC);

CREATE TABLE IF NOT EXISTS nutzap_info_latest (
    pubkey TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    relays_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    mints_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    p2pk TEXT NOT NULL DEFAULT '',
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE note_discovery_stats
    ADD COLUMN IF NOT EXISTS nutzap_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS nutzap_sats BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS zap_daily (
    day DATE PRIMARY KEY,
    zap_count BIGINT NOT NULL DEFAULT 0,
    sats_total BIGINT NOT NULL DEFAULT 0,
    self_zap_count BIGINT NOT NULL DEFAULT 0,
    nutzap_count BIGINT NOT NULL DEFAULT 0,
    nutzap_sats BIGINT NOT NULL DEFAULT 0,
    hist JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS zap_participant_daily (
    day DATE NOT NULL,
    pubkey TEXT NOT NULL,
    role TEXT NOT NULL,
    PRIMARY KEY (day, pubkey, role)
);

CREATE TABLE IF NOT EXISTS profile_zap_daily (
    pubkey TEXT NOT NULL,
    day DATE NOT NULL,
    received_count BIGINT NOT NULL DEFAULT 0,
    received_sats BIGINT NOT NULL DEFAULT 0,
    sent_count BIGINT NOT NULL DEFAULT 0,
    sent_sats BIGINT NOT NULL DEFAULT 0,
    nutzap_received_count BIGINT NOT NULL DEFAULT 0,
    nutzap_received_sats BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (pubkey, day)
);

CREATE TABLE IF NOT EXISTS zap_edges (
    sender_pubkey TEXT NOT NULL,
    receiver_pubkey TEXT NOT NULL,
    zap_count BIGINT NOT NULL DEFAULT 0,
    sats_total BIGINT NOT NULL DEFAULT 0,
    first_at BIGINT NOT NULL,
    last_at BIGINT NOT NULL,
    PRIMARY KEY (sender_pubkey, receiver_pubkey)
);

CREATE TABLE IF NOT EXISTS reaction_content_edges (
    source_event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    target_event_id TEXT NOT NULL,
    content_key TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reaction_content_edges_target
    ON reaction_content_edges (target_event_id);

CREATE TABLE IF NOT EXISTS reaction_content_counts (
    target_event_id TEXT NOT NULL,
    content_key TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (target_event_id, content_key)
);

CREATE OR REPLACE FUNCTION reaction_content_edge_decrement()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE reaction_content_counts
    SET count = count - 1
    WHERE target_event_id = OLD.target_event_id
      AND content_key = OLD.content_key;
    DELETE FROM reaction_content_counts
    WHERE target_event_id = OLD.target_event_id
      AND content_key = OLD.content_key
      AND count <= 0;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS trg_reaction_content_edge_decrement ON reaction_content_edges;
CREATE TRIGGER trg_reaction_content_edge_decrement
    AFTER DELETE ON reaction_content_edges
    FOR EACH ROW
    EXECUTE FUNCTION reaction_content_edge_decrement();

CREATE TABLE IF NOT EXISTS profile_reaction_daily (
    pubkey TEXT NOT NULL,
    day DATE NOT NULL,
    plus_count BIGINT NOT NULL DEFAULT 0,
    minus_count BIGINT NOT NULL DEFAULT 0,
    emoji_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (pubkey, day)
);

CREATE TABLE IF NOT EXISTS author_first_seen (
    pubkey TEXT PRIMARY KEY,
    first_seen_at TIMESTAMPTZ NOT NULL,
    first_event_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_author_first_seen_at
    ON author_first_seen (first_seen_at);

CREATE TABLE IF NOT EXISTS relay_propagation_daily (
    day DATE NOT NULL,
    relay_url TEXT NOT NULL,
    events_seen BIGINT NOT NULL DEFAULT 0,
    first_seen_count BIGINT NOT NULL DEFAULT 0,
    exclusive_count BIGINT NOT NULL DEFAULT 0,
    latency_p50_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    latency_p90_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    future_dated_count BIGINT NOT NULL DEFAULT 0,
    untrusted_event_share DOUBLE PRECISION,
    kind_mix JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (day, relay_url)
);

CREATE TABLE IF NOT EXISTS relay_overlap_daily (
    day DATE NOT NULL,
    relay_a TEXT NOT NULL,
    relay_b TEXT NOT NULL,
    shared_events BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, relay_a, relay_b)
);

CREATE TABLE IF NOT EXISTS ecosystem_daily_rollup (
    day DATE PRIMARY KEY,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE stats_snapshot_history
    ADD COLUMN IF NOT EXISTS zap_count_24h BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sats_24h BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS new_authors_24h BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS nip17_dm_count_24h BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS bridged_share_bps BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_author_activity_daily_posting_date
    ON author_activity_daily (activity_date)
    WHERE post_count > 0;

INSERT INTO untrusted_purge_kind_cursor (kind)
VALUES (10019), (10050)
ON CONFLICT (kind) DO NOTHING;
