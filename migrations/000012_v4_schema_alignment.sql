CREATE TABLE IF NOT EXISTS reaction_events (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    target_event_id TEXT NOT NULL,
    reactor_pubkey TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reaction_events_target_created
    ON reaction_events (target_event_id, created_at DESC, event_id DESC);

CREATE TABLE IF NOT EXISTS repost_events (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    target_event_id TEXT NOT NULL,
    reposter_pubkey TEXT NOT NULL,
    quote TEXT,
    created_at BIGINT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_repost_events_target_created
    ON repost_events (target_event_id, created_at DESC, event_id DESC);

CREATE TABLE IF NOT EXISTS deletion_events (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    deleter_pubkey TEXT NOT NULL,
    target_event_id TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_deletion_events_target_created
    ON deletion_events (target_event_id, created_at DESC, event_id DESC);

CREATE TABLE IF NOT EXISTS contact_lists_latest (
    pubkey TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    contacts_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contact_lists_latest_created
    ON contact_lists_latest (created_at DESC, event_id DESC);

CREATE TABLE IF NOT EXISTS relay_lists_latest (
    pubkey TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    relays_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_relay_lists_latest_created
    ON relay_lists_latest (created_at DESC, event_id DESC);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'derivation_versions'
          AND column_name = 'derivation_name'
    ) THEN
        ALTER TABLE derivation_versions RENAME TO derivation_versions_legacy;
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS derivation_versions (
    projection_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    code_version TEXT NOT NULL DEFAULT 'legacy',
    description TEXT NOT NULL,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (projection_name, version)
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'derivation_versions_legacy') THEN
        INSERT INTO derivation_versions (projection_name, version, code_version, description, activated_at)
        SELECT
            dv.derivation_name,
            dv.version,
            'legacy',
            dv.description,
            dv.updated_at
        FROM derivation_versions_legacy dv
        ON CONFLICT (projection_name, version) DO NOTHING;
    END IF;
END;
$$;
