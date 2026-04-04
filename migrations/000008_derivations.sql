CREATE TABLE IF NOT EXISTS event_references (
    source_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    referenced_event_id TEXT NOT NULL,
    relation TEXT NOT NULL CHECK (relation IN ('root', 'reply', 'mention')),
    tag_index INTEGER NOT NULL,
    relay_hint TEXT,
    marker TEXT,
    derivation_version INTEGER NOT NULL,
    derived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_event_id, tag_index, referenced_event_id, relation)
);

CREATE INDEX IF NOT EXISTS idx_event_references_referenced
    ON event_references (referenced_event_id, relation);

CREATE TABLE IF NOT EXISTS pubkey_references (
    source_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    referenced_pubkey TEXT NOT NULL,
    relation TEXT NOT NULL CHECK (relation IN ('root', 'reply', 'mention')),
    tag_index INTEGER NOT NULL,
    relay_hint TEXT,
    marker TEXT,
    derivation_version INTEGER NOT NULL,
    derived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_event_id, tag_index, referenced_pubkey, relation)
);

CREATE INDEX IF NOT EXISTS idx_pubkey_references_referenced
    ON pubkey_references (referenced_pubkey, relation);

CREATE TABLE IF NOT EXISTS replaceable_state (
    pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    d_tag TEXT NOT NULL DEFAULT '',
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, kind, d_tag)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_replaceable_state_event_id
    ON replaceable_state (event_id);

CREATE TABLE IF NOT EXISTS derivation_versions (
    derivation_name TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    description TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
