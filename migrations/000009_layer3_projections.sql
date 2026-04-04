CREATE TABLE IF NOT EXISTS profiles_latest (
    pubkey TEXT PRIMARY KEY,
    metadata_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    metadata_created_at BIGINT NOT NULL,
    profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_profiles_latest_metadata_created
    ON profiles_latest (metadata_created_at DESC, metadata_event_id DESC);

CREATE TABLE IF NOT EXISTS author_recent_events (
    author_pubkey TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (author_pubkey, event_id)
);

CREATE INDEX IF NOT EXISTS idx_author_recent_events_order
    ON author_recent_events (author_pubkey, created_at DESC, event_id DESC);

CREATE TABLE IF NOT EXISTS reply_count_contributions (
    source_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    target_event_id TEXT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_event_id, target_event_id)
);

CREATE INDEX IF NOT EXISTS idx_reply_count_contributions_target
    ON reply_count_contributions (target_event_id);

CREATE TABLE IF NOT EXISTS reaction_count_contributions (
    source_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    target_event_id TEXT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_event_id, target_event_id)
);

CREATE INDEX IF NOT EXISTS idx_reaction_count_contributions_target
    ON reaction_count_contributions (target_event_id);

CREATE TABLE IF NOT EXISTS repost_count_contributions (
    source_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    target_event_id TEXT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_event_id, target_event_id)
);

CREATE INDEX IF NOT EXISTS idx_repost_count_contributions_target
    ON repost_count_contributions (target_event_id);

CREATE TABLE IF NOT EXISTS reply_counts (
    event_id TEXT PRIMARY KEY,
    count BIGINT NOT NULL CHECK (count >= 0),
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reaction_counts (
    event_id TEXT PRIMARY KEY,
    count BIGINT NOT NULL CHECK (count >= 0),
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS repost_counts (
    event_id TEXT PRIMARY KEY,
    count BIGINT NOT NULL CHECK (count >= 0),
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
