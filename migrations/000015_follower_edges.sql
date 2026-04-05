CREATE TABLE IF NOT EXISTS follower_edges (
    followed_pubkey TEXT NOT NULL,
    follower_pubkey TEXT NOT NULL,
    source_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    contact_list_created_at BIGINT NOT NULL,
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (followed_pubkey, follower_pubkey)
);

CREATE INDEX IF NOT EXISTS idx_follower_edges_lookup
    ON follower_edges (followed_pubkey, contact_list_created_at DESC, source_event_id DESC, follower_pubkey ASC);

CREATE INDEX IF NOT EXISTS idx_follower_edges_by_follower
    ON follower_edges (follower_pubkey);
