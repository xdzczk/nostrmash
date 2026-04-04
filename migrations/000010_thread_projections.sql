CREATE TABLE IF NOT EXISTS thread_edges (
    child_event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    child_created_at BIGINT NOT NULL,
    parent_event_id TEXT NOT NULL,
    root_event_id TEXT,
    parent_missing BOOLEAN NOT NULL DEFAULT FALSE,
    root_missing BOOLEAN NOT NULL DEFAULT FALSE,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_thread_edges_parent_order
    ON thread_edges (parent_event_id, child_created_at ASC, child_event_id ASC);

CREATE INDEX IF NOT EXISTS idx_thread_edges_root
    ON thread_edges (root_event_id, child_created_at ASC, child_event_id ASC);

CREATE TABLE IF NOT EXISTS unresolved_thread_references (
    source_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    missing_event_id TEXT NOT NULL,
    relation TEXT NOT NULL CHECK (relation IN ('root', 'reply')),
    derivation_version INTEGER NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_event_id, missing_event_id, relation)
);

CREATE INDEX IF NOT EXISTS idx_unresolved_thread_references_missing
    ON unresolved_thread_references (missing_event_id);
