-- Kind expansion projections: NIP-22 comments, NIP-18 address reposts, and
-- NIP-56 report targets. Also seeds untrusted-purge cursors for the new
-- author-gated kinds and rebuilds the disallowed-tag partial index so the
-- prune predicate matches the expanded event_tags allowlist.

CREATE TABLE IF NOT EXISTS comment_edges (
    source_event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    root_event_id TEXT NOT NULL DEFAULT '',
    root_address TEXT NOT NULL DEFAULT '',
    parent_event_id TEXT NOT NULL DEFAULT '',
    parent_address TEXT NOT NULL DEFAULT '',
    author_pubkey TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_comment_edges_root_event
    ON comment_edges (root_event_id, created_at ASC, source_event_id ASC)
    WHERE root_event_id <> '';

CREATE INDEX IF NOT EXISTS idx_comment_edges_root_address
    ON comment_edges (root_address, created_at ASC, source_event_id ASC)
    WHERE root_address <> '';

CREATE INDEX IF NOT EXISTS idx_comment_edges_parent_event
    ON comment_edges (parent_event_id, created_at ASC, source_event_id ASC)
    WHERE parent_event_id <> '';

CREATE INDEX IF NOT EXISTS idx_comment_edges_parent_address
    ON comment_edges (parent_address, created_at ASC, source_event_id ASC)
    WHERE parent_address <> '';

CREATE TABLE IF NOT EXISTS address_repost_contributions (
    source_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    target_address TEXT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_event_id, target_address)
);

CREATE INDEX IF NOT EXISTS idx_address_repost_contributions_target
    ON address_repost_contributions (target_address);

CREATE TABLE IF NOT EXISTS report_targets (
    report_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    tag_name TEXT NOT NULL,
    target_value TEXT NOT NULL,
    reporter_pubkey TEXT NOT NULL,
    report_type TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (report_event_id, tag_name, target_value)
);

CREATE INDEX IF NOT EXISTS idx_report_targets_event
    ON report_targets (target_value, created_at DESC, report_event_id DESC)
    WHERE tag_name = 'e';

CREATE INDEX IF NOT EXISTS idx_report_targets_pubkey
    ON report_targets (target_value, created_at DESC, report_event_id DESC)
    WHERE tag_name = 'p';

INSERT INTO untrusted_purge_kind_cursor (kind)
VALUES (20), (21), (22), (1984), (10001), (30000), (30311), (34235), (34236)
ON CONFLICT (kind) DO NOTHING;

-- 000071 created this index with the previous allowlist. IF NOT EXISTS will
-- not replace that predicate, so drop and recreate it to match
-- eventtags.AllowedTagNames (title, summary, status, starts).
DROP INDEX IF EXISTS idx_event_tags_disallowed_tag_name;
CREATE INDEX IF NOT EXISTS idx_event_tags_disallowed_tag_name
    ON event_tags (tag_name)
    WHERE tag_name NOT IN (
        'a', 'd', 'e', 'g', 'group', 'image', 'imeta', 'm', 'p', 'r',
        'series', 'starts', 'status', 'summary', 't', 'thumb', 'title', 'u', 'url', 'video', 'word'
    );
