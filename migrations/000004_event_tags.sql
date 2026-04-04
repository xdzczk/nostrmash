CREATE TABLE IF NOT EXISTS event_tags (
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    tag_name TEXT NOT NULL,
    tag_index INTEGER NOT NULL,
    value_index INTEGER NOT NULL,
    value TEXT NOT NULL,
    raw_values JSONB NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (event_id, tag_index, value_index)
);

CREATE INDEX IF NOT EXISTS idx_event_tags_tag_name_value ON event_tags (tag_name, value)
    WHERE tag_name IN ('e', 'p');
