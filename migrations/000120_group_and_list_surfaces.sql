SET LOCAL statement_timeout = 0;

CREATE TABLE IF NOT EXISTS groups_latest (
    relay_url TEXT NOT NULL,
    group_id TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    about TEXT NOT NULL DEFAULT '',
    picture TEXT NOT NULL DEFAULT '',
    is_public BOOLEAN NOT NULL DEFAULT false,
    is_open BOOLEAN NOT NULL DEFAULT false,
    admin_count INTEGER NOT NULL DEFAULT 0,
    member_count INTEGER NOT NULL DEFAULT 0,
    metadata_event_id TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (relay_url, group_id)
);

CREATE TABLE IF NOT EXISTS group_messages (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    relay_url TEXT NOT NULL,
    group_id TEXT NOT NULL,
    author TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    kind INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_group_messages_group_created
    ON group_messages (relay_url, group_id, created_at DESC);

CREATE TABLE IF NOT EXISTS group_author_daily (
    relay_url TEXT NOT NULL,
    group_id TEXT NOT NULL,
    day DATE NOT NULL,
    pubkey TEXT NOT NULL,
    PRIMARY KEY (relay_url, group_id, day, pubkey)
);

CREATE TABLE IF NOT EXISTS group_activity_daily (
    relay_url TEXT NOT NULL,
    group_id TEXT NOT NULL,
    day DATE NOT NULL,
    message_count BIGINT NOT NULL DEFAULT 0,
    unique_authors BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (relay_url, group_id, day)
);

CREATE TABLE IF NOT EXISTS live_activity_stats (
    address TEXT PRIMARY KEY,
    chat_count BIGINT NOT NULL DEFAULT 0,
    unique_chatters BIGINT NOT NULL DEFAULT 0,
    last_chat_at BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS live_chat_daily (
    day DATE NOT NULL,
    address TEXT NOT NULL,
    chat_count BIGINT NOT NULL DEFAULT 0,
    unique_chatters BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, address)
);

CREATE TABLE IF NOT EXISTS live_chat_author_daily (
    day DATE NOT NULL,
    address TEXT NOT NULL,
    pubkey TEXT NOT NULL,
    PRIMARY KEY (day, address, pubkey)
);

CREATE TABLE IF NOT EXISTS polls (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    author TEXT NOT NULL,
    question TEXT NOT NULL DEFAULT '',
    options JSONB NOT NULL DEFAULT '[]'::jsonb,
    poll_type TEXT NOT NULL DEFAULT 'singlechoice',
    ends_at BIGINT
);

CREATE TABLE IF NOT EXISTS poll_votes (
    poll_event_id TEXT NOT NULL,
    voter TEXT NOT NULL,
    option_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (poll_event_id, voter)
);

CREATE TABLE IF NOT EXISTS poll_option_counts (
    poll_event_id TEXT NOT NULL,
    option_id TEXT NOT NULL,
    votes BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (poll_event_id, option_id)
);

CREATE TABLE IF NOT EXISTS badge_definitions (
    address TEXT PRIMARY KEY,
    issuer TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    image TEXT NOT NULL DEFAULT '',
    thumb TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS badge_awards (
    award_event_id TEXT NOT NULL,
    definition_address TEXT NOT NULL,
    recipient TEXT NOT NULL,
    awarded_at BIGINT NOT NULL,
    PRIMARY KEY (award_event_id, definition_address, recipient)
);

CREATE INDEX IF NOT EXISTS idx_badge_awards_recipient ON badge_awards (recipient, awarded_at DESC);

CREATE TABLE IF NOT EXISTS profile_badges_latest (
    pubkey TEXT PRIMARY KEY,
    accepted JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS zap_goals (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    author TEXT NOT NULL,
    amount_msats BIGINT NOT NULL DEFAULT 0,
    raised_sats BIGINT NOT NULL DEFAULT 0,
    closed_at BIGINT,
    summary TEXT NOT NULL DEFAULT '',
    relays JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS user_status_latest (
    pubkey TEXT NOT NULL,
    d_tag TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    link TEXT NOT NULL DEFAULT '',
    expires_at BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, d_tag)
);

CREATE TABLE IF NOT EXISTS curation_sets (
    address TEXT PRIMARY KEY,
    author TEXT NOT NULL,
    kind INTEGER NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    image TEXT NOT NULL DEFAULT '',
    item_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_curation_sets_kind_updated ON curation_sets (kind, updated_at DESC);

CREATE TABLE IF NOT EXISTS curation_set_items (
    address TEXT NOT NULL REFERENCES curation_sets (address) ON DELETE CASCADE,
    item_type TEXT NOT NULL,
    item_value TEXT NOT NULL,
    PRIMARY KEY (address, item_type, item_value)
);

CREATE TABLE IF NOT EXISTS interest_lists_latest (
    pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    hashtags JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, kind)
);

CREATE TABLE IF NOT EXISTS hashtag_interest_counts (
    hashtag TEXT PRIMARY KEY,
    follower_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS relay_preference_lists_latest (
    pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    relays JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, kind)
);

CREATE TABLE IF NOT EXISTS blossom_servers_latest (
    pubkey TEXT PRIMARY KEY,
    servers JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS emoji_sets_latest (
    pubkey TEXT PRIMARY KEY,
    shortcodes JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS group_lists_latest (
    pubkey TEXT PRIMARY KEY,
    groups JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS file_metadata (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    url TEXT NOT NULL DEFAULT '',
    mime TEXT NOT NULL DEFAULT '',
    size_bytes BIGINT NOT NULL DEFAULT 0,
    dim TEXT NOT NULL DEFAULT '',
    blurhash TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS media_host_daily (
    day DATE NOT NULL,
    canonical_domain TEXT NOT NULL,
    media_count BIGINT NOT NULL DEFAULT 0,
    authors BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, canonical_domain)
);

CREATE TABLE IF NOT EXISTS media_host_author_daily (
    day DATE NOT NULL,
    canonical_domain TEXT NOT NULL,
    pubkey TEXT NOT NULL,
    PRIMARY KEY (day, canonical_domain, pubkey)
);

CREATE TABLE IF NOT EXISTS media_mime_daily (
    day DATE NOT NULL,
    mime TEXT NOT NULL,
    media_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, mime)
);

ALTER TABLE relay_registry
    ADD COLUMN IF NOT EXISTS search_ref_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS blocked_ref_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS favorite_ref_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS dm_ref_count BIGINT NOT NULL DEFAULT 0;

INSERT INTO untrusted_purge_kind_cursor (kind)
VALUES
    (8), (9), (11), (1063), (1068), (1985), (9041),
    (10006), (10007), (10009), (10012), (10015), (10030), (10063),
    (30002), (30004), (30005), (30008), (30009), (30015), (30315)
ON CONFLICT (kind) DO NOTHING;

CREATE OR REPLACE FUNCTION enqueue_superseded_replaceable(
    p_pubkey TEXT,
    p_kind INTEGER,
    p_d_tag TEXT
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    winner_id TEXT;
    winner_created BIGINT;
BEGIN
    IF p_kind NOT IN (
        0, 3, 10000, 10001, 10002, 10003, 10006, 10007, 10009, 10012, 10015, 10019, 10030, 10050, 10063,
        30000, 30002, 30004, 30005, 30008, 30009, 30015, 30023, 30311, 30315, 34235, 34236,
        39000, 39001, 39002, 39003
    ) THEN
        RETURN;
    END IF;

    SELECT event_id, created_at
      INTO winner_id, winner_created
      FROM replaceable_state
     WHERE pubkey = p_pubkey
       AND kind = p_kind
       AND d_tag = p_d_tag;
    IF winner_id IS NULL THEN
        RETURN;
    END IF;

    DELETE FROM replaceable_event_purge WHERE event_id = winner_id;

    INSERT INTO replaceable_event_purge (event_id, created_at)
    SELECT e.id, e.created_at
    FROM events e
    WHERE e.pubkey = p_pubkey
      AND e.kind = p_kind
      AND e.id <> winner_id
      AND (
          e.created_at < winner_created
          OR (e.created_at = winner_created AND e.id < winner_id)
      )
      AND (
          p_d_tag = ''
          OR COALESCE((
              SELECT et.value
              FROM event_tags et
              WHERE et.event_id = e.id
                AND et.tag_name = 'd'
                AND et.value_index = 0
              ORDER BY et.tag_index ASC
              LIMIT 1
          ), '') = p_d_tag
      )
    ON CONFLICT (event_id) DO NOTHING;
END;
$$;
