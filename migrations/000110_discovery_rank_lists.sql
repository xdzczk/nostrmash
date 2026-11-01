-- Rank lists that are not the trending score.
--
-- bookmark_edges is the current public save graph: kind 10003 lists and
-- kind 30003 bookmark sets. One person counts once per note, even if they
-- save it in more than one list. note_bookmark_stats is the rank row.
--
-- note_zap_window_stats and profile_support_stats are rewritten every few
-- minutes by the worker (RefreshPaidRankings). Support rank caps each
-- sender at 100000 sats inside the window so one whale cannot own the list.
--
-- Production statement_timeout is 15s. The bookmark backfill joins the
-- current replaceable winners, not the events heap.
SET LOCAL statement_timeout = 0;

CREATE TABLE IF NOT EXISTS bookmark_edges (
    bookmarker_pubkey TEXT NOT NULL,
    target_event_id TEXT NOT NULL DEFAULT '',
    target_address TEXT NOT NULL DEFAULT '',
    source_kind INTEGER NOT NULL,
    source_d_tag TEXT NOT NULL DEFAULT '',
    list_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    list_created_at BIGINT NOT NULL,
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bookmarker_pubkey, target_event_id, target_address, source_kind, source_d_tag),
    CHECK (target_event_id <> '' OR target_address <> '')
);

CREATE INDEX IF NOT EXISTS idx_bookmark_edges_event
    ON bookmark_edges (target_event_id)
    WHERE target_event_id <> '';

CREATE INDEX IF NOT EXISTS idx_bookmark_edges_address
    ON bookmark_edges (target_address)
    WHERE target_address <> '';

CREATE INDEX IF NOT EXISTS idx_bookmark_edges_source
    ON bookmark_edges (bookmarker_pubkey, source_kind, source_d_tag);

CREATE TABLE IF NOT EXISTS note_bookmark_stats (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    author_pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    bookmark_count BIGINT NOT NULL CHECK (bookmark_count >= 0),
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_note_bookmark_stats_rank
    ON note_bookmark_stats (bookmark_count DESC, event_id ASC)
    WHERE bookmark_count > 0;

CREATE TABLE IF NOT EXISTS note_zap_window_stats (
    event_id TEXT PRIMARY KEY,
    author_pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    sats_24h BIGINT NOT NULL DEFAULT 0 CHECK (sats_24h >= 0),
    zap_count_24h BIGINT NOT NULL DEFAULT 0 CHECK (zap_count_24h >= 0),
    sats_7d BIGINT NOT NULL DEFAULT 0 CHECK (sats_7d >= 0),
    zap_count_7d BIGINT NOT NULL DEFAULT 0 CHECK (zap_count_7d >= 0),
    sats_30d BIGINT NOT NULL DEFAULT 0 CHECK (sats_30d >= 0),
    zap_count_30d BIGINT NOT NULL DEFAULT 0 CHECK (zap_count_30d >= 0),
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_note_zap_window_sats_24h
    ON note_zap_window_stats (sats_24h DESC, event_id ASC)
    WHERE sats_24h > 0;

CREATE INDEX IF NOT EXISTS idx_note_zap_window_sats_7d
    ON note_zap_window_stats (sats_7d DESC, event_id ASC)
    WHERE sats_7d > 0;

CREATE INDEX IF NOT EXISTS idx_note_zap_window_sats_30d
    ON note_zap_window_stats (sats_30d DESC, event_id ASC)
    WHERE sats_30d > 0;

CREATE TABLE IF NOT EXISTS profile_support_stats (
    pubkey TEXT PRIMARY KEY,
    capped_sats_24h BIGINT NOT NULL DEFAULT 0 CHECK (capped_sats_24h >= 0),
    raw_sats_24h BIGINT NOT NULL DEFAULT 0 CHECK (raw_sats_24h >= 0),
    supporters_24h BIGINT NOT NULL DEFAULT 0 CHECK (supporters_24h >= 0),
    capped_sats_7d BIGINT NOT NULL DEFAULT 0 CHECK (capped_sats_7d >= 0),
    raw_sats_7d BIGINT NOT NULL DEFAULT 0 CHECK (raw_sats_7d >= 0),
    supporters_7d BIGINT NOT NULL DEFAULT 0 CHECK (supporters_7d >= 0),
    capped_sats_30d BIGINT NOT NULL DEFAULT 0 CHECK (capped_sats_30d >= 0),
    raw_sats_30d BIGINT NOT NULL DEFAULT 0 CHECK (raw_sats_30d >= 0),
    supporters_30d BIGINT NOT NULL DEFAULT 0 CHECK (supporters_30d >= 0),
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_profile_support_stats_30d
    ON profile_support_stats (capped_sats_30d DESC, supporters_30d DESC, pubkey ASC)
    WHERE capped_sats_30d > 0;

CREATE INDEX IF NOT EXISTS idx_profile_support_stats_7d
    ON profile_support_stats (capped_sats_7d DESC, supporters_7d DESC, pubkey ASC)
    WHERE capped_sats_7d > 0;

CREATE INDEX IF NOT EXISTS idx_profile_support_stats_24h
    ON profile_support_stats (capped_sats_24h DESC, supporters_24h DESC, pubkey ASC)
    WHERE capped_sats_24h > 0;

-- Leaderboard of every account with at least one follower. The >= 1000
-- partial index stays for the ingest frontier; this one serves the public
-- most-followed page, including accounts under that floor.
CREATE INDEX IF NOT EXISTS idx_profile_public_stats_follower_rank
    ON profile_public_stats (follower_count DESC, pubkey ASC)
    WHERE follower_count > 0;

CREATE INDEX IF NOT EXISTS idx_zap_receipts_created_amount
    ON zap_receipts (created_at DESC)
    WHERE amount_sats > 0;

INSERT INTO bookmark_edges (
    bookmarker_pubkey,
    target_event_id,
    target_address,
    source_kind,
    source_d_tag,
    list_event_id,
    list_created_at,
    derivation_version
)
SELECT DISTINCT
    rs.pubkey,
    CASE WHEN et.tag_name = 'e' THEN lower(et.value) ELSE '' END,
    CASE
        WHEN et.tag_name = 'a' THEN
            '30023:' || lower(split_part(et.value, ':', 2)) || ':' ||
            COALESCE(substring(et.value FROM '^30023:[0-9a-fA-F]{64}:(.*)$'), '')
        ELSE ''
    END,
    rs.kind,
    rs.d_tag,
    rs.event_id,
    rs.created_at,
    1
FROM replaceable_state rs
JOIN event_tags et
  ON et.event_id = rs.event_id
 AND et.value_index = 0
 AND et.tag_name IN ('e', 'a')
WHERE rs.kind IN (10003, 30003)
  AND (
    (et.tag_name = 'e' AND lower(et.value) ~ '^[0-9a-f]{64}$')
    OR (et.tag_name = 'a' AND et.value ~* '^30023:[0-9a-f]{64}:.+')
  );

INSERT INTO note_bookmark_stats (
    event_id, author_pubkey, kind, bookmark_count, derivation_version
)
SELECT e.id,
       e.pubkey,
       e.kind,
       COUNT(DISTINCT r.bookmarker_pubkey),
       1
FROM (
    SELECT be.bookmarker_pubkey, be.target_event_id AS event_id
    FROM bookmark_edges be
    WHERE be.target_event_id <> ''
    UNION
    SELECT be.bookmarker_pubkey, rs.event_id
    FROM bookmark_edges be
    JOIN replaceable_state rs
      ON rs.kind = 30023
     AND be.target_address = '30023:' || lower(rs.pubkey) || ':' || rs.d_tag
    WHERE be.target_address <> ''
) AS r
JOIN events e ON e.id = r.event_id
WHERE e.kind IN (1, 30023)
  AND r.bookmarker_pubkey <> e.pubkey
GROUP BY e.id, e.pubkey, e.kind
HAVING COUNT(DISTINCT r.bookmarker_pubkey) > 0;
