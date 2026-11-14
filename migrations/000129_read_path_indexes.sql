-- Cheap indexes and a stored hot-conversation rank.
--
-- bookmark_edges.list_event_id is the foreign key events deletes cascade
-- through. Without an index, every event delete seq-scans the table.
--
-- trust_graph_snapshot is read for MAX(refreshed_at) on discovery requests.
-- A refreshed_at index makes that one index lookup.
--
-- Hot conversations ordered by a CASE expression over thread_summaries, so
-- the reply_weight indexes could not serve the sort. thread_summary_velocity
-- is that same expression, stored on the row and indexed. The read walks the
-- index instead of scoring every active thread.
--
-- search_documents lost its GIN index when Meilisearch became the primary
-- search path. The Postgres fallback still evaluates it, and a leading-wildcard
-- OR forced a sequential scan. The fallback query is now a union of the
-- tsvector match and prefix matches, which these indexes can serve.
--
-- The builds take write locks. The cluster statement_timeout would kill them.
SET LOCAL statement_timeout = 0;

CREATE INDEX IF NOT EXISTS idx_bookmark_edges_list_event
    ON bookmark_edges (list_event_id);

CREATE INDEX IF NOT EXISTS idx_trust_graph_snapshot_refreshed_at
    ON trust_graph_snapshot (refreshed_at DESC);

CREATE OR REPLACE FUNCTION thread_summary_velocity(
    p_version integer,
    p_participants integer,
    p_replies bigint,
    p_weight double precision,
    p_authors integer,
    p_author_version integer
) RETURNS double precision
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN author_n <= 1 THEN 0::double precision
        WHEN author_n <= 5 THEN
            CASE
                WHEN reply_n <= 0 THEN 0::double precision
                ELSE (author_n - 1)::double precision * LEAST(
                    1.0::double precision,
                    (8.0::double precision * author_n::double precision)
                        / GREATEST(reply_n, 1::double precision)
                )
            END
        ELSE base + LEAST(base, 50.0::double precision) * 0.15::double precision
    END
    FROM (
        SELECT
            CASE
                WHEN COALESCE(p_version, 0) >= p_author_version THEN COALESCE(p_authors, 0)
                ELSE COALESCE(p_participants, 1)
            END AS author_n,
            COALESCE(p_replies, 0)::double precision AS reply_n,
            CASE
                WHEN COALESCE(p_version, 0) >= 2 THEN COALESCE(p_weight, 0)
                ELSE COALESCE(p_replies, 0)::double precision
            END AS base
    ) inputs;
$$;

-- Nullable first so a deploy can fill existing rows outside this transaction.
-- A single UPDATE of every thread_summaries row holds those row locks until
-- the surrounding migration transaction commits, which is the rest of this
-- file. Rows that are already filled are left alone.
ALTER TABLE thread_summaries
    ADD COLUMN IF NOT EXISTS velocity_24h double precision,
    ADD COLUMN IF NOT EXISTS velocity_7d double precision,
    ADD COLUMN IF NOT EXISTS velocity_30d double precision;

UPDATE thread_summaries
SET velocity_24h = thread_summary_velocity(derivation_version, participant_count, replies_24h, reply_weight_24h, authors_24h, 4),
    velocity_7d = thread_summary_velocity(derivation_version, participant_count, replies_7d, reply_weight_7d, authors_7d, 4),
    velocity_30d = thread_summary_velocity(derivation_version, participant_count, replies_30d, reply_weight_30d, authors_30d, 5)
WHERE velocity_24h IS NULL
   OR velocity_7d IS NULL
   OR velocity_30d IS NULL;

ALTER TABLE thread_summaries
    ALTER COLUMN velocity_24h SET DEFAULT 0,
    ALTER COLUMN velocity_7d SET DEFAULT 0,
    ALTER COLUMN velocity_30d SET DEFAULT 0;

-- Skip the table scan when a pre-deploy backfill already marked the columns
-- NOT NULL. SET NOT NULL takes an exclusive lock for the whole check.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'thread_summaries'
          AND column_name = 'velocity_24h'
          AND is_nullable = 'YES'
    ) THEN
        ALTER TABLE thread_summaries ALTER COLUMN velocity_24h SET NOT NULL;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'thread_summaries'
          AND column_name = 'velocity_7d'
          AND is_nullable = 'YES'
    ) THEN
        ALTER TABLE thread_summaries ALTER COLUMN velocity_7d SET NOT NULL;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'thread_summaries'
          AND column_name = 'velocity_30d'
          AND is_nullable = 'YES'
    ) THEN
        ALTER TABLE thread_summaries ALTER COLUMN velocity_30d SET NOT NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_thread_summaries_velocity_24h
    ON thread_summaries (velocity_24h DESC, last_activity_at DESC, root_event_id ASC)
    WHERE velocity_24h > 0;

CREATE INDEX IF NOT EXISTS idx_thread_summaries_velocity_7d
    ON thread_summaries (velocity_7d DESC, last_activity_at DESC, root_event_id ASC)
    WHERE velocity_7d > 0;

CREATE INDEX IF NOT EXISTS idx_thread_summaries_velocity_30d
    ON thread_summaries (velocity_30d DESC, last_activity_at DESC, root_event_id ASC)
    WHERE velocity_30d > 0;

CREATE INDEX IF NOT EXISTS idx_search_documents_search_tsv
    ON search_documents USING gin (search_tsv);

CREATE INDEX IF NOT EXISTS idx_search_documents_entity_id_lower
    ON search_documents (lower(entity_id) text_pattern_ops);

-- A plain lower(title) index cannot be built: a few titles are longer than
-- the btree entry limit. The prefix lookup only needs the leading characters.
CREATE INDEX IF NOT EXISTS idx_search_documents_title_lower
    ON search_documents (left(lower(title), 200) text_pattern_ops);
