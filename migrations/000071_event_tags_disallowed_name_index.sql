-- event_tags junk-tag-name prune cost hardening.
--
-- Background
-- ----------
-- PruneFilteredEventTags's "tag_name outside the allowlist" branch had no
-- supporting index, so proving "no more junk names remain" cost a full
-- table scan (128GB / 35M+ rows) every tick, forever — confirmed on
-- production: even a plain COUNT(*) for the sibling kind-scoped branches
-- timed out at 120s. At the default 5-minute run interval this burned
-- continuous IO for a table that is 99%+ likely to already be drained.
--
-- ingest (internal/eventtags.ShouldPersist) already refuses to write
-- non-allowlisted tag names, so this index only ever holds legacy rows
-- written before that filter existed. Once the historical backlog drains,
-- the index stays empty and the prune query costs an index probe instead
-- of a table scan.
--
-- The predicate is a literal copy of internal/eventtags.AllowedTagNames,
-- not a parameter: Postgres can only prove a partial index satisfies a
-- "NOT IN (...)" predicate when the literal list at query time is
-- identical to the index's literal predicate. Keep both, plus the
-- PruneFilteredEventTagsDisallowedNames query, in sync — see
-- TestAllowedTagNamesMatchesDisallowedNameQuery.
--
-- Production built this with CREATE INDEX CONCURRENTLY before this
-- migration shipped; IF NOT EXISTS keeps redeploys idempotent.
CREATE INDEX IF NOT EXISTS idx_event_tags_disallowed_tag_name
    ON event_tags (tag_name)
    WHERE tag_name NOT IN (
        'a', 'd', 'e', 'g', 'group', 'image', 'imeta', 'm', 'p', 'r',
        'series', 't', 'thumb', 'u', 'url', 'video', 'word'
    );
