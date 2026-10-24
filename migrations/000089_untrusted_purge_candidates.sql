-- Candidate index for untrusted-author retention.
--
-- PurgeUntrustedAuthorEvents used to anti-join trust_graph_snapshot against
-- every aged event on each tick. In production that plan walked ~2M index
-- entries (merge anti-join + sort) to find on the order of a thousand
-- deletable rows, ran ~13 minutes, and was cancelled by statement_timeout.
-- Cost scaled with events, which is the failure mode retention-query-cost.mdc
-- exists to prevent.
--
-- These tables are the bounded side of that predicate. The worker pages
-- events by (kind, first_seen_at, id) — an index range on
-- idx_events_kind_first_seen — and records only the untrusted, non-reply
-- rows. first_seen_at only moves forward, so a cursor on created_at would
-- miss a historical event inserted after the cursor passed its created_at.
-- Steady-state ticks touch the newly-aged window and authors who left the
-- trust snapshot, not the historical table. The purge itself then deletes
-- from untrusted_purge_candidates.

CREATE TABLE IF NOT EXISTS untrusted_purge_candidates (
    event_id TEXT PRIMARY KEY,
    pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    created_at BIGINT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_untrusted_purge_candidates_created
    ON untrusted_purge_candidates (created_at ASC, event_id ASC);

CREATE INDEX IF NOT EXISTS idx_untrusted_purge_candidates_pubkey
    ON untrusted_purge_candidates (pubkey);

-- Events whose author is untrusted but that are not yet past the age horizon
-- (typically first_seen_at). Promoted into untrusted_purge_candidates once
-- eligible_at is reached, so the kind cursor never has to rescan them.
CREATE TABLE IF NOT EXISTS untrusted_purge_deferred (
    event_id TEXT PRIMARY KEY,
    pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    created_at BIGINT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    eligible_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_untrusted_purge_deferred_eligible
    ON untrusted_purge_deferred (eligible_at ASC, event_id ASC);

-- Mirror of trust_graph_snapshot.pubkeys from the previous sync. The set
-- difference is the authors who left the graph and whose already-scanned
-- events must be enqueued. It is not used as a source of truth for who is
-- trusted; an empty trust_graph_snapshot still deletes nothing.
CREATE TABLE IF NOT EXISTS untrusted_purge_trusted_pubkeys (
    pubkey TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS untrusted_purge_author_resync (
    pubkey TEXT PRIMARY KEY,
    cursor_created_at BIGINT NOT NULL DEFAULT -1,
    cursor_event_id TEXT NOT NULL DEFAULT ''
);

-- Per-kind keyset cursor over first_seen_at. Walking created_at misses an
-- event inserted after the cursor has passed its created_at (historical
-- backfill). first_seen_at only moves forward, so a caught-up cursor plus
-- the next horizon only reads events that aged in since the previous tick.
-- The epoch default means "not started". first_seen_at is ingest time, so
-- no real row sorts before it. '-infinity' is not used: the sync scans the
-- cursor into time.Time, and pgx rejects infinity.
CREATE TABLE IF NOT EXISTS untrusted_purge_kind_cursor (
    kind INTEGER PRIMARY KEY,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    event_id TEXT NOT NULL DEFAULT ''
);

INSERT INTO untrusted_purge_kind_cursor (kind)
VALUES (1), (4), (5), (9802), (10000), (10003), (30023)
ON CONFLICT (kind) DO NOTHING;

-- Supports the per-kind page: kind = $1 AND first_seen_at in (cursor, horizon)
-- ORDER BY first_seen_at, id. Built CONCURRENTLY on the production hot table
-- before this migration ships; IF NOT EXISTS makes that deploy a no-op.
-- Fresh databases build it here. Raise the cluster statement_timeout so the
-- build is not killed.
SET LOCAL statement_timeout = '30min';

CREATE INDEX IF NOT EXISTS idx_events_kind_first_seen
    ON events (kind, first_seen_at ASC, id ASC);
