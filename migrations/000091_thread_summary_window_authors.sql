-- Distinct authors active in each hot-conversation window: the root, plus
-- everyone else who replied inside the window. Lifetime participant_count
-- stays the all-time headcount. Ranking uses these columns once a row is
-- stamped with thread summary derivation v4; older rows fall back to
-- participant_count so a small group cannot keep ranking on raw reply volume.
ALTER TABLE thread_summaries
    ADD COLUMN IF NOT EXISTS authors_24h INTEGER NOT NULL DEFAULT 0 CHECK (authors_24h >= 0),
    ADD COLUMN IF NOT EXISTS authors_7d INTEGER NOT NULL DEFAULT 0 CHECK (authors_7d >= 0);
