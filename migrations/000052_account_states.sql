-- Account lifecycle state.
--
-- NostrMash promotes accounts, not only events. This table is the durable home
-- of each pubkey's lifecycle state plus the coverage/completeness fields that
-- let the API be honest about what it knows.
--
-- Design notes:
--   * NO foreign key to events. Account state (and its coverage facts) must
--     outlive raw event retention. A pubkey can be "tracked" with durable
--     analytics long after its raw engagement events are purged.
--   * `derived_state` is computed from signals (trust hops, observation count,
--     engagement-by-trusted, profile completeness) by the worker recompute
--     loop. `manual_override`, when set, wins. `state` is the effective state
--     (override ?? derived) and is the column read by the ingest gate and the
--     API. Keeping all three lets us show why an account is where it is.
--   * Observation accounting (observed_count/last_observed_at) is the cheap,
--     counts-only substitute for a raw buffer: it records that a pubkey has
--     been seen N times (including gated events) so promotion can be
--     signal-driven without retaining raw payloads.
CREATE TABLE IF NOT EXISTS account_states (
    pubkey TEXT PRIMARY KEY,
    state TEXT NOT NULL DEFAULT 'unknown',
    derived_state TEXT NOT NULL DEFAULT 'unknown',
    manual_override TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_tracked_at TIMESTAMPTZ,
    last_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    observed_count BIGINT NOT NULL DEFAULT 0,
    last_hydrated_at TIMESTAMPTZ,
    last_successful_hydration_at TIMESTAMPTZ,
    oldest_known_note_at TIMESTAMPTZ,
    newest_known_note_at TIMESTAMPTZ,
    engagement_last_checked_at TIMESTAMPTZ,
    coverage_window_days INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    derivation_version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT account_states_state_valid CHECK (
        state IN ('unknown','observed','candidate','meaningful','trusted','tracked','strategic','blocked')
    ),
    CONSTRAINT account_states_derived_state_valid CHECK (
        derived_state IN ('unknown','observed','candidate','meaningful','trusted','tracked','strategic','blocked')
    ),
    CONSTRAINT account_states_manual_override_valid CHECK (
        manual_override IS NULL OR manual_override IN ('unknown','observed','candidate','meaningful','trusted','tracked','strategic','blocked')
    )
);

CREATE INDEX IF NOT EXISTS idx_account_states_state ON account_states (state);
CREATE INDEX IF NOT EXISTS idx_account_states_state_updated_at ON account_states (state, updated_at DESC);

-- Append-only audit of state transitions. Bounded by a small retention loop;
-- safe to prune (operational, not canonical).
CREATE TABLE IF NOT EXISTS account_state_transitions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pubkey TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    reason TEXT,
    source TEXT NOT NULL DEFAULT 'derived',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_account_state_transitions_pubkey ON account_state_transitions (pubkey, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_account_state_transitions_created_at ON account_state_transitions (created_at);
