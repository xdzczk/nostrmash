-- Account-state bounded-context queries for the sqlc pilot.
--
-- Scope: the single-table account_states / account_state_transitions
-- statements. The multi-context ListAccountSignalsForRecompute join (into the
-- trust and profile projections) is intentionally out of the pilot: it reaches
-- across bounded contexts and stays hand-written in account.go.
--
-- The exported Accounts methods remain the public surface and keep owning
-- transaction orchestration; they call these generated statements through a
-- *Queries bound to either the pool or an in-flight pgx.Tx.

-- name: ApplyAccountStateUpdate :exec
UPDATE account_states
SET derived_state = @derived_state,
    state = @effective_state,
    derivation_version = derivation_version + 1,
    updated_at = now()
WHERE pubkey = @pubkey;

-- name: GetAccountStateForOverride :one
SELECT state, derived_state
FROM account_states
WHERE pubkey = @pubkey;

-- name: InsertUnknownAccountState :exec
INSERT INTO account_states (pubkey, state, derived_state, first_seen_at, last_observed_at, updated_at)
VALUES (@pubkey, 'unknown', 'unknown', now(), now(), now())
ON CONFLICT (pubkey) DO NOTHING;

-- name: ApplyAccountManualOverride :exec
UPDATE account_states
SET manual_override = @manual_override,
    state = @effective_state,
    derivation_version = derivation_version + 1,
    updated_at = now()
WHERE pubkey = @pubkey;

-- name: InsertTrackedAccountState :exec
INSERT INTO account_states (pubkey, state, derived_state, first_seen_at, first_tracked_at, last_observed_at, updated_at)
VALUES (@pubkey, 'tracked', 'unknown', now(), now(), now(), now())
ON CONFLICT (pubkey) DO NOTHING;

-- name: GetAccountStateForPromote :one
SELECT state, (manual_override IS NOT NULL)::bool AS has_override
FROM account_states
WHERE pubkey = @pubkey;

-- name: PromoteAccountToTracked :exec
UPDATE account_states
SET first_tracked_at = COALESCE(first_tracked_at, now()),
    state = CASE
        WHEN manual_override IS NOT NULL THEN state
        WHEN state IN ('tracked','strategic') THEN state
        ELSE 'tracked'
    END,
    derivation_version = derivation_version + 1,
    updated_at = now()
WHERE pubkey = @pubkey;

-- name: UpdateAccountCoverage :exec
UPDATE account_states
SET last_hydrated_at = COALESCE(@last_hydrated_at, last_hydrated_at),
    last_successful_hydration_at = COALESCE(@last_successful_hydration_at, last_successful_hydration_at),
    oldest_known_note_at = COALESCE(@oldest_known_note_at, oldest_known_note_at),
    newest_known_note_at = COALESCE(@newest_known_note_at, newest_known_note_at),
    engagement_last_checked_at = COALESCE(@engagement_last_checked_at, engagement_last_checked_at),
    coverage_window_days = COALESCE(@coverage_window_days, coverage_window_days),
    updated_at = now()
WHERE pubkey = @pubkey;

-- name: GetAccountState :one
SELECT pubkey, state, derived_state, manual_override, first_seen_at, first_tracked_at,
       last_observed_at, observed_count, last_hydrated_at, last_successful_hydration_at,
       oldest_known_note_at, newest_known_note_at, engagement_last_checked_at,
       coverage_window_days, updated_at, derivation_version
FROM account_states
WHERE pubkey = @pubkey;

-- name: LoadPubkeysByStates :many
SELECT pubkey
FROM account_states
WHERE state = ANY(@states::text[]);

-- name: CountAccountStates :many
SELECT state, COUNT(*) AS count
FROM account_states
GROUP BY state;

-- name: InsertAccountStateTransition :exec
INSERT INTO account_state_transitions (pubkey, from_state, to_state, reason, source)
VALUES (@pubkey, @from_state, @to_state, @reason, @source);

-- name: PurgeAccountStateTransitionsOlderThan :execrows
DELETE FROM account_state_transitions
WHERE id IN (
    SELECT t.id FROM account_state_transitions t
    WHERE t.created_at < @cutoff
    ORDER BY t.id
    LIMIT @row_limit
);
