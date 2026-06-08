package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// AccountStateRow is the full persisted account-state record, including
// coverage/completeness fields.
type AccountStateRow struct {
	Pubkey                    string
	State                     string
	DerivedState              string
	ManualOverride            *string
	FirstSeenAt               time.Time
	FirstTrackedAt            *time.Time
	LastObservedAt            time.Time
	ObservedCount             int64
	LastHydratedAt            *time.Time
	LastSuccessfulHydrationAt *time.Time
	OldestKnownNoteAt         *time.Time
	NewestKnownNoteAt         *time.Time
	EngagementLastCheckedAt   *time.Time
	CoverageWindowDays        *int
	UpdatedAt                 time.Time
	DerivationVersion         int64
	Exists                    bool
}

// AccountSignalRow carries the cheap signals used to recompute an account's
// derived state, plus the current persisted state for change detection.
type AccountSignalRow struct {
	Pubkey         string
	TrustHops      int // -1 when not in trust graph
	ObservedCount  int64
	HasProfile     bool
	NoteCount      int
	CurrentState   string
	CurrentDerived string
	ManualOverride *string
	Tracked        bool // first_tracked_at IS NOT NULL
}

// BatchIncrementAccountObservations records that each pubkey was observed an
// additional N times. New pubkeys get an account_states row in the default
// 'unknown' state. This is the counts-only observation accounting that feeds
// signal-driven promotion without retaining raw payloads.
func (s *PostgresStore) BatchIncrementAccountObservations(ctx context.Context, deltas map[string]int64) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	if len(deltas) == 0 {
		return nil
	}
	pubkeys := make([]string, 0, len(deltas))
	counts := make([]int64, 0, len(deltas))
	for pubkey, delta := range deltas {
		pubkey = strings.ToLower(strings.TrimSpace(pubkey))
		if pubkey == "" || delta <= 0 {
			continue
		}
		pubkeys = append(pubkeys, pubkey)
		counts = append(counts, delta)
	}
	if len(pubkeys) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO account_states (pubkey, observed_count, last_observed_at, first_seen_at, updated_at)
		SELECT t.pubkey, t.delta, now(), now(), now()
		FROM unnest($1::text[], $2::bigint[]) AS t(pubkey, delta)
		ON CONFLICT (pubkey) DO UPDATE
		SET observed_count = account_states.observed_count + EXCLUDED.observed_count,
		    last_observed_at = now()
	`, pubkeys, counts)
	if err != nil {
		return fmt.Errorf("batch increment account observations: %w", err)
	}
	return nil
}

// ListAccountSignalsForRecompute returns a batch of account signal rows for the
// derived-state recompute loop. Rows are ordered oldest-updated first so the
// loop naturally revisits stale accounts. When staleBefore is non-zero only
// rows updated before it are returned.
func (s *PostgresStore) ListAccountSignalsForRecompute(ctx context.Context, limit int, staleBefore time.Time) ([]AccountSignalRow, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		SELECT a.pubkey,
		       COALESCE(g.min_hops, -1) AS trust_hops,
		       a.observed_count,
		       (p.pubkey IS NOT NULL) AS has_profile,
		       COALESCE(ps.note_count, 0) AS note_count,
		       a.state,
		       a.derived_state,
		       a.manual_override,
		       (a.first_tracked_at IS NOT NULL) AS tracked
		FROM account_states a
		LEFT JOIN trust_graph_snapshot g ON g.pubkey = a.pubkey
		LEFT JOIN profiles_latest p ON p.pubkey = a.pubkey
		LEFT JOIN profile_public_stats ps ON ps.pubkey = a.pubkey
		WHERE ($1::timestamptz IS NULL OR a.updated_at < $1)
		ORDER BY a.updated_at ASC
		LIMIT $2
	`, nullableTime(staleBefore), limit)
	if err != nil {
		return nil, fmt.Errorf("list account signals: %w", err)
	}
	defer rows.Close()
	out := make([]AccountSignalRow, 0, limit)
	for rows.Next() {
		var r AccountSignalRow
		if err := rows.Scan(
			&r.Pubkey,
			&r.TrustHops,
			&r.ObservedCount,
			&r.HasProfile,
			&r.NoteCount,
			&r.CurrentState,
			&r.CurrentDerived,
			&r.ManualOverride,
			&r.Tracked,
		); err != nil {
			return nil, fmt.Errorf("scan account signal row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read account signal rows: %w", err)
	}
	return out, nil
}

// ApplyAccountState writes the recomputed derived/effective state for a pubkey.
// When the effective state changes it also records an audit transition. The
// update and transition share one transaction.
func (s *PostgresStore) ApplyAccountState(
	ctx context.Context,
	pubkey string,
	fromState string,
	derived string,
	effective string,
	source string,
	reason string,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return fmt.Errorf("pubkey is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin apply account state: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE account_states
		SET derived_state = $2,
		    state = $3,
		    derivation_version = derivation_version + 1,
		    updated_at = now()
		WHERE pubkey = $1
	`, pubkey, derived, effective); err != nil {
		return fmt.Errorf("update account state: %w", err)
	}
	if effective != fromState {
		if err := insertAccountStateTransition(ctx, tx, pubkey, fromState, effective, source, reason); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit apply account state: %w", err)
	}
	return nil
}

// SetAccountManualOverride sets (or clears, when override is empty) the manual
// override for an account and recomputes the effective state. A manual override
// always wins over the derived state. Creates the row if missing.
func (s *PostgresStore) SetAccountManualOverride(ctx context.Context, pubkey string, override string, reason string) (fromState string, err error) {
	if s == nil || s.pool == nil {
		return "", fmt.Errorf("store is not initialized")
	}
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return "", fmt.Errorf("pubkey is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin set override: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var prev string
	var derived string
	err = tx.QueryRow(ctx, `SELECT state, derived_state FROM account_states WHERE pubkey = $1`, pubkey).Scan(&prev, &derived)
	if err != nil {
		if err != pgx.ErrNoRows {
			return "", fmt.Errorf("load account for override: %w", err)
		}
		prev = string("unknown")
		derived = string("unknown")
		if _, err := tx.Exec(ctx, `
			INSERT INTO account_states (pubkey, state, derived_state, first_seen_at, last_observed_at, updated_at)
			VALUES ($1, 'unknown', 'unknown', now(), now(), now())
			ON CONFLICT (pubkey) DO NOTHING
		`, pubkey); err != nil {
			return "", fmt.Errorf("insert account for override: %w", err)
		}
	}

	override = strings.ToLower(strings.TrimSpace(override))
	var effective string
	if override == "" {
		effective = derived
	} else {
		effective = override
	}
	var overrideArg interface{}
	if override == "" {
		overrideArg = nil
	} else {
		overrideArg = override
	}
	if _, err := tx.Exec(ctx, `
		UPDATE account_states
		SET manual_override = $2,
		    state = $3,
		    derivation_version = derivation_version + 1,
		    updated_at = now()
		WHERE pubkey = $1
	`, pubkey, overrideArg, effective); err != nil {
		return "", fmt.Errorf("apply override: %w", err)
	}
	if effective != prev {
		if err := insertAccountStateTransition(ctx, tx, pubkey, prev, effective, "manual", reason); err != nil {
			return "", err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit set override: %w", err)
	}
	return prev, nil
}

// PromoteAccountToTracked marks an account as tracked (sets first_tracked_at)
// and raises its effective state to at least 'tracked', unless a manual
// override is in force (in which case only first_tracked_at is set). Used by
// the hydration service so the ingest gate accepts the fetched content.
func (s *PostgresStore) PromoteAccountToTracked(ctx context.Context, pubkey string, reason string) (fromState string, err error) {
	if s == nil || s.pool == nil {
		return "", fmt.Errorf("store is not initialized")
	}
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return "", fmt.Errorf("pubkey is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin promote: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO account_states (pubkey, state, derived_state, first_seen_at, first_tracked_at, last_observed_at, updated_at)
		VALUES ($1, 'tracked', 'unknown', now(), now(), now(), now())
		ON CONFLICT (pubkey) DO NOTHING
	`, pubkey); err != nil {
		return "", fmt.Errorf("insert account for promote: %w", err)
	}

	var prev string
	var hasOverride bool
	if err := tx.QueryRow(ctx, `
		SELECT state, (manual_override IS NOT NULL) FROM account_states WHERE pubkey = $1
	`, pubkey).Scan(&prev, &hasOverride); err != nil {
		return "", fmt.Errorf("load account for promote: %w", err)
	}

	// Set first_tracked_at if not already set. Only raise the effective state
	// to 'tracked' when there is no manual override and the current rank is
	// below tracked.
	if _, err := tx.Exec(ctx, `
		UPDATE account_states
		SET first_tracked_at = COALESCE(first_tracked_at, now()),
		    state = CASE
		        WHEN manual_override IS NOT NULL THEN state
		        WHEN state IN ('tracked','strategic') THEN state
		        ELSE 'tracked'
		    END,
		    derivation_version = derivation_version + 1,
		    updated_at = now()
		WHERE pubkey = $1
	`, pubkey); err != nil {
		return "", fmt.Errorf("promote account: %w", err)
	}

	if !hasOverride && prev != "tracked" && prev != "strategic" {
		if err := insertAccountStateTransition(ctx, tx, pubkey, prev, "tracked", "hydration", reason); err != nil {
			return "", err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit promote: %w", err)
	}
	return prev, nil
}

// UpdateAccountCoverage records hydration coverage facts for an account. Nil
// pointers leave the corresponding column unchanged.
func (s *PostgresStore) UpdateAccountCoverage(
	ctx context.Context,
	pubkey string,
	lastHydratedAt *time.Time,
	lastSuccessfulHydrationAt *time.Time,
	oldestKnownNoteAt *time.Time,
	newestKnownNoteAt *time.Time,
	engagementLastCheckedAt *time.Time,
	coverageWindowDays *int,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return fmt.Errorf("pubkey is required")
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE account_states
		SET last_hydrated_at = COALESCE($2, last_hydrated_at),
		    last_successful_hydration_at = COALESCE($3, last_successful_hydration_at),
		    oldest_known_note_at = COALESCE($4, oldest_known_note_at),
		    newest_known_note_at = COALESCE($5, newest_known_note_at),
		    engagement_last_checked_at = COALESCE($6, engagement_last_checked_at),
		    coverage_window_days = COALESCE($7, coverage_window_days),
		    updated_at = now()
		WHERE pubkey = $1
	`, pubkey, lastHydratedAt, lastSuccessfulHydrationAt, oldestKnownNoteAt, newestKnownNoteAt, engagementLastCheckedAt, coverageWindowDays)
	if err != nil {
		return fmt.Errorf("update account coverage: %w", err)
	}
	return nil
}

// GetAccountState returns the full account-state row. When the pubkey has no
// row, Exists is false and the rest is zero-valued.
func (s *PostgresStore) GetAccountState(ctx context.Context, pubkey string) (AccountStateRow, error) {
	if s == nil || s.pool == nil {
		return AccountStateRow{}, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return AccountStateRow{}, fmt.Errorf("pubkey is required")
	}
	var r AccountStateRow
	err := s.pool.QueryRow(ctx, `
		SELECT pubkey, state, derived_state, manual_override, first_seen_at, first_tracked_at,
		       last_observed_at, observed_count, last_hydrated_at, last_successful_hydration_at,
		       oldest_known_note_at, newest_known_note_at, engagement_last_checked_at,
		       coverage_window_days, updated_at, derivation_version
		FROM account_states
		WHERE pubkey = $1
	`, pubkey).Scan(
		&r.Pubkey, &r.State, &r.DerivedState, &r.ManualOverride, &r.FirstSeenAt, &r.FirstTrackedAt,
		&r.LastObservedAt, &r.ObservedCount, &r.LastHydratedAt, &r.LastSuccessfulHydrationAt,
		&r.OldestKnownNoteAt, &r.NewestKnownNoteAt, &r.EngagementLastCheckedAt,
		&r.CoverageWindowDays, &r.UpdatedAt, &r.DerivationVersion,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return AccountStateRow{Pubkey: pubkey, State: "unknown", DerivedState: "unknown", Exists: false}, nil
		}
		return AccountStateRow{}, fmt.Errorf("get account state: %w", err)
	}
	r.Exists = true
	return r, nil
}

// LoadIngestAcceptPubkeys returns pubkeys whose account state means the ingest
// gate should accept their authored content (tracked/strategic), in addition to
// graph-trusted authors.
func (s *PostgresStore) LoadIngestAcceptPubkeys(ctx context.Context) ([]string, error) {
	return s.loadPubkeysByStates(ctx, []string{"tracked", "strategic"})
}

// LoadBlockedPubkeys returns pubkeys explicitly blocked. The ingest gate drops
// all kinds from these authors.
func (s *PostgresStore) LoadBlockedPubkeys(ctx context.Context) ([]string, error) {
	return s.loadPubkeysByStates(ctx, []string{"blocked"})
}

func (s *PostgresStore) loadPubkeysByStates(ctx context.Context, states []string) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	rows, err := s.pool.Query(ctx, `SELECT pubkey FROM account_states WHERE state = ANY($1)`, states)
	if err != nil {
		return nil, fmt.Errorf("load pubkeys by state: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, 256)
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return nil, fmt.Errorf("scan pubkey by state: %w", err)
		}
		out = append(out, pubkey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pubkeys by state: %w", err)
	}
	return out, nil
}

// CountAccountStates returns the number of accounts in each state for metrics.
func (s *PostgresStore) CountAccountStates(ctx context.Context) (map[string]int64, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	rows, err := s.pool.Query(ctx, `SELECT state, COUNT(*) FROM account_states GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("count account states: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int64, 8)
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("scan account state count: %w", err)
		}
		out[state] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read account state counts: %w", err)
	}
	return out, nil
}

// PurgeAccountStateTransitionsOlderThan deletes audit rows older than cutoff,
// bounded by limit. Operational retention; canonical account state is untouched.
func (s *PostgresStore) PurgeAccountStateTransitionsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 5000
	}
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM account_state_transitions
		WHERE id IN (
			SELECT id FROM account_state_transitions
			WHERE created_at < $1
			ORDER BY id
			LIMIT $2
		)
	`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("purge account state transitions: %w", err)
	}
	return tag.RowsAffected(), nil
}

func insertAccountStateTransition(ctx context.Context, tx pgx.Tx, pubkey, fromState, toState, source, reason string) error {
	var reasonArg interface{}
	if strings.TrimSpace(reason) == "" {
		reasonArg = nil
	} else {
		reasonArg = reason
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO account_state_transitions (pubkey, from_state, to_state, reason, source)
		VALUES ($1, $2, $3, $4, $5)
	`, pubkey, fromState, toState, reasonArg, source); err != nil {
		return fmt.Errorf("insert account state transition: %w", err)
	}
	return nil
}

func nullableTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}
