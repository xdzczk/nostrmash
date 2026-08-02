package relayregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store provides persistence for the relay registry, probe observations,
// and desired set snapshots. It owns no orchestration logic.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// UpsertSeedRelay inserts or updates a configured bootstrap seed.
// Seeds are competitive floor entries (source_seed=true, active, no pin),
// not permanent pins — admission scoring and caps may demote them. Operator
// blocked/drained policies and source_manual pins are preserved. Legacy
// seed-derived pins (pinned without source_manual) are cleared to active/none
// so free competition can take over on the next refresh.
func (s *Store) UpsertSeedRelay(ctx context.Context, urlKey, normalizedURL string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO relay_registry (
			url_key, normalized_url, source_seed, manual_policy, admission_state
		) VALUES ($1, $2, TRUE, 'none', 'active')
		ON CONFLICT (url_key) DO UPDATE
		SET source_seed = TRUE,
		    normalized_url = EXCLUDED.normalized_url,
		    last_seen_at = now(),
		    updated_at = now(),
		    manual_policy = CASE
		        WHEN relay_registry.manual_policy IN ('blocked', 'drained')
		            THEN relay_registry.manual_policy
		        WHEN relay_registry.source_manual
		             AND relay_registry.manual_policy = 'pinned'
		            THEN relay_registry.manual_policy
		        ELSE 'none'
		    END,
		    admission_state = CASE
		        WHEN relay_registry.manual_policy IN ('blocked', 'drained')
		            THEN relay_registry.admission_state
		        WHEN relay_registry.source_manual
		             AND relay_registry.manual_policy = 'pinned'
		            THEN relay_registry.admission_state
		        WHEN relay_registry.admission_state = 'pinned'
		            THEN 'active'
		        ELSE relay_registry.admission_state
		    END
	`, urlKey, normalizedURL)
	if err != nil {
		return fmt.Errorf("upsert seed relay: %w", err)
	}
	return nil
}

// ClearMissingSeedRelays makes the keep set authoritative for source_seed:
// any row still marked source_seed whose url_key is not in keepURLKeys loses
// the seed flag. Legacy seed-derived pins are cleared (manual_policy pinned →
// none, admission_state pinned → inactive). Operator blocked/drained policies
// and source_manual pins are preserved. An empty keep set clears every
// source_seed row.
//
// Returns the number of rows updated.
func (s *Store) ClearMissingSeedRelays(ctx context.Context, keepURLKeys []string) (int64, error) {
	if keepURLKeys == nil {
		keepURLKeys = []string{}
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE relay_registry
		SET source_seed = FALSE,
		    manual_policy = CASE
		        WHEN source_manual AND manual_policy = 'pinned' THEN manual_policy
		        WHEN manual_policy = 'pinned' THEN 'none'
		        ELSE manual_policy
		    END,
		    admission_state = CASE
		        WHEN source_manual AND manual_policy = 'pinned' THEN admission_state
		        WHEN admission_state = 'pinned' THEN 'inactive'
		        ELSE admission_state
		    END,
		    updated_at = now()
		WHERE source_seed = TRUE
		  AND url_key <> ALL($1::text[])
	`, keepURLKeys)
	if err != nil {
		return 0, fmt.Errorf("clear missing seed relays: %w", err)
	}
	return tag.RowsAffected(), nil
}

// EnsureRelayExists inserts a candidate registry row when missing. It does not
// mark the relay as a seed or apply a pin; use SetManualPolicy for operator
// overrides and UpsertSeedRelay for configured bootstrap seeds.
func (s *Store) EnsureRelayExists(ctx context.Context, urlKey, normalizedURL string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO relay_registry (
			url_key, normalized_url, source_seed, manual_policy, admission_state
		) VALUES ($1, $2, FALSE, 'none', 'candidate')
		ON CONFLICT (url_key) DO UPDATE
		SET normalized_url = EXCLUDED.normalized_url,
		    last_seen_at = now(),
		    updated_at = now()
	`, urlKey, normalizedURL)
	if err != nil {
		return fmt.Errorf("ensure relay exists: %w", err)
	}
	return nil
}

// ListURLKeys returns the set of url_key values currently in the registry.
func (s *Store) ListURLKeys(ctx context.Context) (map[string]struct{}, error) {
	rows, err := s.pool.Query(ctx, `SELECT url_key FROM relay_registry`)
	if err != nil {
		return nil, fmt.Errorf("list relay registry url keys: %w", err)
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var urlKey string
		if err := rows.Scan(&urlKey); err != nil {
			return nil, fmt.Errorf("scan relay registry url key: %w", err)
		}
		out[urlKey] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read relay registry url keys: %w", err)
	}
	return out, nil
}

// UpsertDiscoveredRelay inserts or updates a candidate relay discovered from user relay lists.
func (s *Store) UpsertDiscoveredRelay(
	ctx context.Context,
	urlKey, normalizedURL string,
	distinctUserRefCount int,
	weightedScore float64,
) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO relay_registry (
			url_key, normalized_url, source_user_list,
			distinct_user_ref_count, weighted_user_ref_score
		) VALUES ($1, $2, TRUE, $3, $4)
		ON CONFLICT (url_key) DO UPDATE
		SET source_user_list = TRUE,
		    distinct_user_ref_count = EXCLUDED.distinct_user_ref_count,
		    weighted_user_ref_score = EXCLUDED.weighted_user_ref_score,
		    last_seen_at = now(),
		    updated_at = now()
	`, urlKey, normalizedURL, distinctUserRefCount, weightedScore)
	if err != nil {
		return fmt.Errorf("upsert discovered relay: %w", err)
	}
	return nil
}

// SetManualPolicy applies an operator override to a relay and marks
// source_manual so seed reconciliation cannot overwrite a real ops pin.
// Clearing the policy (none) also clears source_manual.
func (s *Store) SetManualPolicy(ctx context.Context, urlKey string, policy ManualPolicy) error {
	if !policy.Valid() {
		return fmt.Errorf("invalid manual policy: %q", policy)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE relay_registry
		SET manual_policy = $2,
		    source_manual = ($2 <> 'none'),
		    updated_at = now()
		WHERE url_key = $1
	`, urlKey, string(policy))
	if err != nil {
		return fmt.Errorf("set manual policy: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("relay %q not found in registry", urlKey)
	}
	return nil
}

// SetAdmissionState updates the admission state and score for a relay.
func (s *Store) SetAdmissionState(
	ctx context.Context,
	urlKey string,
	state AdmissionState,
	score float64,
	scoreComponents json.RawMessage,
) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE relay_registry
		SET admission_state = $2,
		    score = $3,
		    score_components_json = $4,
		    updated_at = now()
		WHERE url_key = $1
	`, urlKey, string(state), score, scoreComponents)
	if err != nil {
		return fmt.Errorf("set admission state: %w", err)
	}
	return nil
}

// UpdateProbeRollup updates the relay's current probe summary fields from an observation.
func (s *Store) UpdateProbeRollup(ctx context.Context, urlKey string, status ProbeStatus,
	connectOK, subscribeOK, eoseOK bool,
	avgConnectLatency, avgEOSELatency *float64,
	probeFailRate, yieldScore, duplicateRatio float64,
) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE relay_registry
		SET last_probe_at = now(),
		    last_probe_status = $2,
		    last_connect_ok = $3,
		    last_subscribe_ok = $4,
		    last_eose_ok = $5,
		    avg_connect_latency_ms = $6,
		    avg_eose_latency_ms = $7,
		    probe_fail_rate = $8,
		    yield_score = $9,
		    duplicate_ratio = $10,
		    updated_at = now()
		WHERE url_key = $1
	`, urlKey, string(status), connectOK, subscribeOK, eoseOK,
		avgConnectLatency, avgEOSELatency, probeFailRate, yieldScore, duplicateRatio)
	if err != nil {
		return fmt.Errorf("update probe rollup: %w", err)
	}
	return nil
}

// GetRelay returns a single relay registry record by url_key.
func (s *Store) GetRelay(ctx context.Context, urlKey string) (RelayRecord, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT url_key, normalized_url, discovered_at, last_seen_at,
		       source_seed, source_user_list, source_manual,
		       manual_policy, admission_state,
		       score, distinct_user_ref_count, weighted_user_ref_score,
		       last_probe_at, last_probe_status,
		       last_connect_ok, last_subscribe_ok, last_eose_ok,
		       avg_connect_latency_ms, avg_eose_latency_ms,
		       probe_fail_rate, yield_score, duplicate_ratio,
		       score_components_json, capability_summary_json, notes_json,
		       updated_at
		FROM relay_registry
		WHERE url_key = $1
	`, urlKey)
	return scanRelayRecord(row)
}

// ListRelays returns relay registry rows matching the given filter.
func (s *Store) ListRelays(ctx context.Context, filter ListFilter) ([]RelayRecord, error) {
	var conditions []string
	var args []any
	argIdx := 1

	if len(filter.AdmissionStates) > 0 {
		strs := make([]string, len(filter.AdmissionStates))
		for i, st := range filter.AdmissionStates {
			strs[i] = string(st)
		}
		conditions = append(conditions, fmt.Sprintf("admission_state = ANY($%d)", argIdx))
		args = append(args, strs)
		argIdx++
	}
	if len(filter.ManualPolicies) > 0 {
		strs := make([]string, len(filter.ManualPolicies))
		for i, p := range filter.ManualPolicies {
			strs[i] = string(p)
		}
		conditions = append(conditions, fmt.Sprintf("manual_policy = ANY($%d)", argIdx))
		args = append(args, strs)
	}

	query := `
		SELECT url_key, normalized_url, discovered_at, last_seen_at,
		       source_seed, source_user_list, source_manual,
		       manual_policy, admission_state,
		       score, distinct_user_ref_count, weighted_user_ref_score,
		       last_probe_at, last_probe_status,
		       last_connect_ok, last_subscribe_ok, last_eose_ok,
		       avg_connect_latency_ms, avg_eose_latency_ms,
		       probe_fail_rate, yield_score, duplicate_ratio,
		       score_components_json, capability_summary_json, notes_json,
		       updated_at
		FROM relay_registry`

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY score DESC, normalized_url ASC"

	limit := filter.Limit
	if limit <= 0 {
		limit = 500
	}
	query += fmt.Sprintf(" LIMIT %d", limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list relays: %w", err)
	}
	defer rows.Close()

	var out []RelayRecord
	for rows.Next() {
		rec, err := scanRelayRecordFromRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read relay rows: %w", err)
	}
	return out, nil
}

// GetDesiredActiveRelays returns the relay URLs from the most recently published desired set.
func (s *Store) GetDesiredActiveRelays(ctx context.Context) ([]string, error) {
	var urlsJSON json.RawMessage
	err := s.pool.QueryRow(ctx, `
		SELECT relay_urls_json
		FROM relay_desired_set
		ORDER BY published_at DESC
		LIMIT 1
	`).Scan(&urlsJSON)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get desired active relays: %w", err)
	}
	var urls []string
	if err := json.Unmarshal(urlsJSON, &urls); err != nil {
		return nil, fmt.Errorf("decode desired relay urls: %w", err)
	}
	return urls, nil
}

// PublishDesiredSet atomically records a new desired active relay set snapshot.
func (s *Store) PublishDesiredSet(ctx context.Context, relayURLs []string, source, notes string) error {
	urlsJSON, err := json.Marshal(relayURLs)
	if err != nil {
		return fmt.Errorf("marshal desired set: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO relay_desired_set (relay_urls_json, source, notes)
		VALUES ($1, $2, $3)
	`, urlsJSON, source, notes)
	if err != nil {
		return fmt.Errorf("publish desired set: %w", err)
	}
	return nil
}

// InsertProbeObservation persists a single probe result.
func (s *Store) InsertProbeObservation(ctx context.Context, obs ProbeObservation) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO relay_probe_observations (
			url_key, probed_at, connect_ok, subscribe_ok, eose_ok,
			connect_latency_ms, eose_latency_ms,
			error_code, error_text_short,
			sample_yield_count, sample_duplicate_ratio,
			capability_snapshot_json
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, obs.URLKey, obs.ProbedAt, obs.ConnectOK, obs.SubscribeOK, obs.EOSEOK,
		obs.ConnectLatencyMs, obs.EOSELatencyMs,
		obs.ErrorCode, obs.ErrorTextShort,
		obs.SampleYieldCount, obs.SampleDupRatio, obs.CapabilityJSON)
	if err != nil {
		return fmt.Errorf("insert probe observation: %w", err)
	}
	return nil
}

// ListRecentObservations returns the most recent probe observations for a relay.
func (s *Store) ListRecentObservations(ctx context.Context, urlKey string, limit int) ([]ProbeObservation, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, url_key, probed_at, connect_ok, subscribe_ok, eose_ok,
		       connect_latency_ms, eose_latency_ms,
		       error_code, error_text_short,
		       sample_yield_count, sample_duplicate_ratio,
		       capability_snapshot_json
		FROM relay_probe_observations
		WHERE url_key = $1
		ORDER BY probed_at DESC
		LIMIT $2
	`, urlKey, limit)
	if err != nil {
		return nil, fmt.Errorf("list observations: %w", err)
	}
	defer rows.Close()

	var out []ProbeObservation
	for rows.Next() {
		var o ProbeObservation
		if err := rows.Scan(
			&o.ID, &o.URLKey, &o.ProbedAt,
			&o.ConnectOK, &o.SubscribeOK, &o.EOSEOK,
			&o.ConnectLatencyMs, &o.EOSELatencyMs,
			&o.ErrorCode, &o.ErrorTextShort,
			&o.SampleYieldCount, &o.SampleDupRatio, &o.CapabilityJSON,
		); err != nil {
			return nil, fmt.Errorf("scan observation: %w", err)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// PurgeOldObservations deletes probe observations older than the given cutoff.
func (s *Store) PurgeOldObservations(ctx context.Context, olderThan time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM relay_probe_observations
		WHERE id IN (
			SELECT id FROM relay_probe_observations
			WHERE probed_at < $1
			ORDER BY probed_at ASC
			LIMIT $2
		)
	`, olderThan, limit)
	if err != nil {
		return 0, fmt.Errorf("purge old observations: %w", err)
	}
	return tag.RowsAffected(), nil
}

// GetActiveAndPinnedRelayURLs returns the normalized URLs of all relays currently
// in active or pinned admission state. Used for desired set derivation.
func (s *Store) GetActiveAndPinnedRelayURLs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT normalized_url
		FROM relay_registry
		WHERE admission_state IN ('active', 'pinned')
		  AND manual_policy != 'blocked'
		  AND manual_policy != 'drained'
		ORDER BY score DESC, normalized_url ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("get active/pinned relay urls: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return nil, fmt.Errorf("scan relay url: %w", err)
		}
		out = append(out, url)
	}
	return out, rows.Err()
}

// ListRelaysForProbing returns relays that should be probed, ordered by probe priority.
// Probation and active/pinned stay ahead so the live set keeps fresh health data.
// Candidate and inactive share the next tier and are ordered by popularity so
// high-ref relays are not starved after a probation-cap demotion.
func (s *Store) ListRelaysForProbing(ctx context.Context, limit int) ([]RelayRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT url_key, normalized_url, discovered_at, last_seen_at,
		       source_seed, source_user_list, source_manual,
		       manual_policy, admission_state,
		       score, distinct_user_ref_count, weighted_user_ref_score,
		       last_probe_at, last_probe_status,
		       last_connect_ok, last_subscribe_ok, last_eose_ok,
		       avg_connect_latency_ms, avg_eose_latency_ms,
		       probe_fail_rate, yield_score, duplicate_ratio,
		       score_components_json, capability_summary_json, notes_json,
		       updated_at
		FROM relay_registry
		WHERE manual_policy != 'blocked'
		  AND admission_state NOT IN ('blocked')
		ORDER BY
			CASE admission_state
				WHEN 'probation' THEN 1
				WHEN 'active' THEN 2
				WHEN 'pinned' THEN 2
				WHEN 'candidate' THEN 3
				WHEN 'inactive' THEN 3
				ELSE 4
			END ASC,
			distinct_user_ref_count DESC,
			COALESCE(last_probe_at, '1970-01-01'::timestamptz) ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list relays for probing: %w", err)
	}
	defer rows.Close()

	var out []RelayRecord
	for rows.Next() {
		rec, err := scanRelayRecordFromRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func scanRelayRecord(row pgx.Row) (RelayRecord, error) {
	var r RelayRecord
	var manualPolicy, admissionState string
	var probeStatus *string
	if err := row.Scan(
		&r.URLKey, &r.NormalizedURL, &r.DiscoveredAt, &r.LastSeenAt,
		&r.SourceSeed, &r.SourceUserList, &r.SourceManual,
		&manualPolicy, &admissionState,
		&r.Score, &r.DistinctUserRefCount, &r.WeightedUserRefScore,
		&r.LastProbeAt, &probeStatus,
		&r.LastConnectOK, &r.LastSubscribeOK, &r.LastEOSEOK,
		&r.AvgConnectLatency, &r.AvgEOSELatency,
		&r.ProbeFailRate, &r.YieldScore, &r.DuplicateRatio,
		&r.ScoreComponents, &r.CapabilitySummary, &r.Notes,
		&r.UpdatedAt,
	); err != nil {
		return r, fmt.Errorf("scan relay record: %w", err)
	}
	r.ManualPolicy = ManualPolicy(manualPolicy)
	r.AdmissionState = AdmissionState(admissionState)
	if probeStatus != nil {
		ps := ProbeStatus(*probeStatus)
		r.LastProbeStatus = &ps
	}
	return r, nil
}

func scanRelayRecordFromRows(rows pgx.Rows) (RelayRecord, error) {
	var r RelayRecord
	var manualPolicy, admissionState string
	var probeStatus *string
	if err := rows.Scan(
		&r.URLKey, &r.NormalizedURL, &r.DiscoveredAt, &r.LastSeenAt,
		&r.SourceSeed, &r.SourceUserList, &r.SourceManual,
		&manualPolicy, &admissionState,
		&r.Score, &r.DistinctUserRefCount, &r.WeightedUserRefScore,
		&r.LastProbeAt, &probeStatus,
		&r.LastConnectOK, &r.LastSubscribeOK, &r.LastEOSEOK,
		&r.AvgConnectLatency, &r.AvgEOSELatency,
		&r.ProbeFailRate, &r.YieldScore, &r.DuplicateRatio,
		&r.ScoreComponents, &r.CapabilitySummary, &r.Notes,
		&r.UpdatedAt,
	); err != nil {
		return r, fmt.Errorf("scan relay record: %w", err)
	}
	r.ManualPolicy = ManualPolicy(manualPolicy)
	r.AdmissionState = AdmissionState(admissionState)
	if probeStatus != nil {
		ps := ProbeStatus(*probeStatus)
		r.LastProbeStatus = &ps
	}
	return r, nil
}
