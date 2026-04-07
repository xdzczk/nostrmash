package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type TrustPubkeyFrontierEntry struct {
	Pubkey      string
	TrustRank   int64
	TrustScore  float64
	SourceRunID int64
}

type TrustPubkeyFrontierRefreshResult struct {
	CandidatesUpserted int
	PromotedToActive   int
	ReactivatedFailed  int
	ActiveCount        int
}

func (s *PostgresStore) RefreshTrustPubkeyFrontier(
	ctx context.Context,
	limit int,
	minStableWindow time.Duration,
	maxPromotions int,
) (TrustPubkeyFrontierRefreshResult, error) {
	if s == nil || s.pool == nil {
		return TrustPubkeyFrontierRefreshResult{}, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 2000
	}
	if maxPromotions <= 0 {
		maxPromotions = 100
	}
	candidates, err := s.ListTrustPubkeyCandidates(ctx, limit)
	if err != nil {
		return TrustPubkeyFrontierRefreshResult{}, err
	}
	if len(candidates) == 0 {
		return TrustPubkeyFrontierRefreshResult{}, nil
	}
	now := time.Now().UTC()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TrustPubkeyFrontierRefreshResult{}, fmt.Errorf("begin trust pubkey frontier refresh: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	seenPubkeys := make([]string, 0, len(candidates))
	upserted := 0
	for _, candidate := range candidates {
		pubkey := strings.TrimSpace(candidate.Pubkey)
		if pubkey == "" {
			continue
		}
		seenPubkeys = append(seenPubkeys, pubkey)
		commandTag, err := tx.Exec(ctx, `
			INSERT INTO ingest_pubkey_frontier (
				pubkey,
				source_run_id,
				trust_rank,
				trust_score,
				state,
				first_seen_at,
				next_eligible_at,
				updated_at
			)
			VALUES ($1, $2, $3, $4, 'candidate', $5, $6, $5)
			ON CONFLICT (pubkey) DO UPDATE
			SET source_run_id = EXCLUDED.source_run_id,
				trust_rank = EXCLUDED.trust_rank,
				trust_score = EXCLUDED.trust_score,
				updated_at = EXCLUDED.updated_at
		`, pubkey, candidate.SourceRunID, candidate.Rank, candidate.Score, now, now.Add(minStableWindow))
		if err != nil {
			return TrustPubkeyFrontierRefreshResult{}, fmt.Errorf("upsert trust pubkey frontier candidate %q: %w", pubkey, err)
		}
		upserted += int(commandTag.RowsAffected())
	}

	if len(seenPubkeys) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return TrustPubkeyFrontierRefreshResult{}, fmt.Errorf("commit empty trust pubkey frontier refresh: %w", err)
		}
		return TrustPubkeyFrontierRefreshResult{}, nil
	}

	var reactivated int
	if err := tx.QueryRow(ctx, `
		WITH changed AS (
			UPDATE ingest_pubkey_frontier
			SET state = 'candidate',
			    updated_at = now()
			WHERE state = 'failed'
			  AND next_eligible_at <= now()
			  AND pubkey = ANY($1)
			RETURNING pubkey
		)
		SELECT COUNT(*) FROM changed
	`, seenPubkeys).Scan(&reactivated); err != nil {
		return TrustPubkeyFrontierRefreshResult{}, fmt.Errorf("reactivate failed trust pubkey frontier rows: %w", err)
	}

	promoted := 0
	stableSeconds := minStableWindow.Seconds()
	if stableSeconds < 0 {
		stableSeconds = 0
	}
	if err := tx.QueryRow(ctx, `
		WITH promotable AS (
			SELECT pubkey
			FROM ingest_pubkey_frontier
			WHERE pubkey = ANY($1)
			  AND state = 'candidate'
			  AND first_seen_at <= (clock_timestamp() - make_interval(secs => $2))
			ORDER BY trust_rank ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		changed AS (
			UPDATE ingest_pubkey_frontier f
			SET state = 'active',
			    next_eligible_at = LEAST(f.next_eligible_at, now()),
			    updated_at = now()
			FROM promotable p
			WHERE f.pubkey = p.pubkey
			RETURNING f.pubkey
		)
		SELECT COUNT(*) FROM changed
	`, seenPubkeys, stableSeconds, maxPromotions).Scan(&promoted); err != nil {
		return TrustPubkeyFrontierRefreshResult{}, fmt.Errorf("promote trust pubkey frontier rows: %w", err)
	}

	activeCount := 0
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM ingest_pubkey_frontier WHERE state = 'active'`).Scan(&activeCount); err != nil {
		return TrustPubkeyFrontierRefreshResult{}, fmt.Errorf("count active trust pubkey frontier rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return TrustPubkeyFrontierRefreshResult{}, fmt.Errorf("commit trust pubkey frontier refresh: %w", err)
	}
	return TrustPubkeyFrontierRefreshResult{
		CandidatesUpserted: upserted,
		PromotedToActive:   promoted,
		ReactivatedFailed:  reactivated,
		ActiveCount:        activeCount,
	}, nil
}

func (s *PostgresStore) ClaimTrustPubkeyFrontierForFetch(
	ctx context.Context,
	limit int,
	cooldown time.Duration,
) ([]TrustPubkeyFrontierEntry, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 50
	}
	if cooldown < 0 {
		cooldown = 0
	}
	rows, err := s.pool.Query(ctx, `
		WITH claimable AS (
			SELECT pubkey
			FROM ingest_pubkey_frontier
			WHERE state = 'active'
			  AND next_eligible_at <= now()
			ORDER BY trust_rank ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE ingest_pubkey_frontier f
		SET state = 'cooldown',
		    last_selected_at = now(),
		    next_eligible_at = now() + make_interval(secs => $2),
		    fetch_attempts = f.fetch_attempts + 1,
		    updated_at = now()
		FROM claimable c
		WHERE f.pubkey = c.pubkey
		RETURNING f.pubkey, f.trust_rank, f.trust_score, f.source_run_id
	`, limit, cooldown.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim trust pubkey frontier rows: %w", err)
	}
	defer rows.Close()
	out := make([]TrustPubkeyFrontierEntry, 0, limit)
	for rows.Next() {
		var entry TrustPubkeyFrontierEntry
		if err := rows.Scan(
			&entry.Pubkey,
			&entry.TrustRank,
			&entry.TrustScore,
			&entry.SourceRunID,
		); err != nil {
			return nil, fmt.Errorf("scan claimed trust pubkey frontier row: %w", err)
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read claimed trust pubkey frontier rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) MarkTrustPubkeyFetchSuccess(
	ctx context.Context,
	pubkey string,
	cooldown time.Duration,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return fmt.Errorf("pubkey is required")
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE ingest_pubkey_frontier
		SET state = 'active',
		    last_fetched_at = now(),
		    next_eligible_at = now() + make_interval(secs => $2),
		    success_count = success_count + 1,
		    last_error = NULL,
		    last_error_at = NULL,
		    updated_at = now()
		WHERE pubkey = $1
	`, pubkey, cooldown.Seconds())
	if err != nil {
		return fmt.Errorf("mark trust pubkey fetch success: %w", err)
	}
	return nil
}

func (s *PostgresStore) MarkTrustPubkeyFetchFailure(
	ctx context.Context,
	pubkey string,
	retryAfter time.Duration,
	cause error,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return fmt.Errorf("pubkey is required")
	}
	errMsg := ""
	if cause != nil {
		errMsg = cause.Error()
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE ingest_pubkey_frontier
		SET state = 'failed',
		    next_eligible_at = now() + make_interval(secs => $2),
		    last_error = $3,
		    last_error_at = now(),
		    updated_at = now()
		WHERE pubkey = $1
	`, pubkey, retryAfter.Seconds(), errMsg)
	if err != nil {
		return fmt.Errorf("mark trust pubkey fetch failure: %w", err)
	}
	return nil
}

type TrustRelaySuggestionRefreshResult struct {
	Upserted      int
	NewPromotions int
}

func (s *PostgresStore) RefreshTrustRelaySuggestions(
	ctx context.Context,
	candidates []TrustRelayCandidate,
	stableWindow time.Duration,
	maxPromotions int,
) (TrustRelaySuggestionRefreshResult, error) {
	if s == nil || s.pool == nil {
		return TrustRelaySuggestionRefreshResult{}, fmt.Errorf("store is not initialized")
	}
	if maxPromotions <= 0 {
		maxPromotions = 20
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TrustRelaySuggestionRefreshResult{}, fmt.Errorf("begin trust relay suggestions refresh: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now := time.Now().UTC()
	seenRelays := make([]string, 0, len(candidates))
	upserted := 0
	for _, candidate := range candidates {
		relayURL := strings.TrimSpace(strings.ToLower(candidate.RelayURL))
		if relayURL == "" {
			continue
		}
		seenRelays = append(seenRelays, relayURL)
		sampleRaw, err := json.Marshal(candidate.SupportingPubkeysSample)
		if err != nil {
			return TrustRelaySuggestionRefreshResult{}, fmt.Errorf("encode relay suggestion sample for %q: %w", relayURL, err)
		}
		commandTag, err := tx.Exec(ctx, `
			INSERT INTO trust_relay_suggestions (
				relay_url,
				weighted_score,
				supporting_pubkeys_count,
				supporting_pubkeys_sample,
				source_run_id,
				source_computed_at,
				first_seen_at,
				last_seen_at,
				updated_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $7)
			ON CONFLICT (relay_url) DO UPDATE
			SET weighted_score = EXCLUDED.weighted_score,
			    supporting_pubkeys_count = EXCLUDED.supporting_pubkeys_count,
			    supporting_pubkeys_sample = EXCLUDED.supporting_pubkeys_sample,
			    source_run_id = EXCLUDED.source_run_id,
			    source_computed_at = EXCLUDED.source_computed_at,
			    last_seen_at = EXCLUDED.last_seen_at,
			    updated_at = EXCLUDED.updated_at
		`, relayURL, candidate.WeightedScore, candidate.SupportingPubkeysCount, sampleRaw, candidate.SourceRunID, candidate.SourceComputedAt, now)
		if err != nil {
			return TrustRelaySuggestionRefreshResult{}, fmt.Errorf("upsert trust relay suggestion %q: %w", relayURL, err)
		}
		upserted += int(commandTag.RowsAffected())
	}

	if len(seenRelays) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE trust_relay_suggestions
			SET is_recommended = false,
			    updated_at = now()
			WHERE is_recommended = true
			  AND relay_url <> ALL($1)
		`, seenRelays); err != nil {
			return TrustRelaySuggestionRefreshResult{}, fmt.Errorf("demote stale trust relay suggestions: %w", err)
		}
	}

	stableSeconds := stableWindow.Seconds()
	if stableSeconds < 0 {
		stableSeconds = 0
	}
	promoted := 0
	if err := tx.QueryRow(ctx, `
		WITH promotable AS (
			SELECT relay_url
			FROM trust_relay_suggestions
			WHERE relay_url = ANY($1)
			  AND is_recommended = false
			  AND first_seen_at <= (clock_timestamp() - make_interval(secs => $2))
			ORDER BY weighted_score DESC, relay_url ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		changed AS (
			UPDATE trust_relay_suggestions s
			SET is_recommended = true,
			    last_promoted_at = now(),
			    updated_at = now()
			FROM promotable p
			WHERE s.relay_url = p.relay_url
			RETURNING s.relay_url
		)
		SELECT COUNT(*) FROM changed
	`, seenRelays, stableSeconds, maxPromotions).Scan(&promoted); err != nil {
		return TrustRelaySuggestionRefreshResult{}, fmt.Errorf("promote trust relay suggestions: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return TrustRelaySuggestionRefreshResult{}, fmt.Errorf("commit trust relay suggestions refresh: %w", err)
	}
	return TrustRelaySuggestionRefreshResult{
		Upserted:      upserted,
		NewPromotions: promoted,
	}, nil
}

type TrustRelaySuggestion struct {
	RelayURL                string
	WeightedScore           float64
	SupportingPubkeysCount  int
	SupportingPubkeysSample []string
	SourceRunID             *int64
	SourceComputedAt        *time.Time
	FirstSeenAt             time.Time
	LastSeenAt              time.Time
	LastPromotedAt          *time.Time
	IsRecommended           bool
	UpdatedAt               time.Time
}

func (s *PostgresStore) ListTrustRelaySuggestions(
	ctx context.Context,
	limit int,
	recommendedOnly bool,
) ([]TrustRelaySuggestion, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT relay_url, weighted_score, supporting_pubkeys_count, supporting_pubkeys_sample,
		       source_run_id, source_computed_at, first_seen_at, last_seen_at,
		       last_promoted_at, is_recommended, updated_at
		FROM trust_relay_suggestions
		WHERE (NOT $1::boolean OR is_recommended = true)
		ORDER BY is_recommended DESC, weighted_score DESC, relay_url ASC
		LIMIT $2
	`, recommendedOnly, limit)
	if err != nil {
		return nil, fmt.Errorf("list trust relay suggestions: %w", err)
	}
	defer rows.Close()
	out := make([]TrustRelaySuggestion, 0, limit)
	for rows.Next() {
		var item TrustRelaySuggestion
		var sampleRaw []byte
		if err := rows.Scan(
			&item.RelayURL,
			&item.WeightedScore,
			&item.SupportingPubkeysCount,
			&sampleRaw,
			&item.SourceRunID,
			&item.SourceComputedAt,
			&item.FirstSeenAt,
			&item.LastSeenAt,
			&item.LastPromotedAt,
			&item.IsRecommended,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan trust relay suggestion: %w", err)
		}
		if len(sampleRaw) > 0 {
			if err := json.Unmarshal(sampleRaw, &item.SupportingPubkeysSample); err != nil {
				return nil, fmt.Errorf("decode trust relay suggestion sample: %w", err)
			}
		}
		item.RelayURL = strings.TrimSpace(strings.ToLower(item.RelayURL))
		item.FirstSeenAt = item.FirstSeenAt.UTC()
		item.LastSeenAt = item.LastSeenAt.UTC()
		item.UpdatedAt = item.UpdatedAt.UTC()
		if item.SourceComputedAt != nil {
			ts := item.SourceComputedAt.UTC()
			item.SourceComputedAt = &ts
		}
		if item.LastPromotedAt != nil {
			ts := item.LastPromotedAt.UTC()
			item.LastPromotedAt = &ts
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trust relay suggestions: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetTrustPubkeyFrontierStats(ctx context.Context) (active, candidate, cooldown, failed int, err error) {
	if s == nil || s.pool == nil {
		return 0, 0, 0, 0, fmt.Errorf("store is not initialized")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT state, COUNT(*)
		FROM ingest_pubkey_frontier
		GROUP BY state
	`)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("query trust pubkey frontier stats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("scan trust pubkey frontier stat row: %w", err)
		}
		switch strings.TrimSpace(state) {
		case "active":
			active = count
		case "candidate":
			candidate = count
		case "cooldown":
			cooldown = count
		case "failed":
			failed = count
		}
	}
	if err := rows.Err(); err != nil && err != pgx.ErrNoRows {
		return 0, 0, 0, 0, fmt.Errorf("read trust pubkey frontier stats: %w", err)
	}
	return active, candidate, cooldown, failed, nil
}
