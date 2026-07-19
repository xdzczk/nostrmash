package trust

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type TrustRelaySuggestionRefreshResult struct {
	Upserted      int
	NewPromotions int
}

func (s *Trust) RefreshTrustRelaySuggestions(
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

func (s *Trust) ListTrustRelaySuggestions(
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
