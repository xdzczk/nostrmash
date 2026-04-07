package api

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

type adminRelayState struct {
	RelayURL           string                 `json:"relay_url"`
	Configured         bool                   `json:"configured"`
	Disabled           bool                   `json:"disabled"`
	LatestCheckpointAt *time.Time             `json:"latest_checkpoint_at,omitempty"`
	Checkpoints        []adminRelayCheckpoint `json:"checkpoints"`
}

type adminRelayCheckpoint struct {
	Mode               string     `json:"mode"`
	FilterGroup        string     `json:"filter_group"`
	State              string     `json:"state"`
	Status             string     `json:"status"`
	Since              *int64     `json:"since,omitempty"`
	Until              *int64     `json:"until,omitempty"`
	Cursor             *string    `json:"cursor,omitempty"`
	LastConnectedAt    *time.Time `json:"last_connected_at,omitempty"`
	LastDisconnectedAt *time.Time `json:"last_disconnected_at,omitempty"`
	LastProgressAt     *time.Time `json:"last_progress_at,omitempty"`
	LastError          *string    `json:"last_error,omitempty"`
	LastErrorAt        *time.Time `json:"last_error_at,omitempty"`
	EOSESeenAt         *time.Time `json:"eose_seen_at,omitempty"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

type adminRelaySuggestion struct {
	RelayURL                string     `json:"relay_url"`
	WeightedScore           float64    `json:"weighted_score"`
	SupportingPubkeysCount  int        `json:"supporting_pubkeys_count"`
	SupportingPubkeysSample []string   `json:"supporting_pubkeys_sample,omitempty"`
	AlreadyConfigured       bool       `json:"already_configured"`
	Disabled                bool       `json:"disabled"`
	Recommended             bool       `json:"recommended"`
	SourceRunID             *int64     `json:"source_run_id,omitempty"`
	SourceComputedAt        *time.Time `json:"source_computed_at,omitempty"`
	FirstSeenAt             time.Time  `json:"first_seen_at"`
	LastSeenAt              time.Time  `json:"last_seen_at"`
	LastPromotedAt          *time.Time `json:"last_promoted_at,omitempty"`
	UpdatedAt               time.Time  `json:"updated_at"`
}

func (s *adminService) GetRelays(ctx context.Context) ([]adminRelayState, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT relay_url, mode, filter_group, "since", "until", cursor, last_connected_at, last_disconnected_at,
		       last_progress_at, eose_seen_at, status, last_error, last_error_at, updated_at
		FROM ingest_checkpoints
		ORDER BY relay_url ASC, mode ASC, filter_group ASC
	`)
	if err != nil {
		if strings.Contains(err.Error(), `column "since" does not exist`) {
			rows, err = s.pool.Query(ctx, `
				SELECT relay_url, mode, filter_group, since_ts, until_ts, cursor_val, NULL::timestamptz AS last_connected_at,
				       NULL::timestamptz AS last_disconnected_at, NULL::timestamptz AS last_progress_at, eose_seen_at,
				       status, NULL::text AS last_error, NULL::timestamptz AS last_error_at, updated_at
				FROM ingest_checkpoints
				ORDER BY relay_url ASC, mode ASC, filter_group ASC
			`)
		}
		if err != nil {
			return nil, fmt.Errorf("list relay checkpoints: %w", err)
		}
	}
	defer rows.Close()

	relayByURL := make(map[string]*adminRelayState)
	for _, relayURL := range s.configuredRelays {
		trimmed := strings.TrimSpace(relayURL)
		if trimmed == "" {
			continue
		}
		relayByURL[trimmed] = &adminRelayState{
			RelayURL:    trimmed,
			Configured:  true,
			Disabled:    s.isDisabledRelay(trimmed),
			Checkpoints: make([]adminRelayCheckpoint, 0),
		}
	}

	for rows.Next() {
		var checkpoint model.IngestCheckpoint
		if err := rows.Scan(
			&checkpoint.RelayURL,
			&checkpoint.Mode,
			&checkpoint.FilterGroup,
			&checkpoint.Since,
			&checkpoint.Until,
			&checkpoint.Cursor,
			&checkpoint.LastConnectedAt,
			&checkpoint.LastDisconnectedAt,
			&checkpoint.LastProgressAt,
			&checkpoint.EOSESeenAt,
			&checkpoint.Status,
			&checkpoint.LastError,
			&checkpoint.LastErrorAt,
			&checkpoint.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan relay checkpoint: %w", err)
		}
		relayURL := strings.TrimSpace(checkpoint.RelayURL)
		entry, ok := relayByURL[relayURL]
		if !ok {
			entry = &adminRelayState{
				RelayURL:    relayURL,
				Checkpoints: make([]adminRelayCheckpoint, 0, 1),
			}
			relayByURL[relayURL] = entry
		}
		entry.Checkpoints = append(entry.Checkpoints, adminRelayCheckpoint{
			Mode:               checkpoint.Mode,
			FilterGroup:        checkpoint.FilterGroup,
			State:              checkpoint.Status,
			Status:             checkpoint.Status,
			Since:              checkpoint.Since,
			Until:              checkpoint.Until,
			Cursor:             checkpoint.Cursor,
			LastConnectedAt:    checkpoint.LastConnectedAt,
			LastDisconnectedAt: checkpoint.LastDisconnectedAt,
			LastProgressAt:     checkpoint.LastProgressAt,
			LastError:          checkpoint.LastError,
			LastErrorAt:        checkpoint.LastErrorAt,
			EOSESeenAt:         checkpoint.EOSESeenAt,
			UpdatedAt:          checkpoint.UpdatedAt.UTC(),
		})
		if entry.LatestCheckpointAt == nil || checkpoint.UpdatedAt.After(*entry.LatestCheckpointAt) {
			latest := checkpoint.UpdatedAt.UTC()
			entry.LatestCheckpointAt = &latest
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read relay checkpoints: %w", err)
	}

	relays := make([]adminRelayState, 0, len(relayByURL))
	for _, relayState := range relayByURL {
		slices.SortFunc(relayState.Checkpoints, func(a, b adminRelayCheckpoint) int {
			if cmp := strings.Compare(a.Mode, b.Mode); cmp != 0 {
				return cmp
			}
			return strings.Compare(a.FilterGroup, b.FilterGroup)
		})
		relays = append(relays, *relayState)
	}
	slices.SortFunc(relays, func(a, b adminRelayState) int {
		return strings.Compare(a.RelayURL, b.RelayURL)
	})
	return relays, nil
}

func (s *adminService) isDisabledRelay(relayURL string) bool {
	_, ok := s.disabledRelays[relayURL]
	return ok
}

func (s *adminService) GetRelaySuggestions(
	ctx context.Context,
	limit int,
	recommendedOnly bool,
) ([]adminRelaySuggestion, error) {
	if limit <= 0 {
		limit = 50
	}
	storeReader := store.NewPostgresStore(s.pool)
	candidates, err := storeReader.ListTrustRelayCandidates(ctx, store.TrustRelayCandidateQuery{
		TopPubkeys: 2000,
		Limit:      max(limit*2, 200),
	})
	if err != nil {
		return nil, fmt.Errorf("list trust relay candidates: %w", err)
	}
	_, _ = storeReader.RefreshTrustRelaySuggestions(ctx, candidates, 10*time.Minute, 25)
	suggestions, err := storeReader.ListTrustRelaySuggestions(ctx, limit, recommendedOnly)
	if err != nil {
		return nil, fmt.Errorf("list trust relay suggestions: %w", err)
	}
	configured := make(map[string]struct{}, len(s.configuredRelays))
	for _, relayURL := range s.configuredRelays {
		normalized := strings.TrimSpace(strings.ToLower(relayURL))
		if normalized == "" {
			continue
		}
		configured[normalized] = struct{}{}
	}
	out := make([]adminRelaySuggestion, 0, len(suggestions))
	for _, suggestion := range suggestions {
		_, alreadyConfigured := configured[suggestion.RelayURL]
		out = append(out, adminRelaySuggestion{
			RelayURL:                suggestion.RelayURL,
			WeightedScore:           suggestion.WeightedScore,
			SupportingPubkeysCount:  suggestion.SupportingPubkeysCount,
			SupportingPubkeysSample: append([]string(nil), suggestion.SupportingPubkeysSample...),
			AlreadyConfigured:       alreadyConfigured,
			Disabled:                s.isDisabledRelay(suggestion.RelayURL),
			Recommended:             suggestion.IsRecommended,
			SourceRunID:             suggestion.SourceRunID,
			SourceComputedAt:        suggestion.SourceComputedAt,
			FirstSeenAt:             suggestion.FirstSeenAt,
			LastSeenAt:              suggestion.LastSeenAt,
			LastPromotedAt:          suggestion.LastPromotedAt,
			UpdatedAt:               suggestion.UpdatedAt,
		})
	}
	return out, nil
}

func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}
