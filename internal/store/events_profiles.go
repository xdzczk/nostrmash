package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

// GetProfileByPubkey fetches the latest projected profile for one pubkey.
func (s *PostgresStore) GetProfileByPubkey(ctx context.Context, pubkey string) (out ProfileProjection, err error) {
	started := time.Now()
	ctx, span := traceutil.StartSpan(ctx, "store.get_profile_by_pubkey")
	defer func() {
		span.End(err)
		metrics.ObserveDBOperation("get_profile_by_pubkey", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return out, fmt.Errorf("pubkey is required")
	}

	var profileText string
	err = s.pool.QueryRow(ctx, `
		SELECT pubkey, metadata_event_id, metadata_created_at, profile_json::text
		FROM profiles_latest
		WHERE pubkey = $1
	`, pubkey).Scan(&out.Pubkey, &out.MetadataEventID, &out.MetadataCreatedAt, &profileText)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			derived, fallbackErr := s.getProfileProjectionFromLatestMetadataEvent(ctx, pubkey)
			if fallbackErr != nil {
				return out, fallbackErr
			}
			return derived, nil
		}
		return out, fmt.Errorf("get profile by pubkey: %w", err)
	}
	out.ProfileJSON = json.RawMessage(profileText)
	return out, nil
}

// GetProfilesByPubkeys fetches projected profiles for a unique set of pubkeys.
func (s *PostgresStore) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]ProfileProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(pubkeys) == 0 {
		return map[string]ProfileProjection{}, nil
	}

	normalized := make([]string, 0, len(pubkeys))
	seen := make(map[string]struct{}, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		normalized = append(normalized, pubkey)
	}
	if len(normalized) == 0 {
		return map[string]ProfileProjection{}, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, metadata_event_id, metadata_created_at, profile_json::text
		FROM profiles_latest
		WHERE pubkey = ANY($1::text[])
	`, normalized)
	if err != nil {
		return nil, fmt.Errorf("get profiles by pubkeys: %w", err)
	}
	defer rows.Close()

	out := make(map[string]ProfileProjection, len(normalized))
	for rows.Next() {
		var row ProfileProjection
		var profileText string
		if err := rows.Scan(&row.Pubkey, &row.MetadataEventID, &row.MetadataCreatedAt, &profileText); err != nil {
			return nil, fmt.Errorf("scan profile row: %w", err)
		}
		row.ProfileJSON = json.RawMessage(profileText)
		out[row.Pubkey] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read profile rows: %w", err)
	}
	missing := make([]string, 0)
	for _, pubkey := range normalized {
		if _, ok := out[pubkey]; ok {
			continue
		}
		missing = append(missing, pubkey)
	}
	if len(missing) == 0 {
		return out, nil
	}
	derived, err := s.getProfileProjectionsFromLatestMetadataEvents(ctx, missing)
	if err != nil {
		return nil, err
	}
	for pubkey, profile := range derived {
		out[pubkey] = profile
	}
	return out, nil
}

func (s *PostgresStore) getProfileProjectionFromLatestMetadataEvent(ctx context.Context, pubkey string) (ProfileProjection, error) {
	rows, err := s.getProfileProjectionsFromLatestMetadataEvents(ctx, []string{pubkey})
	if err != nil {
		return ProfileProjection{}, err
	}
	profile, ok := rows[pubkey]
	if !ok {
		return ProfileProjection{}, ErrNotFound
	}
	return profile, nil
}

func (s *PostgresStore) getProfileProjectionsFromLatestMetadataEvents(
	ctx context.Context,
	pubkeys []string,
) (map[string]ProfileProjection, error) {
	if len(pubkeys) == 0 {
		return map[string]ProfileProjection{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (pubkey) pubkey, id, created_at, content
		FROM events
		WHERE kind = 0
		  AND pubkey = ANY($1::text[])
		ORDER BY pubkey, created_at DESC, id DESC
	`, pubkeys)
	if err != nil {
		return nil, fmt.Errorf("get latest metadata events by pubkey: %w", err)
	}
	defer rows.Close()

	out := make(map[string]ProfileProjection, len(pubkeys))
	for rows.Next() {
		var (
			row     ProfileProjection
			content string
		)
		if err := rows.Scan(&row.Pubkey, &row.MetadataEventID, &row.MetadataCreatedAt, &content); err != nil {
			return nil, fmt.Errorf("scan latest metadata event row: %w", err)
		}
		row.ProfileJSON = normalizeProfileJSONContent(content)
		out[row.Pubkey] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read latest metadata event rows: %w", err)
	}
	return out, nil
}

func normalizeProfileJSONContent(content string) json.RawMessage {
	content = strings.TrimSpace(content)
	if content == "" {
		return json.RawMessage(`{}`)
	}
	var profile map[string]any
	if err := json.Unmarshal([]byte(content), &profile); err != nil {
		return json.RawMessage(`{}`)
	}
	if profile == nil {
		return json.RawMessage(`{}`)
	}
	encoded, err := json.Marshal(profile)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(encoded)
}
