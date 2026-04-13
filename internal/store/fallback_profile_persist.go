package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// PersistFallbackProfile upserts a relay-fetched profile into profiles_latest
// so that subsequent lookups and searches find it locally. Only writes if the
// incoming profile is newer than (or absent from) the existing row.
//
// Because profiles_latest.metadata_event_id has a FK to events.id, we first
// ensure a minimal kind-0 event row exists in events before upserting the
// projection.
func (s *PostgresStore) PersistFallbackProfile(ctx context.Context, pp ProfileProjection) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	pubkey := strings.TrimSpace(pp.Pubkey)
	if pubkey == "" {
		return fmt.Errorf("pubkey is required")
	}
	eventID := strings.TrimSpace(pp.MetadataEventID)
	if eventID == "" {
		return fmt.Errorf("metadata_event_id is required")
	}

	profileJSON := pp.ProfileJSON
	if len(profileJSON) == 0 {
		profileJSON = json.RawMessage(`{}`)
	}

	rawEvent, err := json.Marshal(map[string]any{
		"id":         eventID,
		"pubkey":     pubkey,
		"created_at": pp.MetadataCreatedAt,
		"kind":       0,
		"tags":       [][]string{},
		"content":    string(profileJSON),
		"sig":        "",
	})
	if err != nil {
		return fmt.Errorf("build fallback event json: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at, inserted_at)
		VALUES ($1, $2, $3, 0, '', $4, $5, now(), now())
		ON CONFLICT (id) DO NOTHING
	`, eventID, pubkey, pp.MetadataCreatedAt, string(profileJSON), json.RawMessage(rawEvent))
	if err != nil {
		return fmt.Errorf("persist fallback kind-0 event: %w", err)
	}

	name, displayName, about, nip05 := extractProfileFields(profileJSON)

	_, err = s.pool.Exec(ctx, `
		INSERT INTO profiles_latest (
			pubkey, metadata_event_id, metadata_created_at, profile_json,
			name, display_name, about, nip05, derivation_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0)
		ON CONFLICT (pubkey) DO UPDATE
		SET metadata_event_id   = EXCLUDED.metadata_event_id,
		    metadata_created_at = EXCLUDED.metadata_created_at,
		    profile_json        = EXCLUDED.profile_json,
		    name                = EXCLUDED.name,
		    display_name        = EXCLUDED.display_name,
		    about               = EXCLUDED.about,
		    nip05               = EXCLUDED.nip05,
		    updated_at          = now()
		WHERE EXCLUDED.metadata_created_at > profiles_latest.metadata_created_at
		   OR (EXCLUDED.metadata_created_at = profiles_latest.metadata_created_at
		       AND EXCLUDED.metadata_event_id > profiles_latest.metadata_event_id)
	`, pubkey, eventID, pp.MetadataCreatedAt, profileJSON,
		name, displayName, about, nip05)
	if err != nil {
		return fmt.Errorf("persist fallback profile: %w", err)
	}
	return nil
}

func extractProfileFields(profileJSON json.RawMessage) (name, displayName, about, nip05 string) {
	var obj map[string]any
	if err := json.Unmarshal(profileJSON, &obj); err != nil {
		return
	}
	name, _ = obj["name"].(string)
	displayName, _ = obj["display_name"].(string)
	about, _ = obj["about"].(string)
	nip05, _ = obj["nip05"].(string)
	return
}
