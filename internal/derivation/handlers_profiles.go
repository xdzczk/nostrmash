package derivation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

func (h *Handlers) ProjectProfilesLatest(ctx context.Context, eventID string) error {
	return h.projectProfilesLatestWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectProfilesLatestWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var kind int
	err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &kind)
	if err != nil {
		return fmt.Errorf("load event for profile projection: %w", err)
	}
	if kind != 0 {
		return nil
	}

	type metadataWinner struct {
		EventID   string
		CreatedAt int64
		Content   string
	}
	var winner metadataWinner
	err = h.pool.QueryRow(ctx, `
		SELECT id, created_at, content
		FROM events
		WHERE pubkey = $1
		  AND kind = 0
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, pubkey).Scan(&winner.EventID, &winner.CreatedAt, &winner.Content)
	if err != nil {
		return fmt.Errorf("load latest metadata event: %w", err)
	}

	profileJSON := json.RawMessage(`{}`)
	var profileName string
	var profileDisplayName string
	var profileAbout string
	var profileNIP05 string
	if strings.TrimSpace(winner.Content) != "" {
		var profile map[string]any
		if err := json.Unmarshal([]byte(winner.Content), &profile); err == nil {
			encoded, marshalErr := json.Marshal(profile)
			if marshalErr == nil {
				profileJSON = encoded
			}
			profileName = profileStringField(profile, "name")
			profileDisplayName = profileStringField(profile, "display_name")
			profileAbout = profileStringField(profile, "about")
			profileNIP05 = profileStringField(profile, "nip05")
		}
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationProfilesLatest,
		ProfilesLatestVersion,
		"Project latest effective replaceable metadata (kind 0) per pubkey",
		versionOverride,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO profiles_latest (
			pubkey, metadata_event_id, metadata_created_at, profile_json,
			name, display_name, about, nip05, derivation_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (pubkey) DO UPDATE
		SET metadata_event_id = EXCLUDED.metadata_event_id,
		    metadata_created_at = EXCLUDED.metadata_created_at,
		    profile_json = EXCLUDED.profile_json,
		    name = EXCLUDED.name,
		    display_name = EXCLUDED.display_name,
		    about = EXCLUDED.about,
		    nip05 = EXCLUDED.nip05,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`,
		pubkey,
		winner.EventID,
		winner.CreatedAt,
		profileJSON,
		profileName,
		profileDisplayName,
		profileAbout,
		profileNIP05,
		writeVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert profiles_latest: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit profile projection tx: %w", err)
	}
	return nil
}

func profileStringField(profile map[string]any, key string) string {
	if profile == nil {
		return ""
	}
	raw, ok := profile[key]
	if !ok {
		return ""
	}
	value, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}
