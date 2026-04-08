package derivation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) ProjectContactListsLatest(ctx context.Context, eventID string) error {
	return h.projectContactListsLatestWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectContactListsLatestWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectReplaceableListLatest(
		ctx,
		eventID,
		3,
		DerivationContactListsLatest,
		ContactListsLatestVersion,
		"Project contact_lists_latest from kind=3 replaceables",
		versionOverride,
		func(tags [][]string) json.RawMessage {
			contacts := make([]string, 0)
			for _, tag := range tags {
				if len(tag) < 2 || tag[0] != "p" {
					continue
				}
				value := strings.TrimSpace(tag[1])
				if value == "" {
					continue
				}
				contacts = append(contacts, value)
			}
			contacts = normalizeUniqueIDs(contacts)
			if len(contacts) == 0 {
				return json.RawMessage(`[]`)
			}
			encoded, err := json.Marshal(contacts)
			if err != nil {
				return json.RawMessage(`[]`)
			}
			return encoded
		},
		func(tx pgx.Tx, pubkey, winnerID string, winnerCreatedAt int64, payload json.RawMessage, writeVersion int) error {
			tag, err := tx.Exec(ctx, `
				INSERT INTO contact_lists_latest (
					pubkey, event_id, created_at, contacts_json, derivation_version
				)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (pubkey) DO UPDATE
				SET event_id = EXCLUDED.event_id,
				    created_at = EXCLUDED.created_at,
				    contacts_json = EXCLUDED.contacts_json,
				    derivation_version = EXCLUDED.derivation_version,
				    updated_at = now()
				WHERE EXCLUDED.created_at > contact_lists_latest.created_at
				   OR (
				       EXCLUDED.created_at = contact_lists_latest.created_at
				       AND EXCLUDED.event_id >= contact_lists_latest.event_id
				   )
			`, pubkey, winnerID, winnerCreatedAt, payload, writeVersion)
			if err != nil {
				return fmt.Errorf("upsert contact_lists_latest: %w", err)
			}
			if tag.RowsAffected() == 0 {
				return nil
			}

			previousFollowed := make([]string, 0)
			rows, err := tx.Query(ctx, `
				SELECT followed_pubkey
				FROM follower_edges
				WHERE follower_pubkey = $1
			`, pubkey)
			if err != nil {
				return fmt.Errorf("load prior follower edges for author: %w", err)
			}
			for rows.Next() {
				var followedPubkey string
				if err := rows.Scan(&followedPubkey); err != nil {
					rows.Close()
					return fmt.Errorf("scan prior follower edge row: %w", err)
				}
				previousFollowed = append(previousFollowed, followedPubkey)
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return fmt.Errorf("read prior follower edge rows: %w", err)
			}
			rows.Close()

			followerWriteVersion, err := resolveDerivationWriteVersion(
				ctx,
				tx,
				DerivationFollowerEdges,
				FollowerEdgesVersion,
				"Project follower edges from latest contact_lists_latest state",
				versionOverride,
			)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				DELETE FROM follower_edges
				WHERE follower_pubkey = $1
			`, pubkey); err != nil {
				return fmt.Errorf("delete follower edges for author: %w", err)
			}

			var contacts []string
			if err := json.Unmarshal(payload, &contacts); err != nil {
				return fmt.Errorf("decode contact list payload: %w", err)
			}
			for _, followedPubkey := range contacts {
				followedPubkey = strings.TrimSpace(followedPubkey)
				if followedPubkey == "" {
					continue
				}
				if _, err := tx.Exec(ctx, `
					INSERT INTO follower_edges (
						followed_pubkey,
						follower_pubkey,
						source_event_id,
						contact_list_created_at,
						derivation_version
					)
					VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (followed_pubkey, follower_pubkey) DO UPDATE
					SET source_event_id = EXCLUDED.source_event_id,
					    contact_list_created_at = EXCLUDED.contact_list_created_at,
					    derivation_version = EXCLUDED.derivation_version,
					    updated_at = now()
				`, followedPubkey, pubkey, winnerID, winnerCreatedAt, followerWriteVersion); err != nil {
					return fmt.Errorf("upsert follower edge: %w", err)
				}
			}
			impactedPubkeys := make([]string, 0, 1+len(previousFollowed)+len(contacts))
			impactedPubkeys = append(impactedPubkeys, pubkey)
			impactedPubkeys = append(impactedPubkeys, previousFollowed...)
			impactedPubkeys = append(impactedPubkeys, contacts...)
			if err := h.projectProfilePublicStatsForPubkeysTx(ctx, tx, impactedPubkeys, versionOverride); err != nil {
				return err
			}
			return nil
		},
	)
}

func (h *Handlers) ProjectRelayListsLatest(ctx context.Context, eventID string) error {
	return h.projectRelayListsLatestWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectRelayListsLatestWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectReplaceableListLatest(
		ctx,
		eventID,
		10002,
		DerivationRelayListsLatest,
		RelayListsLatestVersion,
		"Project relay_lists_latest from kind=10002 replaceables",
		versionOverride,
		func(tags [][]string) json.RawMessage {
			relays := make([]string, 0)
			for _, tag := range tags {
				if len(tag) < 2 || tag[0] != "r" {
					continue
				}
				value := strings.TrimSpace(tag[1])
				if value == "" {
					continue
				}
				relays = append(relays, value)
			}
			relays = normalizeUniqueIDs(relays)
			if len(relays) == 0 {
				return json.RawMessage(`[]`)
			}
			encoded, err := json.Marshal(relays)
			if err != nil {
				return json.RawMessage(`[]`)
			}
			return encoded
		},
		func(tx pgx.Tx, pubkey, winnerID string, winnerCreatedAt int64, payload json.RawMessage, writeVersion int) error {
			_, err := tx.Exec(ctx, `
				INSERT INTO relay_lists_latest (
					pubkey, event_id, created_at, relays_json, derivation_version
				)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (pubkey) DO UPDATE
				SET event_id = EXCLUDED.event_id,
				    created_at = EXCLUDED.created_at,
				    relays_json = EXCLUDED.relays_json,
				    derivation_version = EXCLUDED.derivation_version,
				    updated_at = now()
				WHERE EXCLUDED.created_at > relay_lists_latest.created_at
				   OR (
				       EXCLUDED.created_at = relay_lists_latest.created_at
				       AND EXCLUDED.event_id >= relay_lists_latest.event_id
				   )
			`, pubkey, winnerID, winnerCreatedAt, payload, writeVersion)
			if err != nil {
				return fmt.Errorf("upsert relay_lists_latest: %w", err)
			}
			return nil
		},
	)
}

type replaceableListPayloadBuilder func(tags [][]string) json.RawMessage
type replaceableListUpserter func(tx pgx.Tx, pubkey, winnerID string, winnerCreatedAt int64, payload json.RawMessage, writeVersion int) error

func (h *Handlers) projectReplaceableListLatest(
	ctx context.Context,
	eventID string,
	requiredKind int,
	derivationName string,
	derivationVersion int,
	derivationDescription string,
	versionOverride *int,
	buildPayload replaceableListPayloadBuilder,
	upsert replaceableListUpserter,
) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	if buildPayload == nil || upsert == nil {
		return fmt.Errorf("projection handlers are not configured")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var kind int
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &kind); err != nil {
		return fmt.Errorf("load event for %s: %w", derivationName, err)
	}
	if kind != requiredKind {
		return nil
	}

	var winnerID string
	var winnerCreatedAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT id, created_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&winnerID, &winnerCreatedAt); err != nil {
		return fmt.Errorf("load event row for %s: %w", derivationName, err)
	}

	tags, err := h.loadEventTags(ctx, winnerID)
	if err != nil {
		return err
	}
	payload := buildPayload(tags)

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		derivationName,
		derivationVersion,
		derivationDescription,
		versionOverride,
	)
	if err != nil {
		return err
	}
	if err := upsert(tx, pubkey, winnerID, winnerCreatedAt, payload, writeVersion); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}
