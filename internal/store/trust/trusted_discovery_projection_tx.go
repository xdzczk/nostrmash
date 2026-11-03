package trust

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func refreshTrustedNoteDiscoveryProjectionTx(ctx context.Context, tx pgx.Tx, derivationVersion int) error {
	var snapshotRefreshedAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT MAX(refreshed_at)
		FROM trust_graph_snapshot
	`).Scan(&snapshotRefreshedAt); err != nil {
		return fmt.Errorf("load trust snapshot timestamp for trusted note projection: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM trusted_note_discovery_candidates t
		WHERE NOT EXISTS (
			SELECT 1
			FROM note_discovery_stats n
			WHERE n.event_id = t.event_id
		)
	`); err != nil {
		return fmt.Errorf("delete stale trusted note candidates: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_note_discovery_candidates (
			event_id,
			author_pubkey,
			min_hops,
			trust_score,
			source_run_id,
			trust_snapshot_refreshed_at,
			derivation_version,
			projected_at
		)
		SELECT
			n.event_id,
			n.author_pubkey,
			latest.min_hops,
			latest.score,
			latest.source_run_id,
			$1,
			$2,
			now()
		FROM note_discovery_stats n
		LEFT JOIN trust_pubkeys_latest latest ON latest.pubkey = n.author_pubkey
		LEFT JOIN trusted_note_discovery_candidates existing ON existing.event_id = n.event_id
		WHERE existing.event_id IS NULL
		   OR existing.author_pubkey IS DISTINCT FROM n.author_pubkey
		   OR existing.min_hops IS DISTINCT FROM latest.min_hops
		   OR existing.trust_score IS DISTINCT FROM latest.score
		   OR existing.source_run_id IS DISTINCT FROM latest.source_run_id
		   OR existing.derivation_version IS DISTINCT FROM $2
		ON CONFLICT (event_id) DO UPDATE
		SET author_pubkey = EXCLUDED.author_pubkey,
		    min_hops = EXCLUDED.min_hops,
		    trust_score = EXCLUDED.trust_score,
		    source_run_id = EXCLUDED.source_run_id,
		    trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
		WHERE trusted_note_discovery_candidates.author_pubkey IS DISTINCT FROM EXCLUDED.author_pubkey
		   OR trusted_note_discovery_candidates.min_hops IS DISTINCT FROM EXCLUDED.min_hops
		   OR trusted_note_discovery_candidates.trust_score IS DISTINCT FROM EXCLUDED.trust_score
		   OR trusted_note_discovery_candidates.source_run_id IS DISTINCT FROM EXCLUDED.source_run_id
		   OR trusted_note_discovery_candidates.derivation_version IS DISTINCT FROM EXCLUDED.derivation_version
	`, snapshotRefreshedAt, derivationVersion); err != nil {
		return fmt.Errorf("refresh trusted note discovery projection: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_discovery_projection_state (
			projection_name,
			trust_snapshot_refreshed_at,
			refreshed_at,
			derivation_version
		)
		VALUES ('trusted_note_discovery_candidates', $1, now(), $2)
		ON CONFLICT (projection_name) DO UPDATE
		SET trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    refreshed_at = now(),
		    derivation_version = EXCLUDED.derivation_version
	`, snapshotRefreshedAt, derivationVersion); err != nil {
		return fmt.Errorf("upsert trusted note projection state: %w", err)
	}
	return nil
}

func refreshTrustedProfileDiscoveryProjectionTx(ctx context.Context, tx pgx.Tx, derivationVersion int) error {
	var snapshotRefreshedAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT MAX(refreshed_at)
		FROM trust_graph_snapshot
	`).Scan(&snapshotRefreshedAt); err != nil {
		return fmt.Errorf("load trust snapshot timestamp for trusted profile projection: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM trusted_profile_discovery_candidates t
		WHERE NOT EXISTS (
			SELECT 1
			FROM profile_discovery_stats p
			WHERE p.pubkey = t.pubkey
		)
	`); err != nil {
		return fmt.Errorf("delete stale trusted profile candidates: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_profile_discovery_candidates (
			pubkey,
			min_hops,
			trust_score,
			source_run_id,
			trust_snapshot_refreshed_at,
			derivation_version,
			projected_at
		)
		SELECT
			p.pubkey,
			latest.min_hops,
			latest.score,
			latest.source_run_id,
			$1,
			$2,
			now()
		FROM profile_discovery_stats p
		LEFT JOIN trust_pubkeys_latest latest ON latest.pubkey = p.pubkey
		LEFT JOIN trusted_profile_discovery_candidates existing ON existing.pubkey = p.pubkey
		WHERE existing.pubkey IS NULL
		   OR existing.min_hops IS DISTINCT FROM latest.min_hops
		   OR existing.trust_score IS DISTINCT FROM latest.score
		   OR existing.source_run_id IS DISTINCT FROM latest.source_run_id
		   OR existing.derivation_version IS DISTINCT FROM $2
		ON CONFLICT (pubkey) DO UPDATE
		SET min_hops = EXCLUDED.min_hops,
		    trust_score = EXCLUDED.trust_score,
		    source_run_id = EXCLUDED.source_run_id,
		    trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
		WHERE trusted_profile_discovery_candidates.min_hops IS DISTINCT FROM EXCLUDED.min_hops
		   OR trusted_profile_discovery_candidates.trust_score IS DISTINCT FROM EXCLUDED.trust_score
		   OR trusted_profile_discovery_candidates.source_run_id IS DISTINCT FROM EXCLUDED.source_run_id
		   OR trusted_profile_discovery_candidates.derivation_version IS DISTINCT FROM EXCLUDED.derivation_version
	`, snapshotRefreshedAt, derivationVersion); err != nil {
		return fmt.Errorf("refresh trusted profile discovery projection: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_discovery_projection_state (
			projection_name,
			trust_snapshot_refreshed_at,
			refreshed_at,
			derivation_version
		)
		VALUES ('trusted_profile_discovery_candidates', $1, now(), $2)
		ON CONFLICT (projection_name) DO UPDATE
		SET trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    refreshed_at = now(),
		    derivation_version = EXCLUDED.derivation_version
	`, snapshotRefreshedAt, derivationVersion); err != nil {
		return fmt.Errorf("upsert trusted profile projection state: %w", err)
	}
	return nil
}
