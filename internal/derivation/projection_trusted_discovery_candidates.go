package derivation

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	storetrust "github.com/xdzczk/nostrmash/internal/store/trust"
)

func (h *Handlers) rebuildTrustedNoteDiscoveryWithVersion(ctx context.Context, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationTrustedNoteDiscovery,
		TrustedNoteDiscoveryVersion,
		"Project trust-qualified discovery metadata for note candidates",
		versionOverride,
	)
	if err != nil {
		return err
	}
	// Rebuilds may run against manually seeded snapshot/score rows (tests and
	// operator rebuilds). Refresh the denormalized hop+score table first so
	// projection joins stay on a single source of truth.
	if err := storetrust.RefreshTrustPubkeysLatestTx(ctx, tx); err != nil {
		return err
	}
	if err := refreshTrustedNoteDiscoveryTx(ctx, tx, writeVersion); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trusted note discovery rebuild tx: %w", err)
	}
	return nil
}

func (h *Handlers) rebuildTrustedProfileDiscoveryWithVersion(ctx context.Context, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationTrustedProfileDiscovery,
		TrustedProfileDiscoveryVersion,
		"Project trust-qualified discovery metadata for profile candidates",
		versionOverride,
	)
	if err != nil {
		return err
	}
	if err := storetrust.RefreshTrustPubkeysLatestTx(ctx, tx); err != nil {
		return err
	}
	if err := refreshTrustedProfileDiscoveryTx(ctx, tx, writeVersion); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trusted profile discovery rebuild tx: %w", err)
	}
	return nil
}

func refreshTrustedNoteDiscoveryTx(ctx context.Context, tx pgx.Tx, writeVersion int) error {
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
		ON CONFLICT (event_id) DO UPDATE
		SET author_pubkey = EXCLUDED.author_pubkey,
		    min_hops = EXCLUDED.min_hops,
		    trust_score = EXCLUDED.trust_score,
		    source_run_id = EXCLUDED.source_run_id,
		    trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`, snapshotRefreshedAt, writeVersion); err != nil {
		return fmt.Errorf("refresh trusted note candidates: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_discovery_projection_state (
			projection_name,
			trust_snapshot_refreshed_at,
			refreshed_at,
			derivation_version
		)
		VALUES ($1, $2, now(), $3)
		ON CONFLICT (projection_name) DO UPDATE
		SET trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    refreshed_at = now(),
		    derivation_version = EXCLUDED.derivation_version
	`, DerivationTrustedNoteDiscovery, snapshotRefreshedAt, writeVersion); err != nil {
		return fmt.Errorf("upsert trusted note projection state: %w", err)
	}
	return nil
}

func refreshTrustedProfileDiscoveryTx(ctx context.Context, tx pgx.Tx, writeVersion int) error {
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
		ON CONFLICT (pubkey) DO UPDATE
		SET min_hops = EXCLUDED.min_hops,
		    trust_score = EXCLUDED.trust_score,
		    source_run_id = EXCLUDED.source_run_id,
		    trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`, snapshotRefreshedAt, writeVersion); err != nil {
		return fmt.Errorf("refresh trusted profile candidates: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_discovery_projection_state (
			projection_name,
			trust_snapshot_refreshed_at,
			refreshed_at,
			derivation_version
		)
		VALUES ($1, $2, now(), $3)
		ON CONFLICT (projection_name) DO UPDATE
		SET trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    refreshed_at = now(),
		    derivation_version = EXCLUDED.derivation_version
	`, DerivationTrustedProfileDiscovery, snapshotRefreshedAt, writeVersion); err != nil {
		return fmt.Errorf("upsert trusted profile projection state: %w", err)
	}
	return nil
}
