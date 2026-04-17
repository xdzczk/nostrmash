package derivation

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
)

func upsertDerivationVersion(
	ctx context.Context,
	tx pgx.Tx,
	name string,
	version int,
	description string,
) error {
	codeVersion := strings.TrimSpace(os.Getenv("APP_VERSION"))
	if codeVersion == "" {
		codeVersion = "dev"
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO derivation_versions (projection_name, version, code_version, description, activated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (projection_name, version) DO UPDATE
		SET code_version = EXCLUDED.code_version,
		    description = EXCLUDED.description
	`,
		name,
		version,
		codeVersion,
		description,
	)
	if err != nil {
		return fmt.Errorf("upsert derivation version %q: %w", name, err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO derivation_active_versions (
			derivation_name, active_version, target_version, description
		)
		VALUES ($1, $2, $2, $3)
		ON CONFLICT (derivation_name) DO UPDATE
		SET target_version = EXCLUDED.target_version,
		    description = EXCLUDED.description,
		    updated_at = now()
	`,
		name,
		version,
		description,
	)
	if err != nil {
		return fmt.Errorf("upsert derivation active version %q: %w", name, err)
	}
	return nil
}

// resolveDerivationWriteVersion returns the active version for a derivation.
//
// Per-job hot path (versionOverride == nil): a pure SELECT against
// derivation_active_versions. The row is guaranteed to exist because
// EnsureRegisteredDerivations runs once at worker startup. We deliberately
// avoid UPSERTing here: the underlying rows are keyed by a small fixed set of
// projection names, so doing a write per job creates pathological row-lock
// contention when many workers run in parallel and effectively serializes the
// entire derivation pipeline.
//
// Rebuild path (versionOverride != nil): retains the original write semantics
// so admin-triggered rebuilds can register new target versions and bump
// metadata. Rebuilds are infrequent and sequential, so contention is
// acceptable.
func resolveDerivationWriteVersion(
	ctx context.Context,
	tx pgx.Tx,
	name string,
	targetVersion int,
	description string,
	versionOverride *int,
) (int, error) {
	if versionOverride != nil {
		if err := upsertDerivationVersion(ctx, tx, name, *versionOverride, description); err != nil {
			return 0, err
		}
		return *versionOverride, nil
	}
	var activeVersion int
	if err := tx.QueryRow(ctx, `
		SELECT active_version
		FROM derivation_active_versions
		WHERE derivation_name = $1
	`, name).Scan(&activeVersion); err != nil {
		return 0, fmt.Errorf("load active derivation version %q (ensure EnsureRegisteredDerivations ran at startup): %w", name, err)
	}
	return activeVersion, nil
}
