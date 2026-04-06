package api

import (
	"context"
	"fmt"
	"time"
)

type adminDerivationVersionResponse struct {
	DerivationName  string    `json:"derivation_name"`
	ActiveVersion   int       `json:"active_version"`
	TargetVersion   int       `json:"target_version"`
	CompiledVersion int       `json:"compiled_version"`
	Description     string    `json:"description"`
	Status          string    `json:"status"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (s *adminService) GetDerivationVersions(ctx context.Context) ([]adminDerivationVersionResponse, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			a.derivation_name,
			a.active_version,
			a.target_version,
			COALESCE(vm.max_known_version, a.target_version) AS compiled_version,
			COALESCE(vt.description, a.description) AS description,
			a.updated_at
		FROM derivation_active_versions a
		LEFT JOIN (
			SELECT projection_name, MAX(version) AS max_known_version
			FROM derivation_versions
			GROUP BY projection_name
		) vm ON vm.projection_name = a.derivation_name
		LEFT JOIN derivation_versions vt
			ON vt.projection_name = a.derivation_name
		   AND vt.version = a.target_version
		ORDER BY a.derivation_name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list derivation versions: %w", err)
	}
	defer rows.Close()

	out := make([]adminDerivationVersionResponse, 0)
	for rows.Next() {
		var row adminDerivationVersionResponse
		if err := rows.Scan(
			&row.DerivationName,
			&row.ActiveVersion,
			&row.TargetVersion,
			&row.CompiledVersion,
			&row.Description,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan derivation version row: %w", err)
		}
		row.Status = "aligned"
		if row.ActiveVersion != row.TargetVersion {
			row.Status = "rebuild_pending"
		}
		row.UpdatedAt = row.UpdatedAt.UTC()
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read derivation version rows: %w", err)
	}
	return out, nil
}
