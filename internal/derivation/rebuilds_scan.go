package derivation

type rebuildRunRowScanner interface {
	Scan(dest ...any) error
}

func scanProjectionRebuildRun(row rebuildRunRowScanner) (ProjectionRebuildRun, error) {
	out := ProjectionRebuildRun{}
	var scopeEventID *string
	var scopePubkey *string
	err := row.Scan(
		&out.ID,
		&out.DerivationName,
		&out.TargetVersion,
		&out.Scope.Type,
		&scopeEventID,
		&scopePubkey,
		&out.Scope.StartCreatedAt,
		&out.Scope.EndCreatedAt,
		&out.Status,
		&out.JobID,
		&out.Attempts,
		&out.StartedAt,
		&out.FinishedAt,
		&out.LastError,
	)
	if err != nil {
		return out, err
	}
	if scopeEventID != nil {
		out.Scope.EventID = *scopeEventID
	}
	if scopePubkey != nil {
		out.Scope.Pubkey = *scopePubkey
	}
	return out, nil
}
