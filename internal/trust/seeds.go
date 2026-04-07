package trust

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func loadActiveSeeds(ctx context.Context, queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}) (map[string]struct{}, error) {
	rows, err := queryer.Query(ctx, `
		SELECT pubkey
		FROM trust_seeds
		WHERE is_active = true
	`)
	if err != nil {
		return nil, fmt.Errorf("list active trust seeds: %w", err)
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return nil, fmt.Errorf("scan trust seed: %w", err)
		}
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}
		out[pubkey] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trust seed rows: %w", err)
	}
	return out, nil
}
