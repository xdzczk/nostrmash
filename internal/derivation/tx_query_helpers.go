package derivation

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func queryInt64Tx(ctx context.Context, tx pgx.Tx, sql string, args ...any) (int64, error) {
	var value int64
	if err := tx.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func queryFloat64Tx(ctx context.Context, tx pgx.Tx, sql string, args ...any) (float64, error) {
	var value float64
	if err := tx.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func (h *Handlers) eventPubkeyTx(ctx context.Context, tx pgx.Tx, eventID string) (string, error) {
	var pubkey string
	if err := tx.QueryRow(ctx, `
		SELECT pubkey
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("load target event pubkey: %w", err)
	}
	return pubkey, nil
}
