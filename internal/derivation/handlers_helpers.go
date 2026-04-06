package derivation

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/jobs"
)

func (h *Handlers) loadEventTags(ctx context.Context, eventID string) ([][]string, error) {
	var rawEvent string
	if err := h.pool.QueryRow(ctx, `SELECT raw_json::text FROM events WHERE id = $1`, eventID).Scan(&rawEvent); err != nil {
		return nil, fmt.Errorf("load raw event for derivation: %w", err)
	}
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(rawEvent), &payload); err != nil {
		return nil, fmt.Errorf("decode event tags: %w", err)
	}
	return payload.Tags, nil
}

func normalizeUniqueIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	slices.Sort(out)
	return out
}

func nullIfBlank(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func firstTagValue(tags [][]string, tagName string) string {
	for _, tag := range tags {
		if len(tag) < 2 {
			continue
		}
		if tag[0] != tagName {
			continue
		}
		value := strings.TrimSpace(tag[1])
		if value != "" {
			return value
		}
	}
	return ""
}

func parseZapAmountSats(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	amount, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || amount <= 0 {
		return 0
	}
	if amount >= 1000 {
		return amount / 1000
	}
	return amount
}

func eventExistsTx(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)
	`, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check referenced event existence: %w", err)
	}
	return exists, nil
}

func enqueueDerivationJobTx(ctx context.Context, tx pgx.Tx, jobType, eventID, idempotencySuffix string) error {
	return jobs.EnqueueEventJobTx(ctx, tx, jobType, eventID, idempotencySuffix, 5)
}
