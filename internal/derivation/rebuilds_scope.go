package derivation

import (
	"context"
	"fmt"
	"strings"
)

func (h *Handlers) scopeEventIDs(ctx context.Context, scope ProjectionRebuildScope) ([]string, error) {
	switch scope.Type {
	case RebuildScopeEvent:
		return []string{scope.EventID}, nil
	case RebuildScopePubkey:
		return h.queryEventIDs(ctx, `
			SELECT id
			FROM events
			WHERE pubkey = $1
			ORDER BY created_at ASC, id ASC
		`, scope.Pubkey)
	case RebuildScopeTimeRange:
		return h.queryEventIDs(ctx, `
			SELECT id
			FROM events
			WHERE created_at >= $1
			  AND created_at <= $2
			ORDER BY created_at ASC, id ASC
		`, *scope.StartCreatedAt, *scope.EndCreatedAt)
	case RebuildScopeFull:
		return h.queryEventIDs(ctx, `
			SELECT id
			FROM events
			ORDER BY created_at ASC, id ASC
		`)
	default:
		return nil, fmt.Errorf("unsupported rebuild scope type %q", scope.Type)
	}
}

func (h *Handlers) queryEventIDs(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := h.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query scope events: %w", err)
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan scope event id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read scope events: %w", err)
	}
	return ids, nil
}

func normalizeRebuildScope(scope ProjectionRebuildScope) (ProjectionRebuildScope, error) {
	out := ProjectionRebuildScope{
		Type: strings.ToLower(strings.TrimSpace(scope.Type)),
	}
	switch out.Type {
	case RebuildScopeFull:
		return out, nil
	case RebuildScopeEvent, "event-scoped":
		out.Type = RebuildScopeEvent
		out.EventID = strings.TrimSpace(scope.EventID)
		if out.EventID == "" {
			return out, fmt.Errorf("event_id is required for event rebuild scope")
		}
		return out, nil
	case RebuildScopePubkey, "pubkey-scoped":
		out.Type = RebuildScopePubkey
		out.Pubkey = strings.TrimSpace(scope.Pubkey)
		if out.Pubkey == "" {
			return out, fmt.Errorf("pubkey is required for pubkey rebuild scope")
		}
		return out, nil
	case RebuildScopeTimeRange, "time-range":
		out.Type = RebuildScopeTimeRange
		if scope.StartCreatedAt == nil || scope.EndCreatedAt == nil {
			return out, fmt.Errorf("start_created_at and end_created_at are required for time_range rebuild scope")
		}
		if *scope.StartCreatedAt > *scope.EndCreatedAt {
			return out, fmt.Errorf("start_created_at must be <= end_created_at")
		}
		out.StartCreatedAt = scope.StartCreatedAt
		out.EndCreatedAt = scope.EndCreatedAt
		return out, nil
	default:
		return out, fmt.Errorf("unsupported rebuild scope type %q", scope.Type)
	}
}
