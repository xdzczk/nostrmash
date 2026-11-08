package api

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/derivation"
)

type storedEntityBaseline struct {
	typical    float64
	sampleDays int
}

func (h Handlers) loadEntityBaselines(ctx context.Context, entityType string, keys []string) map[string]storedEntityBaseline {
	out := map[string]storedEntityBaseline{}
	if h.pool == nil || len(keys) == 0 || entityType == "" {
		return out
	}
	rows, err := h.pool.Query(ctx, `
		SELECT entity_key, typical, sample_days
		FROM entity_baseline
		WHERE entity_type = $1 AND metric = 'unique_authors' AND entity_key = ANY($2::text[])
	`, entityType, keys)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var row storedEntityBaseline
		if err := rows.Scan(&key, &row.typical, &row.sampleDays); err != nil {
			return out
		}
		out[key] = row
	}
	return out
}

func attachEntityBaseline(item map[string]any, row storedEntityBaseline, value float64) {
	if row.sampleDays < 7 || row.typical <= 0 || value < 0 {
		return
	}
	item["baseline"] = map[string]any{
		"metric":        "unique_authors",
		"window":        "24h",
		"value":         value,
		"typical":       row.typical,
		"typical_basis": "entity_30d_median",
		"ratio":         value / row.typical,
	}
}

func (h Handlers) attachHashtagBaselines(ctx context.Context, window string, items []map[string]any) {
	if window != "24h" || len(items) == 0 {
		return
	}
	keys := make([]string, 0, len(items))
	for _, item := range items {
		if key, ok := item["hashtag"].(string); ok && key != "" {
			keys = append(keys, key)
		}
	}
	baselines := h.loadEntityBaselines(ctx, "hashtag", keys)
	for _, item := range items {
		key, _ := item["hashtag"].(string)
		value, ok := asFloat(item["unique_authors"])
		if !ok {
			continue
		}
		attachEntityBaseline(item, baselines[key], value)
	}
}

func (h Handlers) attachDomainBaselines(ctx context.Context, window string, items []map[string]any) {
	if window != "24h" || len(items) == 0 {
		return
	}
	keys := make([]string, 0, len(items))
	for _, item := range items {
		if key, ok := item["domain"].(string); ok && key != "" {
			keys = append(keys, key)
		}
	}
	baselines := h.loadEntityBaselines(ctx, "domain", keys)
	for _, item := range items {
		key, _ := item["domain"].(string)
		value, ok := asFloat(item["unique_authors"])
		if !ok {
			continue
		}
		attachEntityBaseline(item, baselines[key], value)
	}
}

func asFloat(value any) (float64, bool) {
	switch n := value.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	default:
		return 0, false
	}
}

func weekdaySeriesBaseline(ctx context.Context, q derivation.RowQuerier, metric string, value float64) (map[string]any, error) {
	typical, samples, err := derivation.SameWeekdayMedian(ctx, q, metric)
	if err != nil {
		return nil, err
	}
	if samples < 4 || typical <= 0 || value < 0 {
		return nil, nil
	}
	return map[string]any{
		"metric":        metric,
		"window":        "24h",
		"value":         value,
		"typical":       typical,
		"typical_basis": "same_weekday_median",
		"ratio":         value / typical,
	}, nil
}
