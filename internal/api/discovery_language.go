package api

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/query"
)

func discoveryCacheParams(params map[string]any, lang string) map[string]any {
	if lang != "" {
		params["lang"] = lang
	}
	return params
}

func discoveryScope(lang string) query.DiscoveryScope {
	return query.DiscoveryScope{Language: lang}
}

// attachDiscoveryLanguage echoes the active language and adds the chart the
// selector is built from. The chart stays global so a filtered page can still
// offer the other languages. A chart lookup failure leaves the ranking itself.
func (h Handlers) attachDiscoveryLanguage(ctx context.Context, payload map[string]any, lang string) {
	if lang != "" {
		payload["language"] = lang
	}
	stats, err := h.service.GetPublicDiscoveryNetworkStats(ctx, 8, query.DiscoveryScope{})
	if err != nil {
		return
	}
	if len(stats.TopLanguages24h) == 0 && len(stats.TopLanguages7d) == 0 && len(stats.TopLanguages30d) == 0 {
		return
	}
	payload["top_languages"] = map[string]any{
		"24h": stats.TopLanguages24h,
		"7d":  stats.TopLanguages7d,
		"30d": stats.TopLanguages30d,
	}
}

func selectableDiscoveryLanguages(stats query.PublicDiscoveryNetworkStats, limit int) []string {
	if limit <= 0 {
		return nil
	}
	out := make([]string, 0, limit)
	for _, row := range stats.TopLanguages24h {
		switch row.Language {
		case "", "und", "und-uncertain":
			continue
		}
		out = append(out, row.Language)
		if len(out) == limit {
			break
		}
	}
	return out
}
