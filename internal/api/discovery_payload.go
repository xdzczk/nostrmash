package api

import "github.com/xdzczk/nostrmash/internal/query"

func buildRelayStatsPayload(stats query.PublicDiscoveryNetworkStats) map[string]any {
	relays := map[string]any{
		"total":      stats.Relays,
		"active_24h": stats.RelaySummary.Active24h,
		"active_7d":  stats.RelaySummary.Active7d,
		"event_volume": map[string]any{
			"24h": stats.RelaySummary.EventVolume.Last24h,
			"7d":  stats.RelaySummary.EventVolume.Last7d,
		},
		"unique_authors": map[string]any{
			"24h": stats.RelaySummary.UniqueAuthors.Last24h,
			"7d":  stats.RelaySummary.UniqueAuthors.Last7d,
		},
	}
	if len(stats.TopRelays) > 0 {
		topRelays := make([]map[string]any, 0, len(stats.TopRelays))
		for _, relay := range stats.TopRelays {
			topRelays = append(topRelays, map[string]any{
				"relay_url":      relay.RelayURL,
				"event_count":    relay.EventCount,
				"unique_authors": relay.UniqueAuthors,
			})
		}
		relays["top"] = topRelays
	}
	return relays
}
