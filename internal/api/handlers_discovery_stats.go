package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
	"golang.org/x/sync/singleflight"
)

// publicStatsHashtagFetchLimit is the snapshot width. Both stats endpoints
// read this once and slice per request so note volume, hashtags, and relay
// rankings cannot come from different refreshes.
const publicStatsHashtagFetchLimit = 50

// publicStatsMemo is the single in-process copy of the public stats snapshot.
// Network, content, and relay responses are rendered from it and stored
// together, so those endpoints agree until the next refresh.
type publicStatsMemo struct {
	mu        sync.Mutex
	stats     query.PublicDiscoveryNetworkStats
	expiresAt time.Time
	ready     bool
	loads     singleflight.Group
	fills     singleflight.Group
}

func (m *publicStatsMemo) getFresh(now time.Time) (query.PublicDiscoveryNetworkStats, bool) {
	if m == nil {
		return query.PublicDiscoveryNetworkStats{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.ready || !m.expiresAt.After(now) {
		return query.PublicDiscoveryNetworkStats{}, false
	}
	return m.stats, true
}

func (m *publicStatsMemo) store(stats query.PublicDiscoveryNetworkStats, ttl time.Duration, now time.Time) {
	if m == nil || ttl <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats = stats
	m.expiresAt = now.Add(ttl)
	m.ready = true
}

func (h Handlers) GetNetworkStats(w http.ResponseWriter, r *http.Request) {
	hashtagLimit, err := parseBoundedPositiveInt(r, "hashtag_limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := h.serveSharedDiscoveryStats(r.Context(), w, "network_stats", map[string]any{
		"hashtag_limit": hashtagLimit,
	}, hashtagLimit, func(stats query.PublicDiscoveryNetworkStats) map[string]any {
		return h.networkStatsPayload(stats)
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "network stats are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func (h Handlers) GetContentStats(w http.ResponseWriter, r *http.Request) {
	hashtagLimit, err := parseBoundedPositiveInt(r, "hashtag_limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := h.serveSharedDiscoveryStats(r.Context(), w, "content_stats", map[string]any{
		"hashtag_limit": hashtagLimit,
	}, hashtagLimit, func(stats query.PublicDiscoveryNetworkStats) map[string]any {
		return h.contentStatsPayload(stats)
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "content stats are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func (h Handlers) GetRelayStats(w http.ResponseWriter, r *http.Request) {
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if window == "" {
		window = "7d"
	}
	if window != "24h" && window != "7d" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "window must be one of 24h, 7d")
		return
	}
	if err := h.serveSharedDiscoveryStats(r.Context(), w, "relay_stats", map[string]any{
		"window": window,
	}, 10, func(stats query.PublicDiscoveryNetworkStats) map[string]any {
		return h.relayStatsPayload(stats, window)
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "relay stats are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func (h Handlers) GetDiscoveryStatsSeries(w http.ResponseWriter, r *http.Request) {
	metric := strings.TrimSpace(r.URL.Query().Get("metric"))
	switch metric {
	case "note_volume", "active_authors", "relay_events":
	case "":
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "metric is required")
		return
	default:
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "metric must be one of note_volume, active_authors, relay_events")
		return
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if window == "" {
		window = "7d"
	}
	if window != "24h" && window != "7d" && window != "30d" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "window must be one of 24h, 7d, 30d")
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyStats, "discovery_stats_series", map[string]any{
		"metric": metric,
		"window": window,
	})
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		series, seriesErr := h.service.GetDiscoveryStatsSeries(ctx, metric, window)
		if seriesErr != nil {
			return nil, seriesErr
		}
		points := make([]map[string]any, 0, len(series.Points))
		for _, point := range series.Points {
			points = append(points, map[string]any{
				"t": point.T.Unix(),
				"v": point.V,
			})
		}
		scope := "indexed_notes"
		summary := "Each point is the rolling 24-hour total for indexed notes, sampled once per UTC hour. It is not an hourly increment."
		if metric == "relay_events" {
			scope = "event_relays"
			summary = "Each point is the rolling 24-hour count of events seen on relays, sampled once per UTC hour. It is not an hourly increment."
		}
		if metric == "active_authors" {
			summary = "Each point is the rolling 24-hour count of distinct note authors, sampled once per UTC hour. It is not an hourly increment, and it is not the count of pubkeys seen on relays."
		}
		return map[string]any{
			"metric":        series.Metric,
			"window":        series.Window,
			"computed_at":   series.ComputedAt,
			"points":        points,
			"consistency":   "eventual",
			"trust_applied": false,
			"result_scope":  scope,
			"point_meaning": "rolling_24h_total",
			"summary":       summary,
		}, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "discovery stats series are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func (h Handlers) serveSharedDiscoveryStats(
	ctx context.Context,
	w http.ResponseWriter,
	endpoint string,
	params map[string]any,
	hashtagLimit int,
	build func(query.PublicDiscoveryNetworkStats) map[string]any,
) error {
	if h.discoveryCache == nil || h.publicStatsMemo == nil {
		stats, err := h.service.GetPublicDiscoveryNetworkStats(ctx, hashtagLimit, query.DiscoveryScope{})
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, build(stats))
		return nil
	}
	policy := h.newPublicCachePolicy(publicCacheFamilyStats, endpoint, params)
	if payload, fresh, ok := h.discoveryCache.get(policy.key); ok {
		if h.cacheLookupObserver != nil {
			h.cacheLookupObserver(string(policy.family), policy.endpoint, true)
		}
		if !fresh {
			h.refreshSharedDiscoveryStatsAsync(hashtagLimit)
		}
		h.writeCachedPayload(w, policy, payload)
		return nil
	}
	if h.cacheLookupObserver != nil {
		h.cacheLookupObserver(string(policy.family), policy.endpoint, false)
	}
	if err := h.fillSharedDiscoveryStats(ctx, hashtagLimit); err != nil {
		return err
	}
	payload, _, ok := h.discoveryCache.get(policy.key)
	if !ok {
		return fmt.Errorf("discovery stats cache missing %s", policy.key)
	}
	h.writeCachedPayload(w, policy, payload)
	return nil
}

func (h Handlers) refreshSharedDiscoveryStatsAsync(hashtagLimit int) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), discoveryStaleRefreshTimeout)
		defer cancel()
		_ = h.fillSharedDiscoveryStats(ctx, hashtagLimit)
	}()
}

func (h Handlers) fillSharedDiscoveryStats(ctx context.Context, hashtagLimit int) error {
	if hashtagLimit <= 0 {
		hashtagLimit = 10
	}
	_, err, _ := h.publicStatsMemo.fills.Do(strconv.Itoa(hashtagLimit), func() (any, error) {
		stats, loadErr := h.memoizedPublicStats(ctx)
		if loadErr != nil {
			return nil, loadErr
		}
		sliced := slicePublicStatsHashtags(stats, hashtagLimit)
		if err := h.storeDiscoveryStatsPayload("network_stats", map[string]any{
			"hashtag_limit": hashtagLimit,
		}, h.networkStatsPayload(sliced)); err != nil {
			return nil, err
		}
		if err := h.storeDiscoveryStatsPayload("content_stats", map[string]any{
			"hashtag_limit": hashtagLimit,
		}, h.contentStatsPayload(sliced)); err != nil {
			return nil, err
		}
		for _, window := range []string{"24h", "7d"} {
			if err := h.storeDiscoveryStatsPayload("relay_stats", map[string]any{
				"window": window,
			}, h.relayStatsPayload(stats, window)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

func (h Handlers) memoizedPublicStats(ctx context.Context) (query.PublicDiscoveryNetworkStats, error) {
	if stats, ok := h.publicStatsMemo.getFresh(time.Now()); ok {
		return stats, nil
	}
	loaded, err, _ := h.publicStatsMemo.loads.Do("public-discovery-stats", func() (any, error) {
		if stats, ok := h.publicStatsMemo.getFresh(time.Now()); ok {
			return stats, nil
		}
		stats, loadErr := h.service.GetPublicDiscoveryNetworkStats(ctx, publicStatsHashtagFetchLimit, query.DiscoveryScope{})
		if loadErr != nil {
			return query.PublicDiscoveryNetworkStats{}, loadErr
		}
		h.publicStatsMemo.store(stats, h.cacheConfig.ttlForFamily(publicCacheFamilyStats), time.Now())
		return stats, nil
	})
	if err != nil {
		return query.PublicDiscoveryNetworkStats{}, err
	}
	stats, ok := loaded.(query.PublicDiscoveryNetworkStats)
	if !ok {
		return query.PublicDiscoveryNetworkStats{}, fmt.Errorf("public stats cache returned %T", loaded)
	}
	return stats, nil
}

func (h Handlers) storeDiscoveryStatsPayload(endpoint string, params map[string]any, payload map[string]any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	policy := h.newPublicCachePolicy(publicCacheFamilyStats, endpoint, params)
	ttl := h.cacheConfig.ttlForFamily(policy.family)
	if ttl > 0 && h.discoveryCache != nil {
		h.discoveryCache.set(policy.key, encoded, ttl)
	}
	return nil
}

func slicePublicStatsHashtags(stats query.PublicDiscoveryNetworkStats, limit int) query.PublicDiscoveryNetworkStats {
	if stats.TopHashtags == nil {
		return stats
	}
	windows := *stats.TopHashtags
	windows.Last24h = sliceHashtags(windows.Last24h, limit)
	windows.Last7d = sliceHashtags(windows.Last7d, limit)
	stats.TopHashtags = &windows
	return stats
}

func (h Handlers) networkStatsPayload(stats query.PublicDiscoveryNetworkStats) map[string]any {
	network := map[string]any{
		"totals": map[string]any{
			"events_ingested":    stats.EventsIngested,
			"projected_profiles": stats.ProjectedProfiles,
		},
		"activity": map[string]any{
			"active_authors": stats.ActiveAuthors,
			"note_volume":    stats.NoteVolume,
		},
		"relays": buildRelayStatsPayload(stats, "7d"),
	}
	if stats.TopHashtags != nil {
		network["top_hashtags"] = stats.TopHashtags
	}
	if len(stats.TopLanguages24h) > 0 || len(stats.TopLanguages7d) > 0 {
		network["top_languages"] = map[string]any{
			"24h": stats.TopLanguages24h,
			"7d":  stats.TopLanguages7d,
		}
	}
	payload := map[string]any{
		"network":       network,
		"computed_at":   stats.ComputedAt,
		"consistency":   "eventual",
		"trust_applied": false,
		"result_scope":  "mixed",
		"sections":      publicStatsSections(stats),
	}
	return payload
}

func (h Handlers) contentStatsPayload(stats query.PublicDiscoveryNetworkStats) map[string]any {
	content := map[string]any{
		"totals": map[string]any{
			"events_ingested":    stats.EventsIngested,
			"projected_profiles": stats.ProjectedProfiles,
		},
		"note_volume": stats.NoteVolume,
	}
	if stats.TopHashtags != nil {
		content["top_hashtags"] = stats.TopHashtags
	}
	if len(stats.TopLanguages24h) > 0 || len(stats.TopLanguages7d) > 0 {
		content["top_languages"] = map[string]any{
			"24h": stats.TopLanguages24h,
			"7d":  stats.TopLanguages7d,
		}
	}
	sections := publicStatsSections(stats)
	delete(sections, "active_authors")
	delete(sections, "relays")
	return map[string]any{
		"content":       content,
		"computed_at":   stats.ComputedAt,
		"consistency":   "eventual",
		"trust_applied": false,
		"result_scope":  "mixed",
		"sections":      sections,
	}
}

func (h Handlers) relayStatsPayload(stats query.PublicDiscoveryNetworkStats, window string) map[string]any {
	return map[string]any{
		"window":        window,
		"relays":        buildRelayStatsPayload(stats, window),
		"computed_at":   stats.ComputedAt,
		"consistency":   "eventual",
		"trust_applied": false,
		"result_scope":  "event_relays",
		"sections": map[string]any{
			"relays": publicStatsSections(stats)["relays"],
		},
	}
}

func publicStatsSections(stats query.PublicDiscoveryNetworkStats) map[string]any {
	times := stats.SectionTimes
	return map[string]any{
		"note_volume": statsSection(
			"indexed_notes",
			false,
			"Distinct notes in the discovery projection, counted by the note's created_at.",
			map[string]time.Time{"24h": times.Home24h, "7d": times.Home7d},
		),
		"active_authors": statsSection(
			"indexed_notes",
			false,
			"Distinct authors of those notes. This is not the number of pubkeys seen on relays.",
			map[string]time.Time{"24h": times.Home24h, "7d": times.Home7d},
		),
		"top_hashtags": statsSection(
			"trust_graph",
			true,
			"Hashtags from notes whose authors are inside the trust graph.",
			map[string]time.Time{"24h": times.Hashtags24h, "7d": times.Hashtags7d},
		),
		"top_languages": statsSection(
			"indexed_notes",
			false,
			"Languages of indexed notes.",
			map[string]time.Time{"24h": times.Languages24h, "7d": times.Languages7d},
		),
		"relays": statsSection(
			"event_relays",
			false,
			"Relays that have delivered events. total is the all-time distinct relay URL count. unique_authors counts every pubkey seen on a relay, any event kind, by seen_at. The leaderboard top list is event count for the selected window.",
			map[string]time.Time{
				"summary": times.Relays,
				"top_24h": times.TopRelays24h,
				"top_7d":  times.TopRelays7d,
			},
		),
	}
}

func statsSection(scope string, trustApplied bool, summary string, computed map[string]time.Time) map[string]any {
	section := map[string]any{
		"scope":         scope,
		"trust_applied": trustApplied,
		"summary":       summary,
	}
	freshness := map[string]any{}
	for key, value := range computed {
		if value.IsZero() {
			continue
		}
		freshness[key] = value.UTC()
	}
	if len(freshness) > 0 {
		section["computed_at"] = freshness
	}
	return section
}
